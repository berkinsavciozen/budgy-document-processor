
import os
import base64
import json
import logging
import time
from typing import Optional, Dict, List, Any, Union
from datetime import datetime
from fastapi import FastAPI, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import supabase
import fitz  # PyMuPDF
import re
import io
from PIL import Image
import pytesseract
import pdf2image
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("budgy-processor")

app = FastAPI(title="Budgy Document Processor")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict this to specific domains
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize Supabase client
def get_supabase_client():
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_SERVICE_KEY")
    
    if not supabase_url or not supabase_key:
        logger.warning("Supabase credentials not found in environment variables")
        return None
    
    try:
        client = supabase.create_client(supabase_url, supabase_key)
        return client
    except Exception as e:
        logger.error(f"Failed to initialize Supabase client: {str(e)}")
        return None

# Models
class ExtractionOptions(BaseModel):
    enable_ocr: bool = True
    enable_table_detection: bool = True
    page_limit: int = 0  # 0 means all pages
    confidence_threshold: float = 0.5
    use_advanced_extraction: bool = True

class DocumentRequest(BaseModel):
    file_path: str
    document_id: str
    bucket_name: str = "documents"
    extraction_options: Optional[ExtractionOptions] = Field(default_factory=ExtractionOptions)

class Transaction(BaseModel):
    date: str
    description: str
    amount: str
    category: Optional[str] = "Other"
    confidence: Optional[float] = 0.9

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    supabase_client = get_supabase_client()
    supabase_status = "connected" if supabase_client else "disconnected"
    
    return {
        "status": "healthy", 
        "service": "budgy-document-processor",
        "supabase": supabase_status,
        "version": "1.0.0"
    }

# Process document endpoint
@app.post("/process-document")
async def process_document(request: DocumentRequest):
    """Process a document and extract transactions"""
    start_time = time.time()
    logger.info(f"Processing document: {request.document_id} from path: {request.file_path}")
    
    try:
        # Get Supabase client
        supabase_client = get_supabase_client()
        if not supabase_client:
            raise HTTPException(status_code=500, detail="Supabase client initialization failed")
        
        # Download document from storage
        logger.info(f"Downloading file from Supabase storage: {request.file_path}")
        response = supabase_client.storage.from_(request.bucket_name).download(request.file_path)
        
        if not response or len(response) == 0:
            logger.error(f"Failed to download file or file is empty: {request.file_path}")
            raise HTTPException(status_code=404, detail="File not found or empty")
        
        logger.info(f"File downloaded successfully: {len(response)} bytes")
        
        # Extract transactions
        extraction_result = extract_transactions_from_pdf(
            pdf_bytes=response,
            options=request.extraction_options
        )
        
        # Calculate processing time
        processing_time = time.time() - start_time
        logger.info(f"Document processed in {processing_time:.2f} seconds. Found {len(extraction_result['transactions'])} transactions.")
        
        # Add processing metrics
        extraction_result['processing_time_ms'] = int(processing_time * 1000)
        extraction_result['transaction_count'] = len(extraction_result['transactions'])
        
        # Return results
        return {
            "success": True,
            "document_id": request.document_id,
            "file_path": request.file_path,
            "extraction_method": extraction_result['extraction_method'],
            "document_type": extraction_result['document_type'],
            "extraction_quality": extraction_result['quality'],
            "candidate_transactions": extraction_result['transactions'],
            "transaction_count": extraction_result['transaction_count'],
            "processing_time_ms": extraction_result['processing_time_ms']
        }
        
    except Exception as e:
        logger.error(f"Error processing document: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

def extract_transactions_from_pdf(pdf_bytes: bytes, options: ExtractionOptions) -> Dict[str, Any]:
    """Extract transactions from PDF using multiple methods and combine results"""
    logger.info("Starting transaction extraction process")
    
    # Initialize result structure
    result = {
        "transactions": [],
        "extraction_method": "combined",
        "document_type": "unknown",
        "quality": "medium", 
        "page_count": 0
    }
    
    try:
        # Open the PDF and get basic info
        pdf_document = fitz.open(stream=pdf_bytes, filetype="pdf")
        result['page_count'] = len(pdf_document)
        logger.info(f"PDF loaded successfully. Page count: {result['page_count']}")
        
        # Determine document type based on content (basic check)
        first_page_text = pdf_document[0].get_text() if result['page_count'] > 0 else ""
        
        if re.search(r'statement|account|bank|transaction', first_page_text.lower()):
            result['document_type'] = "bank_statement"
        elif re.search(r'invoice|receipt|bill', first_page_text.lower()):
            result['document_type'] = "invoice"
        else:
            result['document_type'] = "financial_document"
        
        logger.info(f"Document type identified as: {result['document_type']}")
        
        # Extract with multiple methods and combine results
        all_transactions = []
        
        # Method 1: Text extraction
        text_transactions = extract_transactions_from_text(pdf_document)
        logger.info(f"Text extraction found {len(text_transactions)} transactions")
        all_transactions.extend(text_transactions)
        
        # Method 2: Pattern-based extraction
        pattern_transactions = extract_transactions_with_patterns(pdf_document)
        logger.info(f"Pattern extraction found {len(pattern_transactions)} transactions")
        all_transactions.extend(pattern_transactions)
        
        # Method 3: OCR if enabled and needed
        is_scanned = is_scanned_document(pdf_document)
        if options.enable_ocr and (is_scanned or len(all_transactions) < 3):
            logger.info("Using OCR extraction (document appears to be scanned or text extraction yielded few results)")
            ocr_transactions = extract_transactions_with_ocr(pdf_bytes)
            logger.info(f"OCR extraction found {len(ocr_transactions)} transactions")
            all_transactions.extend(ocr_transactions)
            result['extraction_method'] = "combined-with-ocr"
        
        # Deduplicate and clean transactions
        result['transactions'] = deduplicate_transactions(all_transactions)
        logger.info(f"After deduplication: {len(result['transactions'])} unique transactions")
        
        # Determine extraction quality
        if len(result['transactions']) > 10:
            result['quality'] = "high"
        elif len(result['transactions']) > 3:
            result['quality'] = "medium"
        else:
            result['quality'] = "low"
            
        # Close the document
        pdf_document.close()
        
        return result
        
    except Exception as e:
        logger.error(f"Error in transaction extraction: {str(e)}", exc_info=True)
        # Return empty result with error info
        return {
            "transactions": [],
            "extraction_method": "failed",
            "document_type": "unknown",
            "quality": "error",
            "error": str(e),
            "page_count": 0
        }

def extract_transactions_from_text(pdf_document) -> List[Dict[str, Any]]:
    """Extract transactions using text extraction"""
    transactions = []
    
    # Common date patterns
    date_patterns = [
        r'\b\d{1,2}/\d{1,2}/\d{2,4}\b',               # MM/DD/YYYY or DD/MM/YYYY
        r'\b\d{1,2}-\d{1,2}-\d{2,4}\b',               # MM-DD-YYYY or DD-MM-YYYY
        r'\b\d{4}/\d{1,2}/\d{1,2}\b',                 # YYYY/MM/DD
        r'\b\d{4}-\d{1,2}-\d{1,2}\b',                 # YYYY-MM-DD
        r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{1,2},? \d{4}\b',  # Month DD, YYYY
        r'\b\d{1,2} (?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{4}\b'     # DD Month YYYY
    ]
    
    # Amount patterns
    amount_patterns = [
        r'\$\s*\d{1,3}(?:,\d{3})*\.\d{2}',           # $1,234.56
        r'\d{1,3}(?:,\d{3})*\.\d{2}\s*\$',           # 1,234.56$
        r'\$\s*\d+\.\d{2}',                          # $123.45
        r'\d+\.\d{2}\s*\$',                          # 123.45$
        r'\b\d{1,3}(?:,\d{3})*\.\d{2}\b',            # 1,234.56
        r'\b\d+\.\d{2}\b'                            # 123.45
    ]
    
    for page_num in range(len(pdf_document)):
        page = pdf_document[page_num]
        text = page.get_text()
        
        # Process each line to find transaction patterns
        lines = text.split('\n')
        for i, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue
                
            # Skip header rows
            if re.search(r'date|description|transaction|amount', line.lower()):
                continue
                
            # Try to find a date in the line
            date_match = None
            for pattern in date_patterns:
                date_match = re.search(pattern, line)
                if date_match:
                    break
                    
            # If no date found, continue to next line
            if not date_match:
                continue
                
            # Try to find an amount in the line
            amount_match = None
            for pattern in amount_patterns:
                amount_match = re.search(pattern, line)
                if amount_match:
                    break
                    
            # If no amount found, look in the next line
            amount_line = line
            if not amount_match and i + 1 < len(lines):
                amount_line = lines[i + 1]
                for pattern in amount_patterns:
                    amount_match = re.search(pattern, amount_line)
                    if amount_match:
                        break
                        
            # If still no amount found, continue to next line
            if not amount_match:
                continue
                
            # Extract description (everything except date and amount)
            date_str = date_match.group(0)
            amount_str = amount_match.group(0)
            description = line.replace(date_str, "").replace(amount_str, "").strip()
            
            # If description is empty, try to get it from previous or next line
            if not description and i > 0:
                description = lines[i - 1].strip()
            if not description and i + 1 < len(lines):
                description = lines[i + 1].strip()
                
            # Clean amount string
            amount_str = amount_str.replace("$", "").replace(",", "").strip()
            
            # Try to determine if it's a credit or debit
            is_credit = False
            if "credit" in line.lower() or "deposit" in line.lower() or "+" in line:
                is_credit = True
            elif "debit" in line.lower() or "withdrawal" in line.lower() or "-" in line:
                is_credit = False
            else:
                # Look for parentheses which often indicate negative amounts
                if "(" in amount_str and ")" in amount_str:
                    is_credit = False
                    amount_str = amount_str.replace("(", "").replace(")", "")
                else:
                    # If no clear indication, positive amounts are credits, negative are debits
                    is_credit = not amount_str.startswith("-")
                    amount_str = amount_str.replace("-", "")
            
            # Format amount with sign based on credit/debit
            formatted_amount = amount_str if is_credit else f"-{amount_str}"
            
            # Standardize date format to YYYY-MM-DD if possible
            try:
                # For simplicity, assume MM/DD/YYYY if in the format XX/XX/XXXX
                if re.match(r'\d{1,2}/\d{1,2}/\d{4}', date_str):
                    month, day, year = date_str.split('/')
                    date_str = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                elif re.match(r'\d{1,2}/\d{1,2}/\d{2}', date_str):
                    month, day, year = date_str.split('/')
                    year = f"20{year}" if int(year) < 50 else f"19{year}"  # Assume 20XX for years < 50
                    date_str = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                elif re.match(r'\d{4}-\d{1,2}-\d{1,2}', date_str):
                    # Already in YYYY-MM-DD format
                    pass
            except Exception:
                # If date parsing fails, keep original format
                pass
                
            # Create transaction entry
            transaction = {
                "date": date_str,
                "description": description[:100] if description else "Unknown",  # Truncate long descriptions
                "amount": formatted_amount,
                "category": "Other",  # Default category
                "confidence": 0.85  # Confidence score for text extraction method
            }
            
            transactions.append(transaction)
            
    return transactions

def extract_transactions_with_patterns(pdf_document) -> List[Dict[str, Any]]:
    """Extract transactions using regular expression patterns optimized for financial documents"""
    transactions = []
    
    # Transaction patterns for different document types
    bank_pattern = re.compile(
        r'(?P<date>\d{1,2}[/-]\d{1,2}[/-]\d{2,4})'  # Date
        r'(?:[\s,]+)'                              # Space or comma
        r'(?P<description>[^$\n]*?)'               # Description (non-greedy)
        r'(?:[\s,]+)'                              # Space or comma
        r'(?P<amount>-?\$?\s*\d{1,3}(?:,\d{3})*\.\d{2}|\$?\s*\d+\.\d{2})'  # Amount with optional $ sign
    )
    
    credit_card_pattern = re.compile(
        r'(?P<date>\d{1,2}[/-]\d{1,2}[/-]\d{2,4})'  # Date
        r'(?:[\s,]+)'                              # Space or comma
        r'(?P<description>.*?)'                    # Description (non-greedy)
        r'(?:[\s,]+)'                              # Space or comma
        r'(?P<amount>\$?\s*\d{1,3}(?:,\d{3})*\.\d{2}|\$?\s*\d+\.\d{2})'  # Amount
    )
    
    for page_num in range(len(pdf_document)):
        page = pdf_document[page_num]
        text = page.get_text()
        
        # Try bank pattern
        for match in bank_pattern.finditer(text):
            date = match.group('date')
            description = match.group('description').strip()
            amount = match.group('amount').replace('$', '').replace(',', '').strip()
            
            # Standardize date if possible
            try:
                if re.match(r'\d{1,2}/\d{1,2}/\d{4}', date):
                    month, day, year = date.split('/')
                    standardized_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                elif re.match(r'\d{1,2}/\d{1,2}/\d{2}', date):
                    month, day, year = date.split('/')
                    year = f"20{year}" if int(year) < 50 else f"19{year}"
                    standardized_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                else:
                    standardized_date = date
            except Exception:
                standardized_date = date
                
            transaction = {
                "date": standardized_date,
                "description": description,
                "amount": amount,
                "category": "Other",
                "confidence": 0.75
            }
            
            transactions.append(transaction)
            
        # Try credit card pattern
        for match in credit_card_pattern.finditer(text):
            date = match.group('date')
            description = match.group('description').strip()
            amount = match.group('amount').replace('$', '').replace(',', '').strip()
            
            # Add if not already added (avoid duplicates)
            duplicate = False
            for existing in transactions:
                if existing["date"] == date and existing["amount"] == amount:
                    duplicate = True
                    break
                    
            if not duplicate:
                try:
                    if re.match(r'\d{1,2}/\d{1,2}/\d{4}', date):
                        month, day, year = date.split('/')
                        standardized_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                    elif re.match(r'\d{1,2}/\d{1,2}/\d{2}', date):
                        month, day, year = date.split('/')
                        year = f"20{year}" if int(year) < 50 else f"19{year}"
                        standardized_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                    else:
                        standardized_date = date
                except Exception:
                    standardized_date = date
                    
                transaction = {
                    "date": standardized_date,
                    "description": description,
                    "amount": amount,
                    "category": "Other",
                    "confidence": 0.7
                }
                
                transactions.append(transaction)
                
    return transactions

def extract_transactions_with_ocr(pdf_bytes: bytes) -> List[Dict[str, Any]]:
    """Extract transactions using OCR for scanned documents"""
    try:
        logger.info("Starting OCR extraction")
        transactions = []
        
        # Convert PDF to images
        images = pdf2image.convert_from_bytes(pdf_bytes, dpi=300)
        logger.info(f"Converted PDF to {len(images)} images for OCR")
        
        # Process each page image with OCR
        for i, image in enumerate(images):
            # Convert to grayscale for better OCR
            gray_image = image.convert('L')
            
            # Perform OCR
            ocr_text = pytesseract.image_to_string(gray_image)
            
            logger.debug(f"OCR extracted {len(ocr_text)} characters from page {i+1}")
            
            # Extract transactions from OCR text using line-by-line analysis
            lines = ocr_text.split('\n')
            
            # Common date patterns in OCR text
            date_patterns = [
                r'\b\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4}\b',  # MM/DD/YYYY or DD/MM/YYYY
                r'\b\d{4}[/.-]\d{1,2}[/.-]\d{1,2}\b',    # YYYY/MM/DD
            ]
            
            # Amount patterns in OCR text
            amount_patterns = [
                r'\$\s*\d{1,3}(?:,\d{3})*\.\d{2}',     # $1,234.56
                r'\d{1,3}(?:,\d{3})*\.\d{2}\s*\$',     # 1,234.56$
                r'\$\s*\d+\.\d{2}',                    # $123.45
                r'\d+\.\d{2}\s*\$',                    # 123.45$
                r'\b\d{1,3}(?:,\d{3})*\.\d{2}\b',      # 1,234.56
                r'\b\d+\.\d{2}\b'                      # 123.45
            ]
            
            # Analyze each line for potential transactions
            for j, line in enumerate(lines):
                line = line.strip()
                if not line or len(line) < 5:
                    continue
                    
                # Look for transaction markers like dates
                date_found = False
                date_match = None
                date_text = None
                
                for pattern in date_patterns:
                    date_match = re.search(pattern, line)
                    if date_match:
                        date_found = True
                        date_text = date_match.group(0)
                        break
                        
                if not date_found:
                    continue
                    
                # Look for amount in same line or next line
                amount_match = None
                amount_text = None
                amount_line = line
                
                for pattern in amount_patterns:
                    amount_match = re.search(pattern, line)
                    if amount_match:
                        amount_text = amount_match.group(0)
                        break
                        
                # If no amount in this line, check next line
                if not amount_match and j + 1 < len(lines):
                    amount_line = lines[j + 1]
                    for pattern in amount_patterns:
                        amount_match = re.search(pattern, amount_line)
                        if amount_match:
                            amount_text = amount_match.group(0)
                            break
                            
                if not amount_match or not amount_text:
                    continue
                    
                # Extract description (everything else in the line)
                description = line.replace(date_text, "").replace(amount_text, "").strip()
                
                # If description is empty, look at adjacent lines
                if not description:
                    # Check line before if exists
                    if j > 0:
                        description = lines[j-1].strip()
                    # If still empty check line after if exists
                    if not description and j + 2 < len(lines):
                        description = lines[j+2].strip()
                    # If still empty, use a placeholder
                    if not description:
                        description = "Transaction"
                        
                # Clean amount (remove $ and commas)
                amount_str = amount_text.replace("$", "").replace(",", "").strip()
                
                # Determine if credit or debit
                is_credit = False
                if "credit" in line.lower() or "deposit" in line.lower() or "+" in line:
                    is_credit = True
                elif "debit" in line.lower() or "withdrawal" in line.lower() or "-" in line:
                    is_credit = False
                else:
                    # Look for common debit indicators
                    if "(" in amount_str and ")" in amount_str:
                        is_credit = False
                        amount_str = amount_str.replace("(", "").replace(")", "")
                    else:
                        # Default assumption: positive is credit, negative is debit
                        is_credit = not amount_str.startswith("-")
                        amount_str = amount_str.replace("-", "")
                
                # Format amount with sign
                formatted_amount = amount_str if is_credit else f"-{amount_str}"
                
                # Try to standardize date (simple approach)
                standardized_date = date_text
                try:
                    if re.match(r'\d{1,2}/\d{1,2}/\d{4}', date_text):
                        month, day, year = date_text.split('/')
                        standardized_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                    elif re.match(r'\d{1,2}-\d{1,2}-\d{4}', date_text):
                        month, day, year = date_text.split('-')
                        standardized_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                except Exception:
                    # Keep original if parsing fails
                    pass
                
                # Add transaction
                transaction = {
                    "date": standardized_date,
                    "description": description[:100],  # Limit description length
                    "amount": formatted_amount,
                    "category": "Other",
                    "confidence": 0.6  # Lower confidence for OCR method
                }
                
                transactions.append(transaction)
                
        logger.info(f"OCR extraction completed, found {len(transactions)} potential transactions")
        return transactions
        
    except Exception as e:
        logger.error(f"Error in OCR extraction: {str(e)}", exc_info=True)
        return []  # Return empty list on error

def is_scanned_document(pdf_document) -> bool:
    """Check if the document appears to be scanned (image-based) rather than digital"""
    # Basic heuristic: check if the first few pages have very little text
    text_chars = 0
    pages_to_check = min(3, len(pdf_document))
    
    for i in range(pages_to_check):
        text_chars += len(pdf_document[i].get_text().strip())
    
    # If there's very little text, it's likely a scanned document
    avg_chars_per_page = text_chars / pages_to_check if pages_to_check > 0 else 0
    return avg_chars_per_page < 500  # Threshold for determining scanned doc

def deduplicate_transactions(transactions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Remove duplicate transactions based on date, amount, and similar descriptions"""
    if not transactions:
        return []
        
    unique_transactions = []
    seen_keys = set()
    
    for transaction in transactions:
        # Create a key for comparison (date + amount + first 10 chars of description)
        date = transaction.get('date', '')
        amount = transaction.get('amount', '')
        description = transaction.get('description', '')[:10].lower()  # First 10 chars, lowercase
        
        comparison_key = f"{date}_{amount}_{description}"
        
        if comparison_key not in seen_keys:
            seen_keys.add(comparison_key)
            
            # Take the transaction with highest confidence if available
            existing_indices = []
            for i, unique_tx in enumerate(unique_transactions):
                u_date = unique_tx.get('date', '')
                u_amount = unique_tx.get('amount', '')
                u_desc = unique_tx.get('description', '')[:10].lower()
                
                if u_date == date and u_amount == amount and u_desc == description:
                    existing_indices.append(i)
                    
            if existing_indices:
                # Compare confidence and keep the one with higher confidence
                current_confidence = transaction.get('confidence', 0)
                for idx in existing_indices:
                    existing_confidence = unique_transactions[idx].get('confidence', 0)
                    if current_confidence > existing_confidence:
                        unique_transactions[idx] = transaction
            else:
                unique_transactions.append(transaction)
                
    return unique_transactions

if __name__ == "__main__":
    # For local development
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
