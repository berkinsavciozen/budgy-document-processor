import os
import logging
import time
from typing import Optional, Dict, List, Any
from fastapi import FastAPI, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import fitz  # PyMuPDF
import pdf2image
import pytesseract
from PIL import Image
import re
from supabase import create_client, Client

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("budgy-processor")

# FastAPI app
app = FastAPI(title="Budgy Document Processor")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Supabase client
def get_supabase_client() -> Optional[Client]:
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_KEY")
    if not url or not key:
        logger.warning("Supabase credentials not found")
        return None
    try:
        return create_client(url, key)
    except Exception as e:
        logger.error(f"Supabase init failed: {e}")
        return None

# Models
class ExtractionOptions(BaseModel):
    enable_ocr: bool = True
    enable_table_detection: bool = True
    page_limit: int = 0
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

# Health check
@app.get("/health")
async def health_check():
    supabase_client = get_supabase_client()
    status = "connected" if supabase_client else "disconnected"
    return {
        "status": "healthy",
        "service": "budgy-document-processor",
        "supabase": status,
        "version": "1.0.0"
    }

# Document processor endpoint
@app.post("/process-document")
async def process_document(request: DocumentRequest):
    start = time.time()
    logger.info(f"Processing: {request.document_id}")
    client = get_supabase_client()
    if not client:
        raise HTTPException(status_code=500, detail="Supabase client failed")

    try:
        content = client.storage.from_(request.bucket_name).download(request.file_path)
        if not content:
            raise HTTPException(status_code=404, detail="File not found or empty")
        logger.info(f"Downloaded {len(content)} bytes")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed download: {e}")

    result = extract_transactions_from_pdf(content, request.extraction_options)
    result["processing_time_ms"] = int((time.time() - start) * 1000)
    result["transaction_count"] = len(result["transactions"])

    return {
        "success": True,
        "document_id": request.document_id,
        "file_path": request.file_path,
        **result
    }

# Extraction Logic
def extract_transactions_from_pdf(pdf_bytes: bytes, options: ExtractionOptions) -> Dict[str, Any]:
    logger.info("Starting extraction")
    result = {
        "transactions": [],
        "extraction_method": "combined",
        "document_type": "unknown",
        "quality": "medium",
        "page_count": 0
    }

    try:
        pdf = fitz.open(stream=pdf_bytes, filetype="pdf")
        result["page_count"] = len(pdf)
        first_text = pdf[0].get_text() if result["page_count"] > 0 else ""
        if re.search(r'statement|account|bank|transaction', first_text.lower()):
            result["document_type"] = "bank_statement"
        elif re.search(r'invoice|receipt|bill', first_text.lower()):
            result["document_type"] = "invoice"
        else:
            result["document_type"] = "financial_document"

        all_transactions = []
        all_transactions += extract_from_text(pdf)
        all_transactions += extract_with_patterns(pdf)

        if options.enable_ocr and (is_scanned(pdf) or len(all_transactions) < 3):
            all_transactions += extract_with_ocr(pdf_bytes)
            result["extraction_method"] = "combined-with-ocr"

        result["transactions"] = deduplicate(all_transactions)
        result["quality"] = (
            "high" if len(result["transactions"]) > 10 else
            "medium" if len(result["transactions"]) > 3 else
            "low"
        )
        pdf.close()
        return result
    except Exception as e:
        logger.error(f"Extraction error: {e}", exc_info=True)
        return {**result, "extraction_method": "failed", "quality": "error", "transactions": []}

def extract_from_text(pdf) -> List[Dict[str, Any]]:
    transactions = []
    date_pattern = r'\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b'
    amount_pattern = r'-?\d+\.\d{2}'
    for page in pdf:
        for line in page.get_text().split("\n"):
            line = line.strip()
            date_match = re.search(date_pattern, line)
            amount_match = re.search(amount_pattern, line)
            if date_match and amount_match:
                try:
                    date = date_match.group(0)
                    amount = amount_match.group(0)
                    desc = line.replace(date, "").replace(amount, "").strip()
                    transactions.append({
                        "date": standardize_date(date),
                        "description": desc or "Transaction",
                        "amount": amount,
                        "category": "Other",
                        "confidence": 0.85
                    })
                except Exception:
                    continue
    return transactions

def extract_with_patterns(pdf) -> List[Dict[str, Any]]:
    pattern = re.compile(r'(?P<date>\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\s+(?P<desc>.*?)\s+(?P<amount>-?\d+\.\d{2})')
    transactions = []
    for page in pdf:
        for match in pattern.finditer(page.get_text()):
            transactions.append({
                "date": standardize_date(match.group('date')),
                "description": match.group('desc'),
                "amount": match.group('amount'),
                "category": "Other",
                "confidence": 0.75
            })
    return transactions

def extract_with_ocr(pdf_bytes) -> List[Dict[str, Any]]:
    transactions = []
    images = pdf2image.convert_from_bytes(pdf_bytes, dpi=300)
    for img in images:
        text = pytesseract.image_to_string(img.convert('L'))
        for line in text.split("\n"):
            parts = line.strip().split()
            if len(parts) >= 3:
                try:
                    date = parts[0]
                    amount = parts[-1].replace(",", "").replace("TL", "").replace("$", "")
                    desc = " ".join(parts[1:-1])
                    transactions.append({
                        "date": standardize_date(date),
                        "description": desc,
                        "amount": amount,
                        "category": "Other",
                        "confidence": 0.6
                    })
                except Exception:
                    continue
    return transactions

def standardize_date(date_str: str) -> str:
    try:
        if "/" in date_str:
            parts = date_str.split("/")
        else:
            parts = date_str.split("-")
        if len(parts[-1]) == 2:
            parts[-1] = "20" + parts[-1]
        return f"{parts[2]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"
    except Exception:
        return date_str

def is_scanned(pdf) -> bool:
    char_count = sum(len(pdf[i].get_text().strip()) for i in range(min(3, len(pdf))))
    return char_count / max(1, min(3, len(pdf))) < 500

def deduplicate(transactions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    unique = []
    for t in transactions:
        key = f"{t['date']}_{t['amount']}_{t['description'][:10].lower()}"
        if key not in seen:
            seen.add(key)
            unique.append(t)
    return unique
