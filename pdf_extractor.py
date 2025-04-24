
"""
PDF Transaction Extractor Module
Extracts transaction data from financial PDFs
"""
import logging
import re
import os
from datetime import datetime
from typing import List, Dict, Any
import pdfplumber

logger = logging.getLogger("budgy-document-processor.pdf_extractor")

def extract_transactions(pdf_path: str) -> List[Dict[str, Any]]:
    """
    Extract transactions from a PDF file.
    
    Args:
        pdf_path: Path to the PDF file
        
    Returns:
        List of transaction dictionaries
    """
    logger.info(f"Extracting transactions from PDF: {pdf_path}")
    
    # Check if file exists
    if not os.path.exists(pdf_path):
        logger.error(f"PDF file not found: {pdf_path}")
        return []
    
    # Check if file is readable
    if not os.access(pdf_path, os.R_OK):
        logger.error(f"PDF file not readable: {pdf_path}")
        return []
    
    transactions = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            logger.info(f"Opened PDF with {len(pdf.pages)} pages")
            
            for page_num, page in enumerate(pdf.pages):
                logger.info(f"Processing page {page_num + 1}")
                text = page.extract_text()
                
                if text is None or text.strip() == "":
                    logger.warning(f"No text extracted from page {page_num + 1}")
                    continue
                
                # Log a sample of the extracted text for debugging
                logger.debug(f"Page {page_num + 1} sample text: {text[:500]}")
                
                # Look for transaction patterns
                # This is a simple example - adjust the patterns based on your PDF formats
                lines = text.split('\n')
                
                for line in lines:
                    # Try to match date patterns (e.g., YYYY-MM-DD, MM/DD/YYYY, etc.)
                    date_match = re.search(r'(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}[/-]\d{1,2}[/-]\d{1,2})', line)
                    # Look for currency patterns (e.g., $100.00, 100.00 USD, etc.)
                    amount_match = re.search(r'(\$|\€|\£)?(\d+,\d+\.\d{2}|\d+\.\d{2}|\d+,\d{3}\.\d{2}|\d+)', line)
                    
                    if date_match and amount_match:
                        # Extract the date and amount
                        date_str = date_match.group(0)
                        amount_str = amount_match.group(0)
                        
                        # Extract the description (everything else in the line)
                        description = line.replace(date_str, "").replace(amount_str, "").strip()
                        
                        # Remove common separators from description
                        description = re.sub(r'^\s*[,;:\-\*]\s*', '', description)
                        
                        # Format the date consistently
                        try:
                            # Attempt to parse the date
                            date_formats = ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y/%m/%d']
                            parsed_date = None
                            
                            for date_format in date_formats:
                                try:
                                    parsed_date = datetime.strptime(date_str, date_format)
                                    break
                                except ValueError:
                                    continue
                            
                            if parsed_date:
                                formatted_date = parsed_date.strftime('%Y-%m-%d')
                            else:
                                formatted_date = date_str
                                
                        except Exception as e:
                            logger.warning(f"Could not parse date '{date_str}': {e}")
                            formatted_date = date_str
                        
                        # Add the transaction
                        transaction = {
                            "date": formatted_date,
                            "description": description,
                            "amount": amount_str,
                            "confidence": 0.8  # Simple confidence score
                        }
                        
                        transactions.append(transaction)
                        logger.debug(f"Found transaction: {transaction}")
            
            logger.info(f"Extracted {len(transactions)} transactions from {pdf_path}")
            return transactions
    except Exception as e:
        logger.exception(f"Error extracting transactions from PDF: {e}")
        return []
