"""
PDF Transaction Extractor Module
Extracts transaction data from financial PDFs (bank & credit-card statements)
with an OCR fallback for CID-encoded fonts.
"""
import logging
import re
import os
from datetime import datetime
from typing import List, Dict, Any

import pdfplumber
from PIL import Image
import pytesseract

logger = logging.getLogger("budgy-document-processor.pdf_extractor")

# Default currency code (e.g. "TRY", "USD", etc.)
DEFAULT_CURRENCY = os.getenv("DEFAULT_CURRENCY", "TRY")

# 1) Bank-statement lines (amount + balance):
#    1 02/07/2024 DESC… -696,44    -10.221,81
ACCOUNT_LINE_RE = re.compile(
    r'^\s*\d+\s+'                             # row no.
    r'(\d{2}/\d{2}/\d{4})\s+'                 # date DD/MM/YYYY
    r'(.+?)\s+'                               # description (non-greedy)
    r'(-?\d{1,3}(?:\.\d{3})*,\d{2})\s+'       # amount
    r'(-?\d{1,3}(?:\.\d{3})*,\d{2})\s*$'      # balance (ignored)
)

# 2) Credit-card lines (amount + installment + maxipuan):
#    15/08/2024 DESC…  519,50 0,05
CREDIT_LINE_RE = re.compile(
    r'^\s*(\d{2}/\d{2}/\d{4})\s*'
    r'(.+?)\s+'
    r'(-?\d{1,3}(?:\.\d{3})*,\d{2})'
    r'(?:\s+[0-9]{1,3}(?:\.\d{3})*,\d{2})+'
    r'\s*$'
)

def extract_transactions(pdf_path: str) -> List[Dict[str, Any]]:
    """
    Extracts transactions from a PDF.

    Returns:
      - date:       ISO 'YYYY-MM-DD'
      - description
      - amount:     float
      - currency:   DEFAULT_CURRENCY
      - confidence: 0.8
    """
    if not os.path.exists(pdf_path) or not os.access(pdf_path, os.R_OK):
        logger.error(f"Cannot read PDF: {pdf_path}")
        return []

    results: List[Dict[str, Any]] = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            logger.info(f"Opened PDF with {len(pdf.pages)} pages")
            for page_num, page in enumerate(pdf.pages, start=1):
                # First try native text
                raw_text = page.extract_text() or ""
                # If garbage (CID markers), fallback to OCR
                if not raw_text.strip() or raw_text.count("(cid:") > 5:
                    logger.info(f"Page {page_num}: falling back to OCR")
                    pil_img = page.to_image(resolution=300).original
                    raw_text = pytesseract.image_to_string(pil_img, lang="tur+eng")

                for line in raw_text.splitlines():
                    text = line.strip()
                    m = ACCOUNT_LINE_RE.match(text) or CREDIT_LINE_RE.match(text)
                    if not m:
                        continue

                    date_str, desc, amt_str = m.group(1), m.group(2), m.group(3)

                    # Normalize date → YYYY-MM-DD
                    try:
                        dt = datetime.strptime(date_str, "%d/%m/%Y")
                        date_out = dt.strftime("%Y-%m-%d")
                    except ValueError:
                        date_out = date_str

                    # Normalize amount: "1.234,56" → 1234.56
                    amount = float(amt_str.replace(".", "").replace(",", "."))

                    results.append({
                        "date":        date_out,
                        "description": desc,
                        "amount":      amount,
                        "currency":    DEFAULT_CURRENCY,
                        "confidence":  0.8
                    })

        logger.info(f"Extracted total {len(results)} transactions from {pdf_path}")
        return results

    except Exception as e:
        logger.exception(f"Error extracting transactions: {e}")
        return []
