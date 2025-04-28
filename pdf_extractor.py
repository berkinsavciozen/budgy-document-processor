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

# Read the default currency from an env var (e.g. "TRY", "USD", ...)
DEFAULT_CURRENCY = os.getenv("DEFAULT_CURRENCY", "TRY")

# Matches lines like:
#    1 02/07/2024 EPOS PARAM/... -696,44 -10.221,81
LINE_RE = re.compile(
    r'^\s*\d+\s+'                             # row number
    r'(\d{2}/\d{2}/\d{4})\s+'                 # date DD/MM/YYYY
    r'(.+?)\s+'                               # description (non-greedy)
    r'(-?\d{1,3}(?:\.\d{3})*,\d{2})\s+'       # amount with dot-thousands + comma-decimal
    r'(-?\d{1,3}(?:\.\d{3})*,\d{2})$'         # balance (ignored)
)

def extract_transactions(pdf_path: str) -> List[Dict[str, Any]]:
    """
    Extract transactions from a PDF file.

    Args:
        pdf_path: Path to the PDF file

    Returns:
        List of transaction dicts, each with:
          - date:        formatted as 'DD/MM/YYYY'
          - description: merchant or transaction text
          - amount:      float (negative for debits)
          - currency:    default currency code (from DEFAULT_CURRENCY)
          - confidence:  simple confidence score
    """
    logger.info(f"Extracting transactions from PDF: {pdf_path}")

    # File existence & readability checks
    if not os.path.exists(pdf_path):
        logger.error(f"PDF file not found: {pdf_path}")
        return []
    if not os.access(pdf_path, os.R_OK):
        logger.error(f"PDF file not readable: {pdf_path}")
        return []

    transactions: List[Dict[str, Any]] = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            logger.info(f"Opened PDF with {len(pdf.pages)} pages")

            for page_num, page in enumerate(pdf.pages, start=1):
                logger.info(f"Processing page {page_num}")
                text = page.extract_text()
                if not text or not text.strip():
                    logger.warning(f"No text extracted from page {page_num}")
                    continue

                for line in text.split("\n"):
                    m = LINE_RE.match(line)
                    if not m:
                        continue

                    date_str, description, amount_str, _balance = m.groups()

                    # Normalize date → DD/MM/YYYY
                    try:
                        parsed_date = datetime.strptime(date_str, "%d/%m/%Y")
                        date_out = parsed_date.strftime("%d/%m/%Y")
                    except ValueError:
                        date_out = date_str  # fallback
