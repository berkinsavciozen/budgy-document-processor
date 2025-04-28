"""
PDF Transaction Extractor Module
Extracts transaction data from financial PDFs (bank statements & credit cards)
"""
import logging
import re
import os
from datetime import datetime
from typing import List, Dict, Any
import pdfplumber

logger = logging.getLogger("budgy-document-processor.pdf_extractor")

# Default currency (e.g. “TRY”, “USD”…)
DEFAULT_CURRENCY = os.getenv("DEFAULT_CURRENCY", "TRY")

# ---- Patterns ----

# 1) Standard bank statement lines:
#    1 02/07/2024 EPOS PARAM/... -696,44 -10.221,81
ACCOUNT_LINE_RE = re.compile(
    r'^\s*\d+\s+'                             # row number
    r'(\d{2}/\d{2}/\d{4})\s+'                 # date DD/MM/YYYY
    r'(.+?)\s+'                               # description
    r'(-?\d{1,3}(?:\.\d{3})*,\d{2})\s+'       # amount
    r'(-?\d{1,3}(?:\.\d{3})*,\d{2})$'         # balance (ignored)
)

# 2) Credit-card statement lines ending in “TL”:
#    06/10/2024 ÖDEME … -10.000,00 TL
CREDIT_LINE_RE = re.compile(
    r'^\s*(\d{2}/\d{2}/\d{4})\s+'             # date DD/MM/YYYY
    r'(.+?)\s+'                               # description
    r'(-?\d{1,3}(?:\.\d{3})*,\d{2})\s*TL$'     # amount + “TL”
)

def extract_transactions(pdf_path: str) -> List[Dict[str, Any]]:
    """
    Extract transactions from a PDF file.

    Returns a list of dicts with:
      - date:        'DD/MM/YYYY'
      - description: text
      - amount:      float
      - currency:    DEFAULT_CURRENCY
      - confidence:  0.8
    """
    logger.info(f"Extracting transactions from PDF: {pdf_path}")
    if not os.path.exists(pdf_path) or not os.access(pdf_path, os.R_OK):
        logger.error(f"Cannot read PDF: {pdf_path}")
        return []

    results: List[Dict[str, Any]] = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            logger.info(f"Opened PDF ({len(pdf.pages)} pages)")

            for pg, page in enumerate(pdf.pages, start=1):
                text = page.extract_text() or ""
                if not text.strip():
                    logger.warning(f"No text on page {pg}")
                    continue

                for line in text.splitlines():
                    line = line.strip()
                    m = ACCOUNT_LINE_RE.match(line) or CREDIT_LINE_RE.match(line)
                    if not m:
                        continue

                    date_str, desc, amt_str = m.groups()[:3]

                    # Normalize date
                    try:
                        parsed = datetime.strptime(date_str, "%d/%m/%Y")
                        date_out = parsed.strftime("%d/%m/%Y")
                    except ValueError:
                        date_out = date_str

                    # Normalize amount: “1.234,56” → 1234.56
                    amt = float(amt_str.replace(".", "").replace(",", "."))

                    results.append({
                        "date":        date_out,
                        "description": desc.strip(),
                        "amount":      amt,
                        "currency":    DEFAULT_CURRENCY,
                        "confidence":  0.8
                    })

        logger.info(f"Total extracted: {len(results)} transactions")
        return results

    except Exception as e:
        logger.exception(f"Error during extraction: {e}")
        return []
