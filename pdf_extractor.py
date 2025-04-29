"""
PDF Transaction Extractor Module
Extracts transaction data from financial PDFs, including account statements and credit-card statements.
"""
import logging
import re
import os
from datetime import datetime
from typing import List, Dict, Any

import pdfplumber
import pytesseract

logger = logging.getLogger("budgy-document-processor.pdf_extractor")

# ----------------------------------------------------------------------------------------------------------------------
# PATTERNS
# ----------------------------------------------------------------------------------------------------------------------

# 1) Tabular account statements (e.g. YPK_Account_July) – date MM/DD/YYYY or DD/MM/YYYY, description, amount, balance
ACCOUNT_LINE_RE = re.compile(
    r'(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\s+(.+?)\s+(-?\d{1,3}(?:\.\d{3})*,\d{2})\s+(-?\d{1,3}(?:\.\d{3})*,\d{2})'
)

# 2) Amount pattern for credit-card style lines; matches e.g. “1.039,00 TL”, “246,00+”, “-25.794,95”
AMOUNT_PATTERN = re.compile(r'(-?\d{1,3}(?:\.\d{3})*,\d{2})(?:\s*TL)?\+?')

# 3) Date pattern: either numeric DD/MM/YYYY or Turkish “10 Kasım 2024”
DATE_PATTERN = re.compile(
    r'(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|' +
    r'\d{1,2}\s+(?:Ocak|Şubat|Mart|Nisan|Mayıs|Haziran|Temmuz|Ağustos|Eylül|Ekim|Kasım|Aralık)\s+\d{4})',
    flags=re.IGNORECASE
)

# Map Turkish month names to month numbers
MONTH_MAP = {
    'ocak': '01', 'şubat': '02', 'mart': '03', 'nisan': '04',
    'mayıs': '05', 'haziran': '06', 'temmuz': '07', 'ağustos': '08',
    'eylül': '09', 'ekim': '10', 'kasım': '11', 'aralık': '12'
}


def extract_transactions(pdf_path: str) -> List[Dict[str, Any]]:
    """
    Extract transactions from a PDF file (both tabular account statements and
    free-form credit-card statements).
    """
    logger.info(f"Extracting transactions from PDF: {pdf_path}")

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
                text = page.extract_text() or ""
                page_transactions = []

                # --- 1) First attempt: structured, tabular statements ---
                lines = text.split('\n')
                for line in lines:
                    m = ACCOUNT_LINE_RE.search(line)
                    if not m:
                        continue

                    date_str, desc, amt_str, _ = m.groups()
                    # parse date
                    for fmt in ('%d/%m/%Y', '%m/%d/%Y', '%d-%m-%Y', '%Y-%m-%d'):
                        try:
                            dt = datetime.strptime(date_str, fmt)
                            break
                        except ValueError:
                            dt = None
                    formatted_date = dt.strftime('%Y-%m-%d') if dt else date_str

                    # normalize amount
                    amount = amt_str.replace('.', '').replace(',', '.')
                    page_transactions.append({
                        "date": formatted_date,
                        "description": desc.strip(),
                        "amount": amount,
                        "confidence": 0.9
                    })

                # --- 2) If nothing found in tabular form, fallback to OCR + free-form parsing ---
                if not page_transactions:
                    logger.info("No tabular transactions found, falling back to OCR on page")
                    img = page.to_image(resolution=300).original
                    ocr_text = pytesseract.image_to_string(img, lang='tur')
                    for line in ocr_text.split('\n'):
                        if not line.strip():
                            continue

                        # find date
                        dm = DATE_PATTERN.search(line)
                        if not dm:
                            continue
                        date_str = dm.group(0).strip()

                        # find amount
                        am = AMOUNT_PATTERN.search(line)
                        if not am:
                            continue
                        amt_str = am.group(1)

                        # parse date: numeric or Turkish month
                        if '/' in date_str or '-' in date_str:
                            for fmt in ('%d/%m/%Y', '%m/%d/%Y', '%d-%m-%Y', '%Y-%m-%d'):
                                try:
                                    dt = datetime.strptime(date_str, fmt)
                                    break
                                except ValueError:
                                    dt = None
                        else:
                            parts = date_str.split()
                            day, mon_name, year = parts
                            mon_num = MONTH_MAP.get(mon_name.lower(), '01')
                            dt = datetime(int(year), int(mon_num), int(day))
                        formatted_date = dt.strftime('%Y-%m-%d') if dt else date_str

                        # normalize amount to decimal string
                        amount = amt_str.replace('.', '').replace(',', '.')

                        # extract description by removing date and amount from the line
                        desc = line
                        desc = desc.replace(date_str, '').strip()
                        desc = AMOUNT_PATTERN.sub('', desc).strip().rstrip('+')

                        page_transactions.append({
                            "date": formatted_date,
                            "description": desc,
                            "amount": amount,
                            "confidence": 0.8
                        })

                logger.info(f"Found {len(page_transactions)} transactions on page {page_num}")
                transactions.extend(page_transactions)

        logger.info(f"Total extracted transactions: {len(transactions)}")
        return transactions

    except Exception as e:
        logger.exception(f"Error extracting transactions from PDF: {e}")
        return []
