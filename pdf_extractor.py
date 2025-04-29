"""
PDF Transaction Extractor Module
Extracts transaction data from financial PDFs (bank & credit-card statements)
with an OCR fallback for empty/CID-encoded/garbled pages, plus support for
Turkish month-name dates and Akbank’s “+”-suffix on TL amounts.
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

DEFAULT_CURRENCY = os.getenv("DEFAULT_CURRENCY", "TRY")

# 1) Bank statements: row#, DD/MM/YYYY, desc, amount, balance
ACCOUNT_LINE_RE = re.compile(
    r'^\s*\d+\s+'
    r'(\d{2}/\d{2}/\d{4})\s+'
    r'(.+?)\s+'
    r'(-?\d{1,3}(?:\.\d{3})*,\d{2})\s+'
    r'(-?\d{1,3}(?:\.\d{3})*,\d{2})\s*$'
)

# 2) Credit cards ending in “TL”, with optional “+”
CREDIT_TL_RE = re.compile(
    r'^\s*(\d{2}/\d{2}/\d{4})\s+'
    r'(.+?)\s+'
    r'(-?\d{1,3}(?:\.\d{3})*,\d{2})\s*TL\+?\s*$'
)

# 3) Credit cards with multiple trailing numeric columns, allow trailing “+”
CREDIT_MULTI_RE = re.compile(
    r'^\s*(\d{2}/\d{2}/\d{4})\s*'
    r'(.+?)\s+'
    r'(-?\d{1,3}(?:\.\d{3})*,\d{2})'
    r'(?:\s+[0-9]{1,3}(?:\.\d{3})*,\d{2})+\+?\s*$'
)

# 4) Turkish month-name dates, e.g. "10 Kasım 2024 … 180,00+"
MONTHS = {
    "Ocak":1, "Şubat":2, "Mart":3, "Nisan":4, "Mayıs":5, "Haziran":6,
    "Temmuz":7, "Ağustos":8, "Eylül":9, "Ekim":10, "Kasım":11, "Aralık":12
}
_month_names = "|".join(MONTHS.keys())
TEXT_DATE_RE = re.compile(
    rf'^\s*(\d{{1,2}})\s+({_month_names})\s+(\d{{4}})\s+(.+?)\s+'
    r'(-?\d{1,3}(?:\.\d{3})*,\d{2})\+?\s*$'
)

def extract_transactions(pdf_path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(pdf_path) or not os.access(pdf_path, os.R_OK):
        logger.error(f"Cannot read PDF: {pdf_path}")
        return []

    results: List[Dict[str, Any]] = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            logger.info(f"Opened PDF with {len(pdf.pages)} pages: {pdf_path}")

            for page_num, page in enumerate(pdf.pages, start=1):
                raw = page.extract_text() or ""
                # detect pages that need OCR:
                needs_ocr = (
                    not raw.strip()
                    or raw.count("(cid:") > 5
                    or "�" in raw
                )
                if needs_ocr:
                    logger.info(f"Page {page_num}: falling back to OCR")
                    img = page.to_image(resolution=300).original
                    raw = pytesseract.image_to_string(img, lang="tur+eng")

                for line in raw.splitlines():
                    txt = line.strip()
                    if not txt:
                        continue

                    date_str = desc = amt_str = None

                    # try bank-style line
                    m = ACCOUNT_LINE_RE.match(txt)
                    if m:
                        date_str, desc, amt_str = m.group(1), m.group(2), m.group(3)
                    else:
                        # try simple TL-terminated credit
                        m = CREDIT_TL_RE.match(txt)
                        if m:
                            date_str, desc, amt_str = m.group(1), m.group(2), m.group(3)
                        else:
                            # try multi-column credit
                            m = CREDIT_MULTI_RE.match(txt)
                            if m:
                                date_str, desc, amt_str = m.group(1), m.group(2), m.group(3)
                            else:
                                # try Turkish-month names
                                m2 = TEXT_DATE_RE.match(txt)
                                if m2:
                                    day, mon, yr, desc, amt_str = (
                                        m2.group(1), m2.group(2), m2.group(3),
                                        m2.group(4), m2.group(5)
                                    )
                                    dt = datetime(int(yr), MONTHS[mon], int(day))
                                    date_str = dt.strftime("%Y-%m-%d")

                    if not (date_str and desc and amt_str):
                        continue

                    # normalize DD/MM/YYYY → YYYY-MM-DD
                    if "/" in date_str:
                        try:
                            dt = datetime.strptime(date_str, "%d/%m/%Y")
                            date_str = dt.strftime("%Y-%m-%d")
                        except ValueError:
                            pass

                    # strip any lingering “+”
                    amt_str = amt_str.rstrip("+")
                    # convert Turkish-style number to float
                    amount = float(amt_str.replace(".", "").replace(",", "."))

                    results.append({
                        "date":        date_str,
                        "description": desc.strip(),
                        "amount":      amount,
                        "currency":    DEFAULT_CURRENCY,
                        "confidence":  0.8
                    })

        logger.info(f"Extracted {len(results)} transactions from {pdf_path}")
        return results

    except Exception as e:
        logger.exception(f"Error extracting transactions from {pdf_path}: {e}")
        return []
