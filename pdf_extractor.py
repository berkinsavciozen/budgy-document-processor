"""
PDF Transaction Extractor Module
Extracts transaction data from financial PDFs (bank & credit-card statements)
with an OCR fallback for empty/CID-encoded/garbled pages, plus support for
Turkish month-name dates.
"""
import logging
import re
import os
from datetime import datetime
from typing import List, Dict, Any

import pdfplumber
import fitz  # PyMuPDF for rendering pages to images for OCR fallback
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

# 2) Credit cards ending in “TL”
CREDIT_TL_RE = re.compile(
    r'^\s*(\d{2}/\d{2}/\d{4})\s+'
    r'(.+?)\s+'
    r'(-?\d{1,3}(?:\.\d{3})*,\d{2})\s*TL\s*$'
)

# 3) Credit cards with multiple trailing numeric columns
CREDIT_MULTI_RE = re.compile(
    r'^\s*(\d{2}/\d{2}/\d{4})\s*'
    r'(.+?)\s+'
    r'(-?\d{1,3}(?:\.\d{3})*,\d{2})'
    r'(?:\s+[0-9]{1,3}(?:\.[0-9]{3})*,\d{2})+\s*$'
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
    """
    Extract a list of transactions from a PDF file.
    Returns list of dicts: {date, description, amount, currency, confidence}
    """
    if not os.path.exists(pdf_path) or not os.access(pdf_path, os.R_OK):
        logger.error(f"Cannot read PDF: {pdf_path}")
        return []

    results: List[Dict[str, Any]] = []
    try:
        # Open both pdfplumber and PyMuPDF for robust OCR fallback
        with pdfplumber.open(pdf_path) as pdf, fitz.open(pdf_path) as doc_fitz:
            logger.info(f"Opened PDF with {len(pdf.pages)} pages: {pdf_path}")

            for page_num, page in enumerate(pdf.pages, start=1):
                raw = page.extract_text() or ""
                # detect pages that need OCR:
                needs_ocr = (
                    not raw.strip() or
                    raw.count("(cid:") > 5 or
                    "�" in raw
                )
                if needs_ocr:
                    logger.info(f"Page {page_num}: falling back to OCR using PyMuPDF rendering")
                    try:
                        # Render page to image via PyMuPDF
                        pix = doc_fitz.load_page(page_num - 1).get_pixmap(dpi=300)
                        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                        raw = pytesseract.image_to_string(img, lang="tur+eng")
                    except Exception as e:
                        logger.warning(f"Page {page_num}: OCR fallback failed ({e}), using extracted text")

                # process lines
                for line in raw.splitlines():
                    txt = line.strip()
                    if not txt:
                        continue

                    # try bank statement pattern
                    m = ACCOUNT_LINE_RE.match(txt)
                    if m:
                        date_str, desc, amt_str = m.group(1), m.group(2), m.group(3)
                    else:
                        # try credit card TL or multi patterns
                        m = CREDIT_TL_RE.match(txt) or CREDIT_MULTI_RE.match(txt)
                        if m:
                            date_str, desc, amt_str = m.group(1), m.group(2), m.group(3)
                        else:
                            # try Turkish month-name dates
                            m2 = TEXT_DATE_RE.match(txt)
                            if not m2:
                                continue
                            day, mon, yr, desc, amt_str = (
                                m2.group(1), m2.group(2), m2.group(3),
                                m2.group(4), m2.group(5)
                            )
                            dt = datetime(int(yr), MONTHS[mon], int(day))
                            date_str = dt.strftime("%Y-%m-%d")

                    # normalize date format DD/MM/YYYY → YYYY-MM-DD
                    if "/" in date_str:
                        try:
                            dt = datetime.strptime(date_str, "%d/%m/%Y")
                            date_str = dt.strftime("%Y-%m-%d")
                        except ValueError:
                            pass

                    amt_str = amt_str.rstrip("+")
                    try:
                        amount = float(amt_str.replace(".", "").replace(",", "."))
                    except ValueError:
                        logger.debug(f"Skipping unparsable amount: {amt_str}")
                        continue

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
