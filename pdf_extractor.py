"""
PDF Transaction Extractor Module
Extracts transaction data from financial PDFs
– including tabular account statements and scanned credit‐card statements.
"""
import logging
import re
import os
from datetime import datetime
from typing import List, Dict, Any

import pdfplumber
import pytesseract
from pdf2image import convert_from_path

logger = logging.getLogger("budgy-document-processor.pdf_extractor")

# ----------------------------------------------------------------------------------------------------------------------
# REGEX PATTERNS
# ----------------------------------------------------------------------------------------------------------------------

# 1) Tabular account statements (e.g. YPK_Account_July)
ACCOUNT_LINE_RE = re.compile(
    r'(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\s+(.+?)\s+(-?\d{1,3}(?:\.\d{3})*,\d{2})\s+(-?\d{1,3}(?:\.\d{3})*,\d{2})'
)

# 2) Free-form lines: date either numeric or Turkish month name
MONTH_MAP = {
    'ocak': '01', 'şubat': '02', 'mart': '03', 'nisan': '04',
    'mayıs': '05', 'haziran': '06', 'temmuz': '07', 'ağustos': '08',
    'eylül': '09', 'ekim': '10', 'kasım': '11', 'aralık': '12'
}
DATE_PATTERN = re.compile(
    r'(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}'                                     # numeric
    r'|\d{1,2}\s+(?:' + '|'.join(MONTH_MAP.keys()) + r')\s+\d{4})',        # Turkish month
    flags=re.IGNORECASE
)

# 3) Amounts – e.g. “1.039,00 TL”, “246,00+”, “-25.794,95”
AMOUNT_PATTERN = re.compile(r'(-?\d{1,3}(?:\.\d{3})*,\d{2})(?:\s*TL)?\+?')

# ----------------------------------------------------------------------------------------------------------------------
# EXTRACTION
# ----------------------------------------------------------------------------------------------------------------------

def extract_transactions(pdf_path: str) -> List[Dict[str, Any]]:
    logger.info(f"Extracting transactions from PDF: {pdf_path}")
    if not os.path.exists(pdf_path) or not os.access(pdf_path, os.R_OK):
        logger.error("PDF not found or not readable.")
        return []

    results: List[Dict[str, Any]] = []

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page_index, page in enumerate(pdf.pages, start=1):
                logger.info(f"Page {page_index}/{len(pdf.pages)}")

                page_text = page.extract_text() or ""
                page_found = []

                # --- A) Tabular extraction (unchanged) ---  
                for line in page_text.split('\n'):
                    m = ACCOUNT_LINE_RE.search(line)
                    if not m:
                        continue
                    raw_date, desc, raw_amt, _ = m.groups()
                    dt = _parse_date(raw_date)
                    amt = raw_amt.replace('.', '').replace(',', '.')
                    page_found.append({
                        "date": dt,
                        "description": desc.strip(),
                        "amount": amt,
                        "confidence": 0.9
                    })

                # --- B) Free-form text extraction from the PDF text itself ---  
                if not page_found and page_text.strip():
                    for line in page_text.split('\n'):
                        dm = DATE_PATTERN.search(line)
                        am = AMOUNT_PATTERN.search(line)
                        if not (dm and am):
                            continue
                        date_str = dm.group(1).strip()
                        amt_str = am.group(1)
                        dt = _parse_date(date_str)
                        desc = line.replace(date_str, '').replace(am.group(0), '').strip().rstrip('+')
                        page_found.append({
                            "date": dt,
                            "description": desc,
                            "amount": amt_str.replace('.', '').replace(',', '.'),
                            "confidence": 0.8
                        })

                # --- C) FULL-PAGE OCR fallback via pdf2image if still nothing found ---  
                if not page_found:
                    logger.info("No text hits → performing full‐page OCR")
                    # Convert *just this page* to image at 300dpi
                    pil_imgs = convert_from_path(
                        pdf_path,
                        dpi=300,
                        first_page=page_index,
                        last_page=page_index
                    )
                    if pil_imgs:
                        ocr = pytesseract.image_to_string(pil_imgs[0], lang='eng+tur')
                        for line in ocr.split('\n'):
                            dm = DATE_PATTERN.search(line)
                            am = AMOUNT_PATTERN.search(line)
                            if not (dm and am):
                                continue
                            date_str = dm.group(1).strip()
                            amt_str = am.group(1)
                            dt = _parse_date(date_str)
                            desc = line.replace(date_str, '').replace(am.group(0), '').strip().rstrip('+')
                            page_found.append({
                                "date": dt,
                                "description": desc,
                                "amount": amt_str.replace('.', '').replace(',', '.'),
                                "confidence": 0.7
                            })

                logger.info(f"→ Found {len(page_found)} transactions on page {page_index}")
                results.extend(page_found)

    except Exception as e:
        logger.exception("Error during extraction")

    logger.info(f"Total transactions: {len(results)}")
    return results


def _parse_date(raw: str) -> str:
    """Normalize either numeric or Turkish‐month dates into YYYY-MM-DD"""
    raw = raw.strip()
    # numeric
    for fmt in ('%d/%m/%Y', '%m/%d/%Y', '%Y-%m-%d', '%d-%m-%Y'):
        try:
            return datetime.strptime(raw, fmt).strftime('%Y-%m-%d')
        except ValueError:
            pass
    # Turkish month
    parts = raw.split()
    if len(parts) == 3:
        d, mon, y = parts
        m = MONTH_MAP.get(mon.lower(), '01')
        try:
            return datetime(int(y), int(m), int(d)).strftime('%Y-%m-%d')
        except:
            pass
    # fallback
    return raw
