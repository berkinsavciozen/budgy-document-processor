"""
PDF Transaction Extractor Module
Extracts transaction data from financial PDFs, including scanned credit-card statements.
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
# PATTERNS
# ----------------------------------------------------------------------------------------------------------------------

# 1) Tabular account statements (unchanged)
ACCOUNT_LINE_RE = re.compile(
    r'(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\s+(.+?)\s+(-?\d{1,3}(?:\.\d{3})*,\d{2})\s+(-?\d{1,3}(?:\.\d{3})*,\d{2})'
)

# 2) Date: numeric or Turkish month name
MONTH_MAP = {
    'ocak': '01','şubat': '02','mart': '03','nisan': '04','mayıs': '05','haziran': '06',
    'temmuz': '07','ağustos': '08','eylül': '09','ekim': '10','kasım': '11','aralık': '12'
}
DATE_PATTERN = re.compile(
    r'^\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}'
    r'|\d{1,2}\s+(?:' + '|'.join(MONTH_MAP.keys()) + r')\s+\d{4})',
    flags=re.IGNORECASE
)

# 3) Amount: e.g. “1.039,00 TL”, “-25.794,95”, with optional “+” suffix
AMOUNT_PATTERN = re.compile(r'(-?\d{1,3}(?:\.\d{3})*,\d{2})(?:\s*TL)?\+?')

# ----------------------------------------------------------------------------------------------------------------------
# MAIN ENTRY
# ----------------------------------------------------------------------------------------------------------------------

def extract_transactions(pdf_path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(pdf_path) or not os.access(pdf_path, os.R_OK):
        logger.error(f"Cannot read PDF: {pdf_path}")
        return []

    all_tx: List[Dict[str, Any]] = []

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for idx, page in enumerate(pdf.pages, start=1):
                logger.info(f"Processing page {idx}/{len(pdf.pages)}")

                text = page.extract_text() or ""
                page_tx = []

                # --- A) Tabular regex (account statements) ---
                for line in text.split('\n'):
                    m = ACCOUNT_LINE_RE.search(line)
                    if m:
                        date_raw, desc, amt_raw, _ = m.groups()
                        page_tx.append({
                            "date": _parse_date(date_raw),
                            "description": desc.strip(),
                            "amount": amt_raw.replace('.', '').replace(',', '.'),
                            "confidence": 0.9
                        })

                # --- B) Free-form regex on extracted text ---
                if not page_tx and text.strip():
                    for line in text.split('\n'):
                        dm = DATE_PATTERN.match(line)
                        am = AMOUNT_PATTERN.search(line)
                        if dm and am:
                            date_raw = dm.group(1)
                            amt_raw = am.group(1)
                            desc = line.replace(date_raw, '').replace(am.group(0), '').strip().rstrip('+')
                            page_tx.append({
                                "date": _parse_date(date_raw),
                                "description": desc,
                                "amount": amt_raw.replace('.', '').replace(',', '.'),
                                "confidence": 0.8
                            })

                # --- C) OCR fallback with multiline grouping ---
                if not page_tx:
                    logger.info("No text matches → performing full-page OCR")
                    ocr_text = _ocr_page_to_text(pdf_path, idx)
                    # group lines by leading date
                    page_tx = _group_ocr_records(ocr_text)
                    if not page_tx:
                        snippet = (ocr_text[:300] + '...') if len(ocr_text) > 300 else ocr_text
                        logger.warning(f"OCR produced no records. Sample OCR text:\n{snippet}")

                logger.info(f"→ Found {len(page_tx)} tx on page {idx}")
                all_tx.extend(page_tx)

    except Exception:
        logger.exception("Error extracting transactions")

    logger.info(f"Total extracted: {len(all_tx)}")
    return all_tx

# ----------------------------------------------------------------------------------------------------------------------
# HELPERS
# ----------------------------------------------------------------------------------------------------------------------

def _parse_date(raw: str) -> str:
    raw = raw.strip()
    # try numeric formats
    for fmt in ('%d/%m/%Y', '%m/%d/%Y', '%Y-%m-%d', '%d-%m-%Y'):
        try:
            return datetime.strptime(raw, fmt).strftime('%Y-%m-%d')
        except ValueError:
            pass
    # Turkish month names
    parts = raw.split()
    if len(parts) == 3:
        d, mon, y = parts
        m = MONTH_MAP.get(mon.lower(), '01')
        try:
            return datetime(int(y), int(m), int(d)).strftime('%Y-%m-%d')
        except ValueError:
            pass
    return raw  # fallback

def _ocr_page_to_text(pdf_path: str, page_number: int) -> str:
    """
    Render just one page at 300dpi via pdf2image → pil → pytesseract.
    """
    try:
        imgs = convert_from_path(
            pdf_path,
            dpi=300,
            first_page=page_number,
            last_page=page_number
        )
        if imgs:
            return pytesseract.image_to_string(imgs[0], lang='eng+tur')
    except Exception:
        logger.exception("OCR on page failed")
    return ""

def _group_ocr_records(ocr_text: str) -> List[Dict[str, Any]]:
    """
    Given raw OCR text, collect lines starting with a DATE_PATTERN
    and attach any subsequent lines (no leading date) as continuation
    of the last description.
    """
    recs: List[Dict[str, Any]] = []
    current = None

    for raw_line in ocr_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        dm = DATE_PATTERN.match(line)
        am = AMOUNT_PATTERN.search(line)

        if dm and am:
            # start new record
            if current:
                recs.append(current)
            date_raw = dm.group(1)
            amt_raw = am.group(1)
            desc = line[dm.end():am.start()].strip().rstrip('+')
            current = {
                "date":   _parse_date(date_raw),
                "description": desc,
                "amount": amt_raw.replace('.', '').replace(',', '.'),
                "confidence": 0.7
            }
        elif current:
            # continuation line
            current["description"] += " " + line

    if current:
        recs.append(current)
    return recs
