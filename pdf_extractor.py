import logging
import re
import os
from datetime import datetime
from typing import List, Dict, Any
import pdfplumber

# new imports
from PIL import Image
import pytesseract

logger = logging.getLogger("budgy-document-processor.pdf_extractor")

DEFAULT_CURRENCY = os.getenv("DEFAULT_CURRENCY", "TRY")

ACCOUNT_LINE_RE = re.compile(…)
CREDIT_LINE_RE = re.compile(…)

def extract_transactions(pdf_path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(pdf_path) or not os.access(pdf_path, os.R_OK):
        logger.error(f"Cannot read PDF: {pdf_path}")
        return []

    results: List[Dict[str, Any]] = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                # 1) try native text extraction
                raw_text = page.extract_text() or ""
                # 2) detect garbage: many “(cid:”
                if not raw_text.strip() or raw_text.count("(cid:") > 5:
                    logger.info(f"Page {page_num}: falling back to OCR")
                    # render page at high resolution
                    pil_img = page.to_image(resolution=300).original
                    # OCR: Turkish + Latin fallback
                    raw_text = pytesseract.image_to_string(pil_img, lang="tur+eng")

                for line in raw_text.splitlines():
                    line = line.strip()
                    m = ACCOUNT_LINE_RE.match(line) or CREDIT_LINE_RE.match(line)
                    if not m:
                        continue

                    date_str, desc, amt_str = m.group(1), m.group(2), m.group(3)
                    # … (your normalization logic here) …
                    dt = datetime.strptime(date_str, "%d/%m/%Y")
                    date_out = dt.strftime("%Y-%m-%d")
                    amount = float(amt_str.replace(".", "").replace(",", "."))

                    results.append({
                        "date":        date_out,
                        "description": desc.strip(),
                        "amount":      amount,
                        "currency":    DEFAULT_CURRENCY,
                        "confidence":  0.8
                    })

        return results

    except Exception as e:
        logger.exception(f"Error extracting transactions: {e}")
        return []
