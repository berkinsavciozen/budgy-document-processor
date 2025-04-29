"""
PDF Transaction Extractor Module
Extracts transaction data from financial PDFs (bank & credit-card statements)
with an OCR fallback for fully image/CID-encoded pages, plus support for
Turkish month-name dates.
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

# 1) Bank-statements: row#, DD/MM/YYYY, desc, amount, balance
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

# 3) Credit cards with multiple trailing numbers (installment, maxipuan…)
CREDIT_MULTI_RE = re.compile(
    r'^\s*(\d{2}/\d{2}/\d{4})\s*'
    r'(.+?)\s+'
    r'(-?\d{1,3}(?:\.\d{3})*,\d{2})'
    r'(?:\s+[0-9]{1,3}(?:\.\d{3})*,\d{2})+\s*$'
)

# 4) Turkish month-name dates, e.g. "10 Kasım 2024 DESC… 180,00+"
MONTHS = {
    "Ocak":1, "Şubat":2, "Mart":3, "Nisan":4, "Mayıs":5, "Haziran":6,
    "Temmuz":7, "Ağustos":8, "Eylül":9, "Ekim":10, "Kasım":11, "Aralık":12
}
# build alternation pattern
_month_names = "|".join(MONTHS.keys())
TEXT_DATE_RE = re.compile(
    rf'^\s*(\d{{1,2}})\s+({_month_names})\s+(\d{{4}})\s+(.+?)\s+'
    r'(-?\d{1,3}(?:\.\d{3})*,\d{2})\+?\s*$'
)

def extract_transactions(pdf_path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(pdf_path) or not os.access(pdf_path, os.R_OK):
        logger.error(f"Cannot read PDF: {pdf_path}")
        return []

    out: List[Dict[str, Any]] = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            logger.info(f"Opened PDF ({len(pdf.pages)} pages): {pdf_path}")
            for pg_num, page in enumerate(pdf.pages, start=1):
                raw = page.extract_text() or ""
                # fallback to OCR if blank or heavy CID garbage
                if not raw.strip() or raw.count("(cid:") > 5:
                    logger.info(f"Page {pg_num}: using OCR fallback")
                    img = page.to_image(resolution=300).original
                    raw = pytesseract.image_to_string(img, lang="tur+eng")

                for line in raw.splitlines():
                    txt = line.strip()
                    if not txt:
                        continue

                    # try each pattern in order
                    m = ACCOUNT_LINE_RE.match(txt)
                    if m:
                        date_str, desc, amt_str = m.group(1), m.group(2), m.group(3)
                    else:
                        m = CREDIT_TL_RE.match(txt) or CREDIT_MULTI_RE.match(txt)
                        if m:
                            date_str, desc, amt_str = m.group(1), m.group(2), m.group(3)
                        else:
                            m2 = TEXT_DATE_RE.match(txt)
                            if not m2:
                                continue
                            # TEXT month‐name match
                            day, mon_name, year, desc, amt_str = (
                                m2.group(1), m2.group(2), m2.group(3),
                                m2.group(4), m2.group(5)
                            )
                            # build ISO date
                            dt = datetime(
                                year=int(year),
                                month=MONTHS[mon_name],
                                day=int(day)
                            )
                            date_str = dt.strftime("%Y-%m-%d")

                    # parse numeric date if still in DD/MM/YYYY
                    if "/" in date_str:
                        try:
                            dt = datetime.strptime(date_str, "%d/%m/%Y")
                            date_str = dt.strftime("%Y-%m-%d")
                        except ValueError:
                            pass

                    # strip trailing '+' that TEXT_DATE_RE may have captured
                    amt_str = amt_str.rstrip("+")

                    # normalize amount: "1.234,56" → 1234.56
                    amount = float(amt_str.replace(".", "").replace(",", "."))

                    out.append({
                        "date":        date_str,
                        "description": desc.strip(),
                        "amount":      amount,
                        "currency":    DEFAULT_CURRENCY,
                        "confidence":  0.8
                    })

        logger.info(f"Extracted {len(out)} txns from {pdf_path}")
        return out

    except Exception as e:
        logger.exception(f"Error extracting txns from {pdf_path}: {e}")
        return []
