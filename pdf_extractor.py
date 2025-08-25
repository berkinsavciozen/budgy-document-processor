# pdf_extractor.py
"""
Robust PDF transaction extraction with:
- Table extraction (pdfplumber)
- Text regex fallback
- OCR fallback (PyMuPDF + Tesseract), tolerant if tesseract binary missing on host
- TR/EU/US amounts; Turkish month names
- Self-learning categories (user_rules), main/sub + legacy 'category'
"""
import os
import re
import io
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional

import pdfplumber
import fitz  # PyMuPDF
from PIL import Image
try:
    import pytesseract  # okay if installed; runtime OCR is try/except
except Exception:  # pragma: no cover
    pytesseract = None  # degrade gracefully

from categorizer import categorize

DEFAULT_CURRENCY = os.getenv("DEFAULT_CURRENCY", "TRY")
TESSERACT_LANG = os.getenv("TESSERACT_LANG", "eng+tur")

TR_MONTHS = {
    "oca": 1, "şub": 2, "sub": 2, "mar": 3, "nis": 4, "may": 5, "haz": 6,
    "tem": 7, "ağu": 8, "agu": 8, "eyl": 9, "eki": 10, "kas": 11, "ara": 12,
}

DATE_PATTERNS = [
    re.compile(r"\b(\d{2})[./](\d{2})[./](\d{4})\b"),            # 31/12/2024 or 31.12.2024
    re.compile(r"\b(\d{4})-(\d{2})-(\d{2})\b"),                  # 2024-12-31
    re.compile(r"\b(\d{1,2})\s+([A-Za-zŞşÜüĞğİıÖöÇç]+)\s+(\d{4})\b"),  # 31 Oca 2024
]

AMOUNT_PATTERNS = [
    re.compile(r"^-?\d{1,3}(?:\.\d{3})*,\d{2}$"),  # EU/TR: 1.234,56
    re.compile(r"^-?\d{1,3}(?:,\d{3})*\.\d{2}$"),  # US:    1,234.56
    re.compile(r"^-?\d+(?:[.,]\d+)?$"),            # plain
]

LINE_RX = re.compile(
    r"(?P<date>\d{2}[./]\d{2}[./]\d{4}|\d{4}-\d{2}-\d{2}|\d{1,2}\s+[A-Za-zŞşÜüĞğİıÖöÇç]+\s+\d{4})"
    r"\s+"
    r"(?P<desc>.+?)"
    r"\s+"
    r"(?P<amt>-?\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})|-?\d+(?:[.,]\d{2})?)"
    r"(?:\s*(?P<cur>TL|TRY|USD|EUR|GBP|₺|€|\$))?$",
    flags=re.IGNORECASE
)


def _normalize_amount(s: str) -> str:
    raw = (s or "").strip()
    if not raw:
        return raw
    raw = re.sub(r"(TL|TRY|USD|EUR|GBP|₺|€|\$)\b", "", raw, flags=re.IGNORECASE).strip()
    if re.search(r",\d{2}$", raw) and "." in raw:
        try:
            return f"{float(raw.replace('.', '').replace(',', '.')):.2f}"
        except Exception:
            pass
    if re.search(r"\.\d{2}$", raw) and "," in raw:
        try:
            return f"{float(raw.replace(',', '')):.2f}"
        except Exception:
            pass
    if raw.count(".") > 1 and "," not in raw:
        try:
            return f"{float(raw.replace('.', '')):.2f}"
        except Exception:
            pass
    if raw.count(",") == 1 and raw.count(".") == 0:
        try:
            return f"{float(raw.replace(',', '.')):.2f}"
        except Exception:
            pass
    try:
        return f"{float(raw):.2f}"
    except Exception:
        return (s or "").strip()


def _parse_date(s: str) -> str:
    s = (s or "").strip()
    for rx in DATE_PATTERNS:
        m = rx.search(s)
        if not m:
            continue
        if rx is DATE_PATTERNS[0]:
            d, mth, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
            return datetime(y, mth, d).strftime("%Y-%m-%d")
        if rx is DATE_PATTERNS[1]:
            y, mth, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
            return datetime(y, mth, d).strftime("%Y-%m-%d")
        if rx is DATE_PATTERNS[2]:
            d = int(m.group(1))
            mon_name = m.group(2).lower()[:3]
            y = int(m.group(3))
            mth = TR_MONTHS.get(mon_name, None)
            if mth:
                return datetime(y, mth, d).strftime("%Y-%m-%d")
    return s


def _infer_type(amount_str: Optional[str]) -> str:
    try:
        if amount_str is None:
            return "expense"
        val = float(_normalize_amount(str(amount_str)))
        return "expense" if val < 0 else "income"
    except Exception:
        return "expense"


def _build_row(date: str, desc: str, amt: str, cur: Optional[str], user_rules: Optional[List[Dict[str, Any]]]) -> Dict[str, Any]:
    tx_type = _infer_type(amt)
    c_main, c_sub = categorize(desc, tx_type, user_rules=user_rules)
    return {
        "date": _parse_date(date),
        "description": desc,
        "amount": _normalize_amount(amt),
        "currency": cur or DEFAULT_CURRENCY,
        "type": tx_type,
        "category_main": c_main,
        "category_sub": c_sub,
        "category": c_main,  # legacy alias
    }


def _from_table(table: List[List[Any]], user_rules: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    if not table or len(table) < 2:
        return []
    header = [(h or "").strip().lower() for h in table[0]]
    out: List[Dict[str, Any]] = []
    for row in table[1:]:
        cells = [(c or "").strip() for c in row]
        def _g(*names):
            for n in names:
                if n in header:
                    i = header.index(n)
                    if i < len(cells):
                        return cells[i]
            return ""
        date = _g("date", "tarih", "islem tarihi", "i̇şlem tarihi")
        desc = _g("description", "açıklama", "aciklama", "işlem", "islem")
        amt  = _g("amount", "tutar", "işlem tutarı", "islem tutari")
        cur  = _g("currency", "para birimi", "pb")

        if not amt:
            # heuristic: last amount-like cell in row
            amt_candidates = [c for c in cells if any(rx.match(c.replace(" ", "")) for rx in AMOUNT_PATTERNS)]
            if amt_candidates: amt = amt_candidates[-1]
        if not desc:
            non_amount = [c for c in cells if c and c != amt]
            desc = max(non_amount, key=len) if non_amount else ""

        if date and desc and amt:
            out.append(_build_row(date, desc, amt, cur, user_rules))
    return out


def _from_free_text(text: str, user_rules: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for line in (text or "").splitlines():
        line = line.strip()
        if not line:
            continue
        m = LINE_RX.search(line)
        if not m:
            continue
        date = m.group("date")
        desc = (m.group("desc") or "").strip()
        amt  = (m.group("amt") or "").strip()
        cur  = m.group("cur")
        if date and desc and amt:
            out.append(_build_row(date, desc, amt, cur, user_rules))
    return out


def _ocr_page(page) -> str:
    # Skip if pytesseract not available
    if pytesseract is None:
        return ""
    pix = page.get_pixmap(dpi=240, alpha=False)
    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
    return pytesseract.image_to_string(img, lang=TESSERACT_LANG)


def extract_transactions(pdf_bytes: bytes, user_rules: Optional[List[Dict[str, Any]]] = None) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    warnings: List[str] = []
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            rows: List[Dict[str, Any]] = []
            for page in pdf.pages:
                tables = page.extract_tables() or []
                for tbl in tables:
                    rows.extend(_from_table(tbl, user_rules))
            if rows:
                return rows, {"extraction_method": "tables", "extraction_quality": "high", "warnings": warnings}

            text_rows: List[Dict[str, Any]] = []
            for page in pdf.pages:
                text_rows.extend(_from_free_text(page.extract_text() or "", user_rules))
            if text_rows:
                return text_rows, {"extraction_method": "text", "extraction_quality": "medium", "warnings": warnings}
    except Exception as e:
        warnings.append(f"pdfplumber failed: {str(e)}")

    # OCR fallback (safe if pytesseract missing)
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        ocr_rows: List[Dict[str, Any]] = []
        for page in doc:
            text = _ocr_page(page)
            if text:
                ocr_rows.extend(_from_free_text(text, user_rules))
        if ocr_rows:
            return ocr_rows, {"extraction_method": "ocr", "extraction_quality": "medium", "warnings": warnings}
    except Exception as e:
        warnings.append(f"OCR failed: {str(e)}")

    return [], {"extraction_method": "none", "extraction_quality": "low", "warnings": warnings}
