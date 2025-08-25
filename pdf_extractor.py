"""
PDF Transaction Extractor
- Extracts tabular rows via pdfplumber
- Falls back to text/regex + OCR (PyMuPDF + Tesseract) when needed
- Handles TR/EU/US number formats and Turkish month names
Returns (transactions, meta)
"""
import os
import re
from datetime import datetime
from typing import List, Dict, Any, Tuple

import pdfplumber
import fitz  # PyMuPDF
from PIL import Image
import pytesseract

DEFAULT_CURRENCY = os.getenv("DEFAULT_CURRENCY", "TRY")
TESSERACT_LANG = os.getenv("TESSERACT_LANG", "eng+tur")

TR_MONTHS = {
    "oca": 1, "şub": 2, "sub": 2, "mar": 3, "nis": 4, "may": 5, "haz": 6,
    "tem": 7, "ağu": 8, "agu": 8, "eyl": 9, "eki": 10, "kas": 11, "ara": 12,
}

DATE_PATTERNS = [
    # 31/12/2024 or 31.12.2024
    re.compile(r"\b(\d{2})[./](\d{2})[./](\d{4})\b"),
    # 2024-12-31
    re.compile(r"\b(\d{4})-(\d{2})-(\d{2})\b"),
    # 31 Oca 2024
    re.compile(r"\b(\d{1,2})\s+([A-Za-zŞşÜüĞğİıÖöÇç]+)\s+(\d{4})\b"),
]

AMOUNT_PATTERNS = [
    # -1.234,56  |  1.234,56  |  -1234,56  (EU/TR)
    re.compile(r"^-?\d{1,3}(?:\.\d{3})*,\d{2}$"),
    # -1,234.56  |  1,234.56  |  -1234.56  (US)
    re.compile(r"^-?\d{1,3}(?:,\d{3})*\.\d{2}$"),
    # Plain int or decimal with sign
    re.compile(r"^-?\d+(?:[.,]\d+)?$"),
]

def _normalize_amount(s: str) -> str:
    """Normalize amount string to dot decimal (e.g., '1234.56'). Return original if cannot."""
    raw = s.strip()
    if not raw:
        return raw
    # Remove currency suffix/prefix if present (TL, TRY, $, €)
    raw = re.sub(r"(TL|TRY|USD|EUR|GBP|₺|€|\$)\b", "", raw, flags=re.IGNORECASE).strip()
    # Try EU/TR format 1.234,56
    if re.search(r",\d{2}$", raw) and "." in raw:
        try:
            val = float(raw.replace(".", "").replace(",", "."))
            return f"{val:.2f}"
        except Exception:
            pass
    # Try US format 1,234.56
    if re.search(r"\.\d{2}$", raw) and "," in raw:
        try:
            val = float(raw.replace(",", ""))
            return f"{val:.2f}"
        except Exception:
            pass
    # Fallback remove thousands like "1.234" w/o decimals
    if raw.count(".") > 1 and "," not in raw:
        try:
            val = float(raw.replace(".", ""))
            return f"{val:.2f}"
        except Exception:
            pass
    # Replace comma decimal if looks like decimal
    if raw.count(",") == 1 and raw.count(".") == 0:
        maybe = raw.replace(",", ".")
        try:
            val = float(maybe)
            return f"{val:.2f}"
        except Exception:
            pass
    # As last resort, try plain float
    try:
        val = float(raw)
        return f"{val:.2f}"
    except Exception:
        return s.strip()

def _parse_date(s: str) -> str:
    s = s.strip()
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
    # Unparseable → return original
    return s

def _infer_type(amount_str: str) -> str:
    try:
        val = float(_normalize_amount(amount_str))
        return "expense" if val < 0 else "income"
    except Exception:
        return "expense"

def _from_table(table: List[List[Any]]) -> List[Dict[str, Any]]:
    if not table or len(table) < 2:
        return []
    header = [(h or "").strip().lower() for h in table[0]]
    results: List[Dict[str, Any]] = []
    for row in table[1:]:
        cells = [(c or "").strip() for c in row]
        rec = {header[i] if i < len(header) else f"col_{i}": (cells[i] if i < len(cells) else "") for i in range(len(header))}
        # try map
        date = rec.get("date") or rec.get("tarih") or rec.get("islem tarihi") or rec.get("i̇şlem tarihi")
        desc = rec.get("description") or rec.get("açıklama") or rec.get("aciklama") or rec.get("işlem") or rec.get("islem")
        amt  = rec.get("amount") or rec.get("tutar") or rec.get("işlem tutarı") or rec.get("islem tutari")
        cur  = rec.get("currency") or rec.get("para birimi") or rec.get("pb") or None

        if not (date and desc and amt):
            # fallback heuristic: find amount-looking cell
            amt_candidates = [c for c in cells if any(rx.match(c.replace(" ", "")) for rx in AMOUNT_PATTERNS)]
            if not amt and amt_candidates:
                amt = amt_candidates[-1]
            if not desc:
                # take the longest non-amount cell as description
                non_amount = [c for c in cells if c and c != amt]
                desc = max(non_amount, key=len) if non_amount else ""

        if not (date and desc and amt):
            continue

        results.append({
            "date": _parse_date(date),
            "description": desc,
            "amount": _normalize_amount(amt),
            "currency": cur or DEFAULT_CURRENCY,
            "category": None,
            "type": _infer_type(amt),
        })
    return results

def _ocr_page(page) -> str:
    pix = page.get_pixmap(dpi=240, alpha=False)
    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
    text = pytesseract.image_to_string(img, lang=TESSERACT_LANG)
    return text

LINE_RX = re.compile(
    r"(?P<date>\d{2}[./]\d{2}[./]\d{4}|\d{4}-\d{2}-\d{2}|\d{1,2}\s+[A-Za-zŞşÜüĞğİıÖöÇç]+\s+\d{4})"
    r"\s+"
    r"(?P<desc>.+?)"
    r"\s+"
    r"(?P<amt>-?\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})|-?\d+(?:[.,]\d{2})?)"
    r"(?:\s*(?P<cur>TL|TRY|USD|EUR|GBP|₺|€|\$))?$",
    flags=re.IGNORECASE
)

def _from_free_text(text: str) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    for line in (text or "").splitlines():
        line = line.strip()
        if not line:
            continue
        m = LINE_RX.search(line)
        if not m:
            continue
        date = _parse_date(m.group("date"))
        desc = m.group("desc").strip()
        amt  = m.group("amt").strip()
        cur  = m.group("cur") or DEFAULT_CURRENCY
        results.append({
            "date": date,
            "description": desc,
            "amount": _normalize_amount(amt),
            "currency": cur,
            "category": None,
            "type": _infer_type(amt),
        })
    return results

def extract_transactions(pdf_bytes: bytes) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Returns (transactions, meta)
    meta: {"extraction_method": "...", "extraction_quality": "...", "warnings": []}
    """
    warnings: List[str] = []
    try:
        with pdfplumber.open(io=io.BytesIO(pdf_bytes)) as pdf:
            rows: List[Dict[str, Any]] = []
            for page in pdf.pages:
                tables = page.extract_tables() or []
                for tbl in tables:
                    rows.extend(_from_table(tbl))

            if rows:
                return rows, {"extraction_method": "tables", "extraction_quality": "high", "warnings": warnings}

            # Fallback: use page text
            text_rows: List[Dict[str, Any]] = []
            for page in pdf.pages:
                text_rows.extend(_from_free_text(page.extract_text() or ""))

            if text_rows:
                return text_rows, {"extraction_method": "text", "extraction_quality": "medium", "warnings": warnings}

    except Exception as e:
        warnings.append(f"pdfplumber failed: {str(e)}")

    # OCR fallback
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        ocr_rows: List[Dict[str, Any]] = []
        for page in doc:
            text = _ocr_page(page)
            ocr_rows.extend(_from_free_text(text))
        if ocr_rows:
            return ocr_rows, {"extraction_method": "ocr", "extraction_quality": "medium", "warnings": warnings}
    except Exception as e:
        warnings.append(f"OCR failed: {str(e)}")

    return [], {"extraction_method": "none", "extraction_quality": "low", "warnings": warnings}
