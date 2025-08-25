# pdf_extractor.py  (only the key diffs; replace your file if easier)
import os, re, io
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional
import pdfplumber, fitz
from PIL import Image
import pytesseract

from categorizer import categorize

DEFAULT_CURRENCY = os.getenv("DEFAULT_CURRENCY", "TRY")
TESSERACT_LANG = os.getenv("TESSERACT_LANG", "eng+tur")

# ... (date & amount utils unchanged) ...

def _infer_type(amount_str: Optional[str]) -> str:
    try:
        if amount_str is None: return "expense"
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

def _from_table(table, user_rules):
    # ... header detection as before ...
    out = []
    # produce rows:
    # out.append(_build_row(date, desc, amt, cur, user_rules))
    return out

def _from_free_text(text: str, user_rules):
    # ... regex parse as before ...
    out = []
    # out.append(_build_row(date, desc, amt, cur, user_rules))
    return out

def _ocr_page(page) -> str:
    pix = page.get_pixmap(dpi=240, alpha=False)
    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
    return pytesseract.image_to_string(img, lang=TESSERACT_LANG)

def extract_transactions(pdf_bytes: bytes, user_rules: Optional[List[Dict[str, Any]]] = None) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    warnings: List[str] = []
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            rows: List[Dict[str, Any]] = []
            for page in pdf.pages:
                for tbl in (page.extract_tables() or []):
                    rows.extend(_from_table(tbl, user_rules))
            if rows:
                return rows, {"extraction_method": "tables", "extraction_quality": "high", "warnings": warnings}
            # fallback text
            text_rows: List[Dict[str, Any]] = []
            for page in pdf.pages:
                text_rows.extend(_from_free_text(page.extract_text() or "", user_rules))
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
            ocr_rows.extend(_from_free_text(text, user_rules))
        if ocr_rows:
            return ocr_rows, {"extraction_method": "ocr", "extraction_quality": "medium", "warnings": warnings}
    except Exception as e:
        warnings.append(f"OCR failed: {str(e)}")

    return [], {"extraction_method": "none", "extraction_quality": "low", "warnings": warnings}
