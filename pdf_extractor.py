"""
PDF Transaction Extractor Module

Goals:
- Extract transaction rows from bank / credit card statements.
- Parse: date, description, amount.
- Normalize amounts with correct sign (expenses negative, income positive).
- Auto-categorize each transaction using heuristic category names that
  match typical personal finance buckets used in Budgi.
- Attach currency when possible.

This module is intentionally self-contained so it can be used both by the
external web service and (optionally) locally.
"""

import logging
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

import pdfplumber
import fitz  # PyMuPDF
from PIL import Image
import pytesseract

logger = logging.getLogger("budgy-pdf-extractor")
logger.setLevel(logging.INFO)

# --- Configuration ---------------------------------------------------------

DEFAULT_CURRENCY = os.getenv("DEFAULT_CURRENCY", "TRY")

# Common date regex: 01.01.2025, 01/01/25, 01-01-2025, 1 Jan 2025, 1 Ocak 2025
DATE_PATTERN = re.compile(
    r"\b("
    r"\d{1,2}[./-]\d{1,2}[./-]\d{2,4}"                      # 01.01.2024, 01/01/24, 01-01-2024
    r"|"
    r"\d{1,2}\s+[A-Za-zÇĞİÖŞÜçğıöşü]{3,}\s+\d{2,4}"         # 1 January 2024 / 1 Ocak 2024
    r")\b",
    re.UNICODE,
)

# Amount pattern: 1.234,56 or 1,234.56 or 1234.56 or -123,45
AMOUNT_PATTERN = re.compile(
    r"[-+]?\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})|\b[-+]?\d+(?:[.,]\d{2})\b"
)

# Expense / income keywords (English + Turkish)
EXPENSE_KEYWORDS = [
    "market", "supermarket", "grocery", "bakkal", "food", "gıda",
    "restaurant", "restoran", "cafe", "kafe", "coffee", "kahve",
    "fuel", "gas", "benzin", "akaryakıt", "otopark", "otopark",
    "uber", "taksi", "taxi", "metro", "otobüs", "bus", "tram",
    "online shopping", "alışveriş", "shop", "mağaza",
    "fatura", "bill", "electric", "su", "doğalgaz", "internet",
    "kira", "rent", "mortgage", "otel", "hotel", "tatil", "travel",
    "pos", "atm", "ödeme", "payment", "withdrawal", "çekim",
]

INCOME_KEYWORDS = [
    "salary", "maaş", "wage", "payroll", "deposit", "yatırma",
    "havale", "eft", "virman", "incoming transfer", "gelen havale",
    "refund", "iade", "interest", "faiz", "bonus", "prize",
]

TRANSFER_KEYWORDS = [
    "transfer", "virman", "havale", "eft",
]

# Category keywords – aligned with typical Budgi style buckets
CATEGORY_KEYWORDS: Dict[str, List[str]] = {
    "Groceries": [
        "grocery", "supermarket", "migros", "carrefour", "bim", "şok",
        "market", "gıda", "a101",
    ],
    "Dining": [
        "restaurant", "restoran", "cafe", "kafe", "coffee", "kahve",
        "starbucks", "burger", "pizza", "döner",
    ],
    "Transportation": [
        "uber", "taksi", "taxi", "metro", "otobüs", "bus", "tram",
        "fuel", "benzin", "akaryakıt", "otopark", "parking",
    ],
    "Housing": [
        "rent", "kira", "mortgage", "ev kredisi", "apartment", "site aidatı",
    ],
    "Utilities": [
        "fatura", "bill", "electric", "elektrik", "water", "su",
        "doğalgaz", "natural gas", "internet", "telefon", "gsm",
    ],
    "Entertainment": [
        "netflix", "spotify", "disney", "apple music", "sinema",
        "tiyatro", "concert", "konser", "bilet",
    ],
    "Shopping": [
        "hepsiburada", "trendyol", "amazon", "n11", "boyner",
        "shopping", "alışveriş", "mağaza",
    ],
    "Travel": [
        "otel", "hotel", "uçuş", "flight", "airline", "airbnb",
        "booking", "tatil", "seyahat",
    ],
    "Health": [
        "eczane", "pharmacy", "hospital", "hastane", "doktor",
        "medical", "clinic", "dentist", "diş",
    ],
    "Income": [
        "salary", "maaş", "payroll", "deposit", "faiz", "interest",
        "bonus", "incoming", "yatan",
    ],
    "Transfer": [
        "transfer", "virman", "havale", "eft",
    ],
}


# --- Helpers ---------------------------------------------------------------

def parse_date(date_str: str) -> Optional[str]:
    """Parse various date formats into ISO yyyy-mm-dd."""
    if not date_str:
        return None

    text = date_str.strip().replace("\u00a0", " ")

    # First: numeric formats dd.mm.yyyy, dd/mm/yy etc.
    numeric_formats = [
        "%d.%m.%Y", "%d.%m.%y",
        "%d/%m/%Y", "%d/%m/%y",
        "%d-%m-%Y", "%d-%m-%y",
    ]
    for fmt in numeric_formats:
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass

    # Month-name formats (English & Turkish)
    # e.g. 1 January 2024, 1 Jan 2024, 1 Ocak 2024
    # First try with datetime directly (English locales)
    for fmt in ("%d %B %Y", "%d %b %Y"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass

    # Manual Turkish month handling
    months_tr = {
        "ocak": 1, "şubat": 2, "mart": 3, "nisan": 4, "mayıs": 5, "haziran": 6,
        "temmuz": 7, "ağustos": 8, "eylül": 9, "ekim": 10, "kasım": 11, "aralık": 12,
    }
    m = re.match(r"(\d{1,2})\s+([A-Za-zÇĞİÖŞÜçğıöşü]+)\s+(\d{2,4})", text, re.UNICODE)
    if m:
        day = int(m.group(1))
        month_name = m.group(2).lower()
        year = int(m.group(3))
        if year < 100:  # 24 -> 2024 assumption
            year += 2000
        month = months_tr.get(month_name)
        if month:
            try:
                return datetime(year, month, day).strftime("%Y-%m-%d")
            except ValueError:
                return None

    return None


def clean_amount_to_float(amount_str: str) -> Optional[float]:
    """Convert a localized amount string to a float, preserving sign."""
    if not amount_str:
        return None

    s = amount_str.strip()
    # Detect negative via '-' or parentheses
    negative = False
    if "(" in s and ")" in s:
        negative = True
    if s.startswith("-") or s.startswith("−"):
        negative = True

    # Remove currency symbols & spaces
    s = re.sub(r"[^\d,.\-]", "", s)

    if not s:
        return None

    # Determine decimal separator (last occurrence of . or ,)
    last_dot = s.rfind(".")
    last_comma = s.rfind(",")
    if last_dot == -1 and last_comma == -1:
        # No separators, pure integer
        try:
            value = float(s.replace("-", ""))
        except ValueError:
            return None
    else:
        if last_dot > last_comma:
            dec_sep = "."
            thou_sep = ","
        else:
            dec_sep = ","
            thou_sep = "."

        # Remove thousands separators
        parts = []
        for ch in s:
            if ch == thou_sep:
                continue
            parts.append(ch)
        s_clean = "".join(parts)

        if dec_sep == ",":
            s_clean = s_clean.replace(",", ".")

        try:
            value = float(s_clean.replace("-", ""))
        except ValueError:
            return None

    return -value if negative else value


def detect_transaction_type(description: str, amount_value: float) -> str:
    """Classify transaction as Expense, Income or Transfer."""
    desc = (description or "").lower()

    if any(k in desc for k in TRANSFER_KEYWORDS):
        return "Transfer"

    if any(k in desc for k in EXPENSE_KEYWORDS):
        return "Expense"

    if any(k in desc for k in INCOME_KEYWORDS):
        return "Income"

    # Fallback by sign
    if amount_value < 0:
        return "Expense"
    if amount_value > 0:
        return "Income"

    return "Transfer"


def categorize_transaction(description: str, transaction_type: Optional[str] = None) -> str:
    """
    Heuristic category assignment based on description text and transaction type.
    Category names are chosen to match typical Budgi buckets.
    """
    desc = (description or "").lower()

    for category, keywords in CATEGORY_KEYWORDS.items():
        if any(k in desc for k in keywords):
            return category

    # Fallback by type
    if transaction_type == "Income":
        return "Income"
    if transaction_type == "Transfer":
        return "Transfer"

    return "Other"


def extract_from_text_lines(text: str) -> List[Dict[str, Any]]:
    """Fallback line-based extraction when tables are not reliable."""
    results: List[Dict[str, Any]] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue

        date_match = DATE_PATTERN.search(line)
        amount_matches = AMOUNT_PATTERN.findall(line)

        if not date_match or not amount_matches:
            continue

        raw_date = date_match.group(0)
        iso_date = parse_date(raw_date)
        if not iso_date:
            continue

        raw_amount = amount_matches[-1]  # last number in line is usually amount
        amount_value = clean_amount_to_float(raw_amount)
        if amount_value is None:
            continue

        # Build description = line without date and amount
        desc = line
        desc = desc.replace(raw_date, "").replace(raw_amount, "").strip()

        tx_type = detect_transaction_type(desc, amount_value)
        category = categorize_transaction(desc, tx_type)

        results.append({
            "date": iso_date,
            "description": desc,
            "amount": f"{amount_value:.2f}",
            "currency": DEFAULT_CURRENCY,
            "confidence": 0.6,
            "transaction_type": tx_type,
            "category": category,
        })

    return results


# --- Main extraction -------------------------------------------------------

def extract_transactions(pdf_path: str, metadata: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Main entrypoint.

    Returns:
        List of dicts with keys:
        - date (YYYY-MM-DD)
        - description
        - amount (string, signed)
        - category
        - currency
        - confidence
        - transaction_type
    """
    logger.info(f"Starting extraction from PDF: {pdf_path}")
    results: List[Dict[str, Any]] = []

    currency = DEFAULT_CURRENCY
    if metadata and isinstance(metadata, dict):
        currency = metadata.get("currency") or DEFAULT_CURRENCY

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page_idx, page in enumerate(pdf.pages, start=1):
                logger.info(f"Processing page {page_idx}/{len(pdf.pages)}")

                tables = page.extract_tables()
                page_results: List[Dict[str, Any]] = []

                if tables:
                    for table in tables:
                        if not table:
                            continue
                        for row in table:
                            if not row:
                                continue
                            row_text = " ".join(cell or "" for cell in row)
                            row_text = row_text.strip()
                            if not row_text:
                                continue

                            date_match = DATE_PATTERN.search(row_text)
                            amount_matches = AMOUNT_PATTERN.findall(row_text)
                            if not date_match or not amount_matches:
                                continue

                            raw_date = date_match.group(0)
                            iso_date = parse_date(raw_date)
                            if not iso_date:
                                continue

                            raw_amount = amount_matches[-1]
                            amount_value = clean_amount_to_float(raw_amount)
                            if amount_value is None:
                                continue

                            desc = row_text.replace(raw_date, "").replace(raw_amount, "").strip()

                            tx_type = detect_transaction_type(desc, amount_value)
                            category = categorize_transaction(desc, tx_type)

                            page_results.append({
                                "date": iso_date,
                                "description": desc,
                                "amount": f"{amount_value:.2f}",
                                "currency": currency,
                                "confidence": 0.8,
                                "transaction_type": tx_type,
                                "category": category,
                            })

                # If table-based extraction failed, fall back to text lines
                if not page_results:
                    logger.info(f"No structured tables found on page {page_idx}, falling back to text lines")
                    text = page.extract_text() or ""
                    page_results = extract_from_text_lines(text)
                    # Override currency and add page info
                    for tx in page_results:
                        tx["currency"] = currency

                results.extend(page_results)

        # If absolutely no transactions found, try OCR on whole document as last resort
        if not results:
            logger.warning("No transactions found via pdfplumber, falling back to OCR (expensive)")
            ocr_results: List[Dict[str, Any]] = []
            doc = fitz.open(pdf_path)
            for page_idx in range(len(doc)):
                page = doc.load_page(page_idx)
                pix = page.get_pixmap(dpi=200)
                img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                text = pytesseract.image_to_string(img, lang="eng+tur")
                ocr_page_results = extract_from_text_lines(text)
                for tx in ocr_page_results:
                    tx["currency"] = currency
                ocr_results.extend(ocr_page_results)

            results = ocr_results

        logger.info(f"Extraction finished: {len(results)} transactions")
        return results

    except Exception as e:
        logger.exception(f"Error extracting transactions from {pdf_path}: {e}")
        return []
