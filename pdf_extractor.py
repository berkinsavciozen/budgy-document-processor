import datetime as dt
import logging
import re
from typing import Any, Dict, List

import fitz  # PyMuPDF

logger = logging.getLogger("budgy-document-processor.pdf_extractor")


DATE_PATTERN = re.compile(r"(\d{2}/\d{2}/\d{4})")
TRANSACTION_PATTERN = re.compile(
    r"(\d{2}/\d{2}/\d{4})\|([^|]+?)\| *(-?\s?[\d\.]+,\d{2}) TL"
)


# --- CLEANUP HELPERS ---


REPLACEMENTS = [
    ("�deme", "Ödeme"),
    ("�ubesi", "Şubesi"),
    ("�cret", "Ücret"),
    ("Alı�veri�", "Alışveriş"),
    ("T�KTAKK�RAL", "TIKTAKKIRAL"),
    ("�STANBUL", "ISTANBUL"),
    ("NOMUPA", "NOMUPA"),  # keep brand as-is, just for completeness
]


SUMMARY_KEYWORDS = [
    "ekstre borcu",
    "dönem özeti",
    "dönem toplamı",
    "hesap özeti",
    "önceki dönem bakiyesi",
]


def _clean_description(desc: str) -> str:
    text = desc.replace("\n", " ").replace("|", " ")
    for old, new in REPLACEMENTS:
        text = text.replace(old, new)

    # Some remaining "�" characters – best effort: drop them
    text = text.replace("�", "")

    # Collapse spaces
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _parse_tr_amount(amount_str: str) -> float:
    """
    Convert strings like '1.582,18' or '- 500,00' to float.
    """
    s = amount_str.replace(" ", "").replace("\u00a0", "")
    negative = s.startswith("-")
    s = s.lstrip("-")
    s = s.replace(".", "")
    integer, frac = s.split(",")
    value = float(f"{integer}.{frac}")
    return -value if negative else value


def _to_iso_date(tr_date: str) -> str:
    """
    Convert '02/11/2024' -> '2024-11-02'
    """
    day, month, year = tr_date.split("/")
    return dt.date(int(year), int(month), int(day)).isoformat()


def _is_summary_row(desc: str) -> bool:
    lowered = desc.lower()
    return any(keyword in lowered for keyword in SUMMARY_KEYWORDS)


def _classify_type(description: str, amount: float) -> str:
    """
    Classify as 'expense' or 'income'.

    Strategy (aligned with credit card statements like Enpara):
    - Card charges / interest → positive amounts → EXPENSE
    - Payments / refunds → negative amounts → INCOME

    You can refine this later with category-specific rules if needed.
    """
    if amount < 0:
        return "income"
    else:
        return "expense"


# --- MAIN PUBLIC FUNCTION ---


def extract_transactions_from_pdf(pdf_bytes: bytes) -> List[Dict[str, Any]]:
    """
    Extract transactions from a TR credit card statement PDF.

    This is tuned for Enpara-style statements (like your sample):
    Each transaction appears in the text as:
        DD/MM/YYYY|<DESCRIPTION>| <AMOUNT> TL
    and the whole page is turned into a single '|' separated string.

    Returns a list of dicts with fields:
        date (YYYY-MM-DD), description, amount (positive),
        currency ('TRY'), type ('income'/'expense'), source, ...
    """
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    all_transactions: List[Dict[str, Any]] = []

    for page_index, page in enumerate(doc):
        raw_text = page.get_text()
        if not raw_text:
            continue

        text = raw_text.replace("\n", "|")
        matches = TRANSACTION_PATTERN.findall(text)

        logger.info("Page %d: found %d transaction candidates", page_index, len(matches))

        for tr_date, raw_desc, raw_amount in matches:
            desc = _clean_description(raw_desc)
            if not desc or _is_summary_row(desc):
                # Skip summary lines like 'Ekstre borcu'
                continue

            try:
                amount = _parse_tr_amount(raw_amount)
            except Exception as exc:
                logger.warning(
                    "Skipping row due to amount parse error: '%s' (%s)", raw_amount, exc
                )
                continue

            # Classification and normalisation
            tx_type = _classify_type(desc, amount)
            iso_date = _to_iso_date(tr_date)

            transaction = {
                "date": iso_date,
                "description": desc,
                # we store absolute amount and use 'type' for direction
                "amount": abs(amount),
                "currency": "TRY",
                "type": tx_type,
                "category_main": None,
                "category_sub": None,
                "source": "credit_card_statement",
            }

            all_transactions.append(transaction)

    # Sort ascending then let API sort if needed; here we keep natural ascending
    all_transactions.sort(key=lambda t: t["date"])
    return all_transactions
