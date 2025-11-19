import datetime as dt
import logging
import re
from typing import Any, Dict, List

import fitz  # PyMuPDF

logger = logging.getLogger("budgy-document-processor.pdf_extractor")

# Mapping for bad PDF encoding commonly found in TR bank statements (Identity-H)
CID_MAP = {
    "(cid:3)": " ",
    "(cid:4)": "ı",
    "(cid:10)": "İ",
    "(cid:12)": "ş",
    "(cid:13)": "ü",
    "(cid:19)": "ç",
    "(cid:23)": "ğ",
    "(cid:24)": "ö",
    "(cid:0)": "Ö", 
    "(cid:8)": "Ö",
    "(cid:9)": "Ş", 
    "(cid:68)": "Ü",
    "(cid:22)": "ğ",
}

# General string replacements (a list of tuples, old: new)
REPLACEMENTS = [
    ("deme", "Ödeme"),
    ("ubesi", "Şubesi"),
    ("cret", "Ücret"),
    ("Alıveri", "Alışveriş"),
    ("TKTAKKRAL", "TIKTAKKIRAL"),
    ("STANBUL", "ISTANBUL"),
]

# Rows to explicitly ignore (Summary table headers/content)
IGNORE_PHRASES = [
    "ekstre borcu",
    "dönem özeti",
    "dönem toplamı",
    "hesap özeti",
    "önceki dönem bakiyesi",
    "minimum ödeme",
    "son ödeme",
    "limit",
    "devreden",
    "toplam",
    "sayfa",
    "taksit", 
    "işlem tarihi", 
    "bir önceki",
    "numaralı sanal kredi kartınızla yapılan işlemler"
]

def _clean_pdf_text(text: str) -> str:
    """
    Fix encoding artifacts and normalize whitespace.
    """
    # 1. Fix CIDs (from dictionary)
    for cid, char in CID_MAP.items():
        text = text.replace(cid, char)

    # 2. Fix known encoding/OCR issues (from list of tuples)
    for old, new in REPLACEMENTS:
        text = text.replace(old, new)
    
    # 3. Generic CID remover if any left
    text = re.sub(r"\(cid:\d+\)", "", text)
    
    # FIX: Remove all ASCII control characters (0-31 and 127), which includes '\b'
    text = re.sub(r'[\x00-\x1F\x7F]', '', text)

    # 4. Handle remaining ''
    text = text.replace("", "")
    
    # 5. Whitespace cleanup
    text = re.sub(r"\s+", " ", text).strip()
    return text

def _parse_tr_amount(amount_str: str) -> float:
    """
    Convert '1.582,18 TL' -> 1582.18
    Convert '- 500,00 TL' -> -500.00
    """
    # Remove TL, currency codes, and spaces/non-breaking spaces
    s = amount_str.upper().replace("TL", "").replace("\u00a0", " ").strip()
    
    negative = False
    if s.startswith("-"):
        negative = True
        s = s[1:].strip()
    
    # Remove thousands separator dot, replace decimal comma with dot
    s = s.replace(".", "").replace(",", ".")
    
    try:
        val = float(s)
        return -val if negative else val
    except ValueError:
        return 0.0

def _to_iso_date(tr_date: str) -> str:
    """
    Convert '02/11/2024' -> '2024-11-02'
    """
    try:
        day, month, year = tr_date.split("/")
        return dt.date(int(year), int(month), int(day)).isoformat()
    except ValueError:
        return tr_date

def extract_transactions_from_pdf(pdf_bytes: bytes) -> List[Dict[str, Any]]:
    """
    Robust extraction for Enpara/TR Credit Card statements.
    Strategy: Line-by-line parsing. 
    A transaction block starts with a DATE and ends with an AMOUNT.
    """
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    all_transactions: List[Dict[str, Any]] = []
    
    # Regex to identify start of a row (Date)
    date_re = re.compile(r"^(\d{2}/\d{2}/\d{4})")
    # Regex to identify end of a row (Amount + TL)
    amount_re = re.compile(r"(-? ?[\d\.]+,\d{2}) ?TL$")

    for page_index, page in enumerate(doc):
        # Use 'text' output for raw lines, then clean them up
        raw_text = page.get_text("text")
        if not raw_text:
            continue
            
        clean_lines = []
        for line in raw_text.split('\n'):
            cleaned = _clean_pdf_text(line)
            if cleaned:
                clean_lines.append(cleaned)
        
        # State machine parsing
        current_tx = {}
        
        i = 0
        while i < len(clean_lines):
            line = clean_lines[i]
            
            # Check for Date Start
            date_match = date_re.match(line)
            if date_match:
                current_tx = {
                    "date": _to_iso_date(date_match.group(1)),
                    "description_parts": [],
                    "amount": None
                }
                
                # The rest of this line might contain description text
                remainder = line[len(date_match.group(0)):].strip()
                if remainder:
                    current_tx["description_parts"].append(remainder)
                
                i += 1
                continue

            # If we are inside a transaction, look for description or amount
            if current_tx:
                # Check for Amount (End of transaction)
                amt_match = amount_re.search(line)
                if amt_match:
                    raw_amt = amt_match.group(1)
                    # Description text before the amount on the same line
                    desc_part = line[:amt_match.start()].strip()
                    if desc_part:
                        current_tx["description_parts"].append(desc_part)
                    
                    # Finalize Transaction
                    full_desc = " ".join(current_tx["description_parts"])
                    
                    # Filter summary rows
                    if not any(ignored in full_desc.lower() for ignored in IGNORE_PHRASES):
                        amount_val = _parse_tr_amount(raw_amt)
                        
                        # Determine Type based on Amount Sign
                        # Positive = Expense, Negative = Income/Payment
                        tx_type = "expense" if amount_val >= 0 else "income"
                        
                        all_transactions.append({
                            "date": current_tx["date"],
                            "description": full_desc,
                            "amount": amount_val, 
                            "currency": "TRY",
                            "type": tx_type,
                            "source": "credit_card_statement"
                        })
                    
                    # Reset state
                    current_tx = {}
                else:
                    # Just a description line
                    current_tx["description_parts"].append(line)
            
            i += 1

    # Sort by date descending (newest first)
    all_transactions.sort(key=lambda t: t["date"], reverse=True)
    return all_transactions
