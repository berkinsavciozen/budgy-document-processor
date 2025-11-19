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
    "(cid:0)": "Ö", # Sometimes appears as bullet or O-umlaut
    "(cid:8)": "Ö",
    "(cid:9)": "Ş", 
    "(cid:68)": "Ü",
    "(cid:22)": "ğ",
}

# Rows to explicitly ignore (Summary table headers/content)
IGNORE_PHRASES = [
    "ekstre borcu",
    "minimum ödeme",
    "son ödeme",
    "limit",
    "önceki dönem",
    "devreden",
    "toplam",
    "sayfa",
    "taksit", # Header row
    "işlem tarihi" # Header row
]

def _clean_pdf_text(text: str) -> str:
    """
    Fix encoding artifacts and normalize whitespace.
    """
    # 1. Fix CIDs
    for cid, char in CID_MAP.items():
        text = text.replace(cid, char)
    
    # 2. Generic CID remover if any left
    text = re.sub(r"\(cid:\d+\)", "", text)
    
    # 3. Whitespace cleanup
    text = re.sub(r"\s+", " ", text).strip()
    return text

def _parse_tr_amount(amount_str: str) -> float:
    """
    Convert '1.582,18 TL' -> 1582.18
    Convert '- 500,00 TL' -> -500.00
    """
    # Remove TL and spaces
    s = amount_str.upper().replace("TL", "").strip()
    # Handle negative sign with space '- 500'
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

    for page in doc:
        # Get text blocks to preserve some structure
        blocks = page.get_text("blocks")
        # Sort blocks by vertical position (top to bottom)
        blocks.sort(key=lambda b: b[1])
        
        for b in blocks:
            # b[4] is the text content of the block
            raw_text = b[4]
            clean_lines = []
            
            # Pre-process lines in the block
            for line in raw_text.split('\n'):
                cleaned = _clean_pdf_text(line)
                if cleaned:
                    clean_lines.append(cleaned)
            
            # Parse lines statefully
            current_tx = {}
            
            i = 0
            while i < len(clean_lines):
                line = clean_lines[i]
                
                # Check for Date Start
                date_match = date_re.match(line)
                if date_match:
                    # If we were building a previous transaction without an amount, discard it (it was likely garbage)
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
                        # Found amount. Capture it.
                        raw_amt = amt_match.group(1)
                        # Sometimes description text is before the amount on the same line
                        desc_part = line[:amt_match.start()].strip()
                        if desc_part:
                            current_tx["description_parts"].append(desc_part)
                        
                        # Finalize Transaction
                        full_desc = " ".join(current_tx["description_parts"])
                        
                        # Filter summary rows
                        if not any(ignored in full_desc.lower() for ignored in IGNORE_PHRASES):
                            # Parse Amount
                            amount_val = _parse_tr_amount(raw_amt)
                            
                            # Determine Type based on Amount Sign
                            # Positive = Expense, Negative = Income/Payment
                            tx_type = "expense" if amount_val > 0 else "income"
                            
                            all_transactions.append({
                                "date": current_tx["date"],
                                "description": full_desc,
                                "amount": amount_val, # Keep signed float
                                "currency": "TRY",
                                "type": tx_type,
                                "source": "credit_card_statement"
                            })
                        
                        # Reset
                        current_tx = {}
                    else:
                        # Just a description line
                        current_tx["description_parts"].append(line)
                
                i += 1

    # Sort by date descending (newest first)
    all_transactions.sort(key=lambda t: t["date"], reverse=True)
    return all_transactions
