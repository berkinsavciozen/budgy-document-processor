# categorizer.py
import re
from typing import Optional, Tuple, List, Dict
from category_taxonomy import (
    INCOME_MAIN, INCOME_SUB, EXPENSE_MAIN, EXPENSE_SUB,
    KEYWORD_MAP, MCC_MAP
)

_IBAN_RX = re.compile(r'\bTR\d{24}\b', re.IGNORECASE)
_MCC_RX  = re.compile(r'\bMCC\W?(\d{4})\b', re.IGNORECASE)

def _norm(s: Optional[str]) -> str:
    return (s or "").strip().lower()

# -------- Self-learning rule shape (Placeholder, currently unused in core logic) --------
def _by_user_rules(desc_lc: str, user_rules: Optional[List[Dict]]) -> Optional[Tuple[str, str]]:
    if not user_rules:
        return None
    hits = []
    for r in user_rules:
        p = _norm(r.get("pattern"))
        if p and p in desc_lc:
            hits.append((len(p), r.get("category_main"), r.get("category_sub"), r.get("weight") or 1.0))
    if not hits:
        return None
    # sort by longest pattern then weight
    hits.sort(key=lambda t: (t[0], t[3]), reverse=True)
    return hits[0][1], hits[0][2]

def _by_mcc(desc_lc: str) -> Optional[Tuple[str, str]]:
    m = _MCC_RX.search(desc_lc)
    if not m:
        return None
    return MCC_MAP.get(m.group(1))

def _by_keywords(desc_lc: str) -> Optional[Tuple[str, str]]:
    hits = []
    for k, v in KEYWORD_MAP.items():
        if k in desc_lc:
            hits.append((k, v))
    
    if not hits:
        return None
    # Sort by length of the keyword match (longest match wins)
    hits.sort(key=lambda x: len(x[0]), reverse=True)
    return hits[0][1]

def _by_rules(desc_lc: str, amount: float) -> Optional[Tuple[str, str]]:
    # 1. FEES & INTEREST
    if "faiz" in desc_lc or "bsmv" in desc_lc or "kkdf" in desc_lc or "ücret" in desc_lc:
        if "alışveriş faizi" in desc_lc:
            return ("Debts & Liabilities", "Overdraft Fees") 
        return ("Taxes & Fees", "Service Charges")

    # 2. PAYMENTS TO CARD (Negative amount on statement)
    if amount < 0 and ("ödeme" in desc_lc or "tahsilat" in desc_lc or "cep şubesi" in desc_lc):
        return ("Debts & Liabilities", "Credit Card Payment")

    # 3. TRANSFERS
    if "eft" in desc_lc or "havale" in desc_lc or "fast" in desc_lc or _IBAN_RX.search(desc_lc):
        if amount < 0:
            return ("Refunds & Adjustments", "Purchase Refunds") # Incoming logic (Transfer credit)
        return ("Debts & Liabilities", "Loan Payment") # Outgoing logic (Transfer debit)

    # 4. CASH
    if "nakit" in desc_lc or "atm" in desc_lc:
        return ("Miscellaneous", "Unplanned Purchases")
        
    return None

def categorize(description: str, amount: float) -> Tuple[str, str]:
    """
    Determine category based on Description and Amount direction.
    Amount < 0 implies a Credit/Payment on a Credit Card statement.
    Amount > 0 implies an Expense.
    """
    desc_lc = _norm(description)

    # 1. Explicit Rules (Fees, Payments, Interest)
    hit = _by_rules(desc_lc, amount)
    if hit:
        return hit

    # 2. Keywords
    hit = _by_keywords(desc_lc)
    if hit:
        return hit

    # 3. Default Fallback based on sign
    if amount < 0:
        # Negative on CC statement = Payment Received or Refund
        return ("Refunds & Adjustments", "Purchase Refunds")
    else:
        # Positive = Spending
        return ("Miscellaneous", "Unplanned Purchases")
