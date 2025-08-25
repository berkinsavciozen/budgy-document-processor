# categorizer.py
import re
from typing import Optional, Tuple, List, Dict
from category_taxonomy import (
    INCOME_MAIN, INCOME_SUB, EXPENSE_MAIN, EXPENSE_SUB,
    ALL_MAIN, KEYWORD_MAP, MCC_MAP
)

_IBAN_RX = re.compile(r'\bTR\d{24}\b', re.IGNORECASE)
_MCC_RX  = re.compile(r'\bMCC\W?(\d{4})\b', re.IGNORECASE)

def _norm(s: Optional[str]) -> str:
    return (s or "").strip().lower()

# -------- Self-learning rule shape --------
# user_rules: List[{"pattern": "yemeksepeti", "category_main": "...", "category_sub": "...", "weight": 1.0}]
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
    hits = [(k, v) for k, v in KEYWORD_MAP.items() if k in desc_lc]
    if not hits:
        return None
    hits.sort(key=lambda kv: len(kv[0]), reverse=True)
    return hits[0][1]

def _by_rules(desc_lc: str, tx_type: Optional[str]) -> Optional[Tuple[str, str]]:
    # Transfers/IBAN
    if _IBAN_RX.search(desc_lc) or "eft" in desc_lc or "havale" in desc_lc or "fast" in desc_lc:
        return ("Debts & Liabilities", "Loan Payment")
    # Income cues
    if tx_type == "income" or ("maas" in desc_lc or "maaş" in desc_lc or "salary" in desc_lc):
        return ("Salary & Wages", "Base Salary")
    # Cash/ATM
    if "nakit" in desc_lc or "atm" in desc_lc:
        return ("Miscellaneous", "Unplanned Purchases")
    # Fees/taxes
    if "komisyon" in desc_lc or "hesap i̇şletim" in desc_lc or "hesap işletim" in desc_lc or "fee" in desc_lc:
        return ("Taxes & Fees", "Service Charges")
    if "bsmv" in desc_lc or "kkdf" in desc_lc or "vergi" in desc_lc or "tax" in desc_lc:
        return ("Taxes & Fees", "Income Tax")
    return None

def _coerce_to_side(main: str, tx_type: str) -> str:
    """If a rule picked the wrong side (e.g., income label for an expense), nudge to a plausible default."""
    if tx_type == "income" and main in EXPENSE_SUB:
        return "Other Income"
    if tx_type == "expense" and main in INCOME_SUB:
        return "Miscellaneous"
    return main

def categorize(description: Optional[str], tx_type: Optional[str], user_rules: Optional[List[Dict]] = None) -> Tuple[str, str]:
    """
    Return (category_main, category_sub) based on:
    user_rules → MCC → keywords → heuristics. Then coerces to correct side by tx_type.
    """
    desc_lc = _norm(description)

    # 1) user rules win
    hit = _by_user_rules(desc_lc, user_rules)
    if hit:
        main, sub = hit
        return _coerce_to_side(main, tx_type or "expense"), sub or _default_sub(main)

    # 2) MCC
    hit = _by_mcc(desc_lc)
    if hit:
        main, sub = hit
        return _coerce_to_side(main, tx_type or "expense"), sub or _default_sub(main)

    # 3) Keywords
    hit = _by_keywords(desc_lc)
    if hit:
        main, sub = hit
        return _coerce_to_side(main, tx_type or "expense"), sub or _default_sub(main)

    # 4) Heuristics
    hit = _by_rules(desc_lc, tx_type)
    if hit:
        main, sub = hit
        return _coerce_to_side(main, tx_type or "expense"), sub or _default_sub(main)

    # 5) Fallback by side
    if tx_type == "income":
        return ("Other Income", "Lottery/Prize")
    return ("Miscellaneous", "Unplanned Purchases")

def _default_sub(main: str) -> str:
    if main in INCOME_SUB:
        return INCOME_SUB[main][0]
    if main in EXPENSE_SUB:
        return EXPENSE_SUB[main][0]
    return "Unplanned Purchases"
