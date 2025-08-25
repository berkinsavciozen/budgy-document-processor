# supabase_utils.py  (ADD these helpers; keep existing functions)
import os, json, requests
from typing import List, Dict, Any, Optional

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")
ANON_KEY = os.getenv("SUPABASE_ANON_KEY", "")

SESSION = requests.Session()
SESSION_TIMEOUT = 60

# ... (keep your existing functions here) ...

def fetch_user_rules(user_id: Optional[str]) -> List[Dict[str, Any]]:
    if not user_id:
        return []
    url = f"{SUPABASE_URL}/rest/v1/user_category_rules?user_id=eq.{user_id}"
    headers = {
        "Authorization": f"Bearer {SERVICE_KEY}",
        "apikey": SERVICE_KEY,
        "Accept": "application/json",
    }
    r = SESSION.get(url, headers=headers, timeout=SESSION_TIMEOUT)
    if r.status_code == 200:
        try:
            return r.json()
        except Exception:
            return []
    return []

def upsert_user_rules(user_id: str, patterns: List[Dict[str, Any]]) -> int:
    """
    patterns: [{pattern, category_main, category_sub, weight?}]
    """
    if not user_id or not patterns:
        return 0
    url = f"{SUPABASE_URL}/rest/v1/user_category_rules"
    headers = {
        "Authorization": f"Bearer {SERVICE_KEY}",
        "apikey": SERVICE_KEY,
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }
    payload = []
    for p in patterns:
        payload.append({
            "user_id": user_id,
            "pattern": p.get("pattern"),
            "category_main": p.get("category_main"),
            "category_sub": p.get("category_sub"),
            "weight": p.get("weight", 1.0),
        })
    r = SESSION.post(url, headers=headers, data=json.dumps(payload), timeout=SESSION_TIMEOUT)
    if r.status_code in (200, 201):
        try:
            return len(r.json())
        except Exception:
            return len(payload)
    raise RuntimeError(f"Upsert user rules failed: {r.status_code} {r.text}")
