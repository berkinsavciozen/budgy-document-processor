# supabase_utils.py
import os
import time
from typing import List, Dict, Any, Optional
import requests

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY", "")

if not SUPABASE_URL:
    raise RuntimeError("SUPABASE_URL is required")
if not SUPABASE_SERVICE_KEY:
    raise RuntimeError("SUPABASE_SERVICE_KEY is required")
if not SUPABASE_ANON_KEY:
    # Not hard-failing here, because some deployments might not use /auth/v1/user resolution
    pass

SESSION = requests.Session()
SESSION.headers.update({"Content-Type": "application/json"})

def _rest_headers(service: bool = True) -> Dict[str, str]:
    key = SUPABASE_SERVICE_KEY if service else SUPABASE_ANON_KEY
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

def _storage_headers() -> Dict[str, str]:
    # For Storage we use the service key to bypass RLS from the server-side processor
    return {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    }

def download_file_from_supabase(file_path: str, bucket: str = "documents") -> Optional[bytes]:
    """
    Downloads a file from Supabase Storage using the service role key.
    """
    if not file_path:
        return None
    url = f"{SUPABASE_URL}/storage/v1/object/{bucket}/{file_path}"
    resp = SESSION.get(url, headers=_storage_headers(), timeout=60)
    if resp.status_code == 200:
        return resp.content
    return None

def get_user_id_from_bearer(user_bearer_token: str) -> Optional[str]:
    """
    Resolve user id from a user's JWT:
    GET /auth/v1/user with headers:
      Authorization: Bearer <user token>
      apikey: <anon or service key>
    """
    if not user_bearer_token:
        return None
    url = f"{SUPABASE_URL}/auth/v1/user"
    headers = {
        "Authorization": f"Bearer {user_bearer_token}",
        "apikey": SUPABASE_ANON_KEY or SUPABASE_SERVICE_KEY,
    }
    resp = SESSION.get(url, headers=headers, timeout=30)
    if resp.status_code == 200:
        try:
            data = resp.json()
            # supabase returns { id, ... }
            return data.get("id")
        except Exception:
            return None
    return None

def _normalize_amount(val: Any) -> Optional[float]:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    if not s:
        return None
    # handle (1.234,56) and -1.234,56 and 1,234.56
    # unify thousand separators
    s = s.replace(" ", "")
    # parentheses as negative
    negative = False
    if s.startswith("(") and s.endswith(")"):
        negative = True
        s = s[1:-1]
    # try Turkish format first: 1.234,56
    try:
        s_tr = s.replace(".", "").replace(",", ".")
        amount = float(s_tr)
        if negative:
            amount = -amount
        return amount
    except Exception:
        pass
    # try plain float
    try:
        amount = float(s)
        if negative:
            amount = -amount
        return amount
    except Exception:
        return None

def _normalize_row(row: Dict[str, Any], file_path: Optional[str], user_id: Optional[str]) -> Dict[str, Any]:
    return {
        "date": row.get("date"),
        "description": row.get("description"),
        "amount": _normalize_amount(row.get("amount")),
        "currency": row.get("currency") or "TRY",
        "category": row.get("category") or None,
        "user_id": user_id,
        "source_file_path": file_path,
    }

def save_transactions_to_db(rows: List[Dict[str, Any]], file_path: Optional[str], user_id: Optional[str]) -> int:
    """
    Inserts transactions into public.transactions via PostgREST.

    Expect a table with columns at least:
      date (text or date), description (text), amount (numeric), currency (text),
      category (text, nullable), user_id (uuid, nullable), source_file_path (text, nullable)

    Returns number of inserted rows.
    """
    if not rows:
        return 0

    payload = [_normalize_row(r, file_path=file_path, user_id=user_id) for r in rows]

    url = f"{SUPABASE_URL}/rest/v1/transactions"
    headers = _rest_headers(service=True)
    # Prefer bulk insert
    resp = SESSION.post(url, headers=headers, json=payload, timeout=60, params={"return": "representation"})
    if resp.status_code in (200, 201):
        try:
            data = resp.json()
            return len(data) if isinstance(data, list) else len(payload)
        except Exception:
            return len(payload)
    else:
        # Surface error as exception for upstream handler
        try:
            raise RuntimeError(f"Supabase insert failed: {resp.status_code} {resp.text}")
        except Exception:
            raise
