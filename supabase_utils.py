# supabase_utils.py
import os
import json
import requests
from typing import List, Dict, Any, Optional

# Public API of this module
__all__ = [
    "download_file_from_supabase",
    "get_user_id_from_bearer",
    "save_transactions_to_db",
    "fetch_user_rules",
    "upsert_user_rules",
]

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")
ANON_KEY = os.getenv("SUPABASE_ANON_KEY", "")

if not SUPABASE_URL:
    raise RuntimeError("SUPABASE_URL is required")
if not SERVICE_KEY:
    raise RuntimeError("SUPABASE_SERVICE_KEY is required")

SESSION = requests.Session()
SESSION_TIMEOUT = 60


def download_file_from_supabase(path: str, bucket: str = "documents") -> Optional[bytes]:
    """
    Download file bytes from Supabase Storage using the SERVICE key.
    """
    if not path:
        return None
    url = f"{SUPABASE_URL}/storage/v1/object/{bucket}/{path}"
    headers = {"Authorization": f"Bearer {SERVICE_KEY}"}
    resp = SESSION.get(url, headers=headers, timeout=SESSION_TIMEOUT)
    if resp.status_code == 200:
        return resp.content
    raise RuntimeError(f"Storage download failed: {resp.status_code} {resp.text}")


def get_user_id_from_bearer(access_token: str) -> Optional[str]:
    """
    Resolve user id via /auth/v1/user using the user's access token.
    """
    if not access_token:
        return None
    url = f"{SUPABASE_URL}/auth/v1/user"
    headers = {"Authorization": f"Bearer {access_token}"}
    resp = SESSION.get(url, headers=headers, timeout=SESSION_TIMEOUT)
    if resp.status_code == 200:
        try:
            return resp.json().get("id")
        except Exception:
            return None
    return None


def _build_insert_row(row: Dict[str, Any], file_path: str, user_id: str, document_id: Optional[str]) -> Dict[str, Any]:
    # Legacy 'category' mirrors 'category_main' for backward compatibility
    category_main = row.get("category_main") or row.get("category")
    return {
        "user_id": user_id,
        "file_path": file_path,
        "date": row.get("date"),
        "description": row.get("description"),
        "amount": str(row.get("amount")) if row.get("amount") is not None else None,
        "currency": row.get("currency"),
        "type": row.get("type"),  # 'income' | 'expense'
        "category_main": category_main,
        "category_sub": row.get("category_sub"),
        "category": category_main,  # keep legacy in sync
        "document_id": document_id or row.get("document_id"),
    }


def save_transactions_to_db(
    rows: List[Dict[str, Any]],
    file_path: str,
    user_id: str,
    document_id: Optional[str] = None,
) -> int:
    """
    Bulk insert into public.transactions via PostgREST using the SERVICE key.
    """
    if not rows:
        return 0
    url = f"{SUPABASE_URL}/rest/v1/transactions"
    headers = {
        "Authorization": f"Bearer {SERVICE_KEY}",
        "apikey": SERVICE_KEY,
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }
    payload = [_build_insert_row(r, file_path=file_path, user_id=user_id, document_id=document_id) for r in rows]
    resp = SESSION.post(url, headers=headers, data=json.dumps(payload), timeout=SESSION_TIMEOUT)
    if resp.status_code in (200, 201):
        try:
            data = resp.json()
            return len(data) if isinstance(data, list) else len(payload)
        except Exception:
            return len(payload)
    raise RuntimeError(f"Supabase insert failed: {resp.status_code} {resp.text}")


def fetch_user_rules(user_id: Optional[str]) -> List[Dict[str, Any]]:
    """
    Fetch per-user self-learning category rules.
    """
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
    Upsert pattern rules: [{pattern, category_main, category_sub?, weight?}]
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
