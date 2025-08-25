# supabase_utils.py
import os
import json
import requests
from typing import List, Dict, Any, Optional

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
    Downloads a file bytes from Supabase Storage using service key.
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
    Resolve user id via /auth/v1/user using the user's own access token.
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
    out = {
        "user_id": user_id,
        "file_path": file_path,
        "date": row.get("date"),
        "description": row.get("description"),
        "amount": str(row.get("amount")) if row.get("amount") is not None else None,
        "currency": row.get("currency"),
        "category": row.get("category"),
        "type": row.get("type"),
        "document_id": document_id or row.get("document_id"),
    }
    # Clean keys with None → leave as null; PostgREST will handle.
    return out

def save_transactions_to_db(
    rows: List[Dict[str, Any]],
    file_path: str,
    user_id: str,
    document_id: Optional[str] = None
) -> int:
    """
    Bulk insert transactions via PostgREST.
    Returns number of inserted rows or raises RuntimeError.
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
