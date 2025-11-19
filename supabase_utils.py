import logging
import os
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests

logger = logging.getLogger("budgy-document-processor.supabase")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")  # optional, mostly unused here
SUPABASE_STORAGE_BUCKET = os.getenv("SUPABASE_STORAGE_BUCKET", "")  # optional
SUPABASE_TRANSACTIONS_TABLE = os.getenv("SUPABASE_TRANSACTIONS_TABLE", "transactions")

if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    logger.warning(
        "SUPABASE_URL or SUPABASE_SERVICE_KEY is not set. Supabase operations will fail."
    )

SESSION = requests.Session()
DEFAULT_TIMEOUT = 30


def _supabase_headers(auth_with_service: bool = True) -> Dict[str, str]:
    key = SUPABASE_SERVICE_KEY if auth_with_service else SUPABASE_ANON_KEY
    if not key:
        key = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY or ""
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }


# ---------- AUTH HELPERS ----------


def get_user_id_from_bearer(authorization_header: Optional[str]) -> Optional[str]:
    """
    Decode the Supabase JWT via /auth/v1/user and return user id.
    """
    if not authorization_header:
        return None

    parts = authorization_header.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None

    token = parts[1]
    try:
        resp = SESSION.get(
            f"{SUPABASE_URL}/auth/v1/user",
            headers={
                "apikey": SUPABASE_SERVICE_KEY,
                "Authorization": f"Bearer {token}",
            },
            timeout=DEFAULT_TIMEOUT,
        )
    except Exception as exc:
        logger.error("Error contacting Supabase auth: %s", exc)
        return None

    if not resp.ok:
        logger.warning("Supabase /auth/v1/user returned %s: %s", resp.status_code, resp.text)
        return None

    data = resp.json()
    # supabase-py style vs raw REST – we handle both shapes
    if isinstance(data, dict):
        if "id" in data:
            return data["id"]
        if "user" in data and isinstance(data["user"], dict):
            return data["user"].get("id")

    return None


# ---------- STORAGE HELPERS ----------


def _build_storage_url_from_path(file_path: str) -> str:
    """
    Accepts:
      - full URL to Supabase object ⇒ returns it as-is
      - 'bucket/path/to/file.pdf' ⇒ builds /storage/v1/object/bucket/path/to/file.pdf
      - 'path/to/file.pdf' with SUPABASE_STORAGE_BUCKET ⇒ uses that bucket
    """
    parsed = urlparse(file_path)
    if parsed.scheme in {"http", "https"}:
        return file_path

    if "/" not in file_path and SUPABASE_STORAGE_BUCKET:
        full_path = f"{SUPABASE_STORAGE_BUCKET}/{file_path}"
    else:
        full_path = file_path.lstrip("/")

    return f"{SUPABASE_URL}/storage/v1/object/{full_path}"


def download_file_from_supabase(file_path: str) -> Optional[bytes]:
    """
    Download a PDF from Supabase Storage.
    """
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        logger.error("Supabase environment variables are missing")
        return None

    url = _build_storage_url_from_path(file_path)
    logger.info("Downloading PDF from %s", url)

    try:
        resp = SESSION.get(
            url,
            headers={
                "apikey": SUPABASE_SERVICE_KEY,
                "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
            },
            timeout=DEFAULT_TIMEOUT,
        )
    except Exception as exc:
        logger.error("Error downloading from Supabase Storage: %s", exc)
        return None

    if not resp.ok:
        logger.error(
            "Supabase Storage GET failed with %s: %s", resp.status_code, resp.text
        )
        return None

    return resp.content


# ---------- DATABASE HELPERS ----------


def save_transactions_to_db(transactions: List[Dict[str, Any]]) -> int:
    """
    Insert confirmed transactions into Supabase REST table.
    Returns the number of rows we *attempted* to insert.
    """
    if not transactions:
        return 0

    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        raise RuntimeError("Supabase configuration is missing")

    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TRANSACTIONS_TABLE}"
    logger.info("Inserting %d transactions into %s", len(transactions), url)

    try:
        resp = SESSION.post(
            url,
            headers={
                **_supabase_headers(auth_with_service=True),
                "Prefer": "return=minimal",
            },
            json=transactions,
            timeout=DEFAULT_TIMEOUT,
        )
    except Exception as exc:
        logger.error("Error during Supabase insert: %s", exc)
        raise

    if not resp.ok:
        logger.error(
            "Supabase insert failed with %s: %s", resp.status_code, resp.text
        )
        raise RuntimeError(
            f"Supabase insert failed with status {resp.status_code}: {resp.text}"
        )

    return len(transactions)
