# main.py
import os
import io
import json
from typing import List, Optional, Any, Dict

from fastapi import FastAPI, File, UploadFile, Form, Header, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from supabase_utils import (
    download_file_from_supabase,
    get_user_id_from_bearer,
    save_transactions_to_db,
)

# ---- Try to reuse your existing extractor if present; otherwise use a simple fallback
def _fallback_extract_transactions(pdf_bytes: bytes) -> List[Dict[str, Any]]:
    """
    Very lightweight extractor to prevent outages if your internal extractor import fails.
    It returns an empty list if it can't infer tabular data. Replace with your preferred fallback.
    """
    try:
        import pdfplumber  # type: ignore
        rows: List[Dict[str, Any]] = []
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables() or []
                for tbl in tables:
                    # naive header normalize
                    if not tbl or len(tbl) < 2:
                        continue
                    header = [h.strip().lower() if isinstance(h, str) else "" for h in tbl[0]]
                    for r in tbl[1:]:
                        rec = {header[i] if i < len(header) else f"col_{i}": (r[i] or "").strip() for i in range(len(r))}
                        # Very conservative mapping
                        candidate = {
                            "date": rec.get("date") or rec.get("tarih") or None,
                            "description": rec.get("description") or rec.get("açıklama") or rec.get("aciklama") or None,
                            "amount": rec.get("amount") or rec.get("tutar") or rec.get("islem tutari") or None,
                            "currency": rec.get("currency") or rec.get("döviz") or rec.get("doviz") or None,
                            "category": None,
                        }
                        if candidate["date"] or candidate["description"] or candidate["amount"]:
                            rows.append(candidate)
        return rows
    except Exception:
        return []

def _extract_transactions(pdf_bytes: bytes) -> List[Dict[str, Any]]:
    """
    Wrapper that prefers your repo's canonical extractor if available.
    Expected signature: extract_transactions(pdf_bytes: bytes) -> List[dict]
    """
    # Try common module names you might already have in the repo
    for mod_name in ("extract", "extractor", "parser", "pipeline"):
        try:
            mod = __import__(mod_name)
            if hasattr(mod, "extract_transactions"):
                return getattr(mod, "extract_transactions")(pdf_bytes)
        except Exception:
            continue
    # Fallback
    return _fallback_extract_transactions(pdf_bytes)

# ----- FastAPI app

app = FastAPI(title="budgy-document-processor", version="v0.5.0")

# ----- CORS (configurable via env)
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*")
allow_origins = [o.strip() for o in ALLOWED_ORIGINS.split(",")] if ALLOWED_ORIGINS else ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ----- Health
@app.get("/health")
def health():
    return {"ok": True, "service": "budgy-document-processor", "version": "v0.5.0"}

# ----- Models for JSON endpoints

class ProcessDocumentRequest(BaseModel):
    file_path: str = Field(..., description="Supabase Storage path, e.g., <user-id>/statements/xxx.pdf")
    bucket_name: str = Field("documents", description="Supabase Storage bucket name")
    document_id: Optional[str] = Field(None, description="Optional: id in your documents table")
    user_id: Optional[str] = Field(None, description="Optional: explicit user id")

class TransactionRow(BaseModel):
    date: Optional[str] = None
    description: Optional[str] = None
    amount: Optional[Any] = None
    currency: Optional[str] = None
    category: Optional[str] = None

class ConfirmTransactionsRequest(BaseModel):
    file_path: Optional[str] = None
    user_id: Optional[str] = None
    transactions: List[TransactionRow]

# ----- Legacy multipart endpoint (kept as-is, but routed through same extractor)
@app.post("/process-pdf")
async def process_pdf(file: UploadFile = File(...), document_id: Optional[str] = Form(None)):
    try:
        content = await file.read()
        tx = _extract_transactions(content)
        return {
            "document_id": document_id,
            "transactions": tx,
            "processor_version": "v0.5.0",
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": "PROCESS_PDF_FAILED", "details": str(e)})

# ----- New OPTIONS for preflight
@app.options("/process-document")
def options_process_document():
    return PlainTextResponse("", status_code=204)

# ----- New JSON endpoint: process from Supabase Storage path
@app.post("/process-document")
async def process_document(req: ProcessDocumentRequest):
    try:
        pdf_bytes = download_file_from_supabase(req.file_path, bucket=req.bucket_name)
        if not pdf_bytes:
            return JSONResponse(
                status_code=404,
                content={"error": "FILE_NOT_FOUND", "file_path": req.file_path, "bucket": req.bucket_name},
            )
        tx = _extract_transactions(pdf_bytes)
        # NOTE: you can optionally upsert document processing log here
        return {
            "document_id": req.document_id,
            "file_path": req.file_path,
            "transactions": tx,
            "processor_version": "v0.5.0",
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": "PROCESS_DOCUMENT_FAILED", "details": str(e)})

# ----- Persist extracted/edited transactions into your DB
@app.post("/confirm-transactions")
async def confirm_transactions(
    request: Request,
    body: ConfirmTransactionsRequest,
    authorization: Optional[str] = Header(None, convert_underscores=False),
):
    try:
        # Resolve user_id from bearer, unless caller provided it explicitly
        resolved_user_id: Optional[str] = body.user_id
        bearer_token: Optional[str] = None
        if authorization and authorization.lower().startswith("bearer "):
            bearer_token = authorization.split(" ", 1)[1].strip()
        if not resolved_user_id and bearer_token:
            resolved_user_id = get_user_id_from_bearer(bearer_token)

        inserted = save_transactions_to_db(
            [t.model_dump() for t in body.transactions],
            file_path=body.file_path,
            user_id=resolved_user_id,
        )
        return {
            "ok": True,
            "inserted_count": inserted,
            "user_id": resolved_user_id,
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": "CONFIRM_TRANSACTIONS_FAILED", "details": str(e)})
