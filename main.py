# main.py
import io
import os
import json
from typing import List, Optional, Any, Dict, Tuple

from fastapi import FastAPI, File, UploadFile, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field

from pdf_extractor import extract_transactions
from supabase_utils import (
    download_file_from_supabase,
    get_user_id_from_bearer,
    save_transactions_to_db,
)

SERVICE_VERSION = "v0.6.0"

# -------------------------
# App & CORS
# -------------------------
app = FastAPI(title="Budgy Document Processor", version=SERVICE_VERSION)

CORS_ENABLED = (os.getenv("CORS_ENABLED", "true").lower() == "true")
ALLOWED_ORIGINS = [
    o.strip()
    for o in os.getenv("ALLOWED_ORIGINS", "*").split(",")
    if o.strip()
]
if CORS_ENABLED:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=ALLOWED_ORIGINS if ALLOWED_ORIGINS else ["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],  # includes Authorization
        expose_headers=["*"],
    )

# -------------------------
# Models
# -------------------------
class ProcessDocumentRequest(BaseModel):
    file_path: str = Field(..., description="Supabase Storage path, e.g., <user-id>/statements/xxx.pdf")
    bucket_name: str = Field("documents", description="Supabase Storage bucket name")
    document_id: Optional[str] = Field(None)
    user_id: Optional[str] = Field(None)

class TransactionRow(BaseModel):
    date: Optional[str] = None
    description: Optional[str] = None
    amount: Optional[Any] = None
    currency: Optional[str] = None
    category: Optional[str] = None
    type: Optional[str] = None  # expense/income (optional)
    document_id: Optional[str] = None
    account_name: Optional[str] = None

class ConfirmTransactionsRequest(BaseModel):
    file_path: Optional[str] = None
    document_id: Optional[str] = None
    user_id: Optional[str] = None
    transactions: List[TransactionRow]

# -------------------------
# Helpers
# -------------------------
def _extract(pdf_bytes: bytes) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Returns (transactions, meta) where meta contains:
    - extraction_method
    - extraction_quality
    - warnings (list)
    """
    try:
        tx, meta = extract_transactions(pdf_bytes)
        if not isinstance(meta, dict):
            meta = {}
        return tx, {
            "extraction_method": meta.get("extraction_method", "auto"),
            "extraction_quality": meta.get("extraction_quality", "medium"),
            "warnings": meta.get("warnings", []),
        }
    except Exception as e:
        return [], {
            "extraction_method": "error",
            "extraction_quality": "low",
            "warnings": [f"Extractor error: {str(e)}"],
        }

# -------------------------
# Health
# -------------------------
@app.get("/health")
def health():
    return {"ok": True, "service": "budgy-document-processor", "version": SERVICE_VERSION}

# Allow preflight on all POST routes
@app.options("/{rest_of_path:path}")
def preflight(rest_of_path: str):
    return PlainTextResponse("", status_code=204)

# -------------------------
# Multipart: /process-pdf
# -------------------------
@app.post("/process-pdf")
async def process_pdf(
    request: Request,
    file: UploadFile = File(...),
    authorization: Optional[str] = Header(default=None),
):
    try:
        pdf_bytes = await file.read()
        transactions, meta = _extract(pdf_bytes)

        return {
            "success": True,
            "transactions": transactions,
            "extraction_method": meta.get("extraction_method"),
            "extraction_quality": meta.get("extraction_quality"),
            "warnings": meta.get("warnings", []),
            "processor_version": SERVICE_VERSION,
            "message": "Processing completed",
        }
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"success": False, "error": "PROCESS_PDF_FAILED", "details": str(e)},
        )

# -------------------------
# JSON: /process-document (supabase storage path)
# -------------------------
@app.post("/process-document")
async def process_document(body: ProcessDocumentRequest):
    try:
        pdf_bytes = download_file_from_supabase(body.file_path, bucket=body.bucket_name)
        if not pdf_bytes:
            return JSONResponse(
                status_code=404,
                content={
                    "success": False,
                    "error": "FILE_NOT_FOUND",
                    "file_path": body.file_path,
                    "bucket": body.bucket_name,
                },
            )

        transactions, meta = _extract(pdf_bytes)
        return {
            "success": True,
            "document_id": body.document_id,
            "file_path": body.file_path,
            "transactions": transactions,
            "extraction_method": meta.get("extraction_method"),
            "extraction_quality": meta.get("extraction_quality"),
            "warnings": meta.get("warnings", []),
            "processor_version": SERVICE_VERSION,
            "message": "Processing completed",
        }
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"success": False, "error": "PROCESS_DOCUMENT_FAILED", "details": str(e)},
        )

# -------------------------
# Persist: /confirm-transactions
# -------------------------
@app.post("/confirm-transactions")
async def confirm_transactions(
    body: ConfirmTransactionsRequest,
    authorization: Optional[str] = Header(default=None),
):
    try:
        # Resolve user id: prefer Bearer token → auth.user, else payload.user_id
        resolved_user_id = body.user_id
        if not resolved_user_id and authorization and authorization.startswith("Bearer "):
            token = authorization.replace("Bearer ", "").strip()
            resolved_user_id = get_user_id_from_bearer(token)

        if not resolved_user_id:
            return JSONResponse(
                status_code=401,
                content={"success": False, "error": "UNAUTHORIZED", "details": "User id not resolved"},
            )

        inserted = save_transactions_to_db(
            [t.model_dump() for t in body.transactions],
            file_path=(body.file_path or ""),
            user_id=resolved_user_id,
            document_id=body.document_id,
        )
        return {
            "success": True,
            "inserted_count": inserted,
            "user_id": resolved_user_id,
        }
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"success": False, "error": "CONFIRM_TRANSACTIONS_FAILED", "details": str(e)},
        )
