# main.py  (replace your previous main.py with this complete file)
import io, os
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
    fetch_user_rules,
    upsert_user_rules,
)

SERVICE_VERSION = "v0.7.0"

app = FastAPI(title="Budgy Document Processor", version=SERVICE_VERSION)

CORS_ENABLED = (os.getenv("CORS_ENABLED", "true").lower() == "true")
ALLOWED_ORIGINS = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "*").split(",") if o.strip()]
if CORS_ENABLED:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=ALLOWED_ORIGINS if ALLOWED_ORIGINS else ["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["*"],
    )

class ProcessDocumentRequest(BaseModel):
    file_path: str
    bucket_name: str = "documents"
    document_id: Optional[str] = None
    user_id: Optional[str] = None  # used to fetch user rules

class TransactionRow(BaseModel):
    date: Optional[str] = None
    description: Optional[str] = None
    amount: Optional[Any] = None
    currency: Optional[str] = None
    category: Optional[str] = None        # legacy: equals category_main
    category_main: Optional[str] = None
    category_sub: Optional[str] = None
    type: Optional[str] = None
    document_id: Optional[str] = None

class ConfirmTransactionsRequest(BaseModel):
    file_path: Optional[str] = None
    document_id: Optional[str] = None
    user_id: Optional[str] = None
    transactions: List[TransactionRow]

class CategoryFeedbackItem(BaseModel):
    pattern: str
    category_main: str
    category_sub: Optional[str] = None
    weight: Optional[float] = 1.0

class CategoryFeedbackRequest(BaseModel):
    user_id: Optional[str] = None
    patterns: List[CategoryFeedbackItem]

@app.get("/health")
def health():
    return {"ok": True, "service": "budgy-document-processor", "version": SERVICE_VERSION}

@app.options("/{rest_of_path:path}")
def preflight(rest_of_path: str):
    return PlainTextResponse("", status_code=204)

def _with_user_rules(transactions: List[Dict[str, Any]], user_id: Optional[str]) -> List[Dict[str, Any]]:
    """
    Re-run categorization overlay using user rules only when the extractor didn't produce one (or produced 'Misc/Other').
    NOTE: The extractor itself already tries to use rules if provided; this is just a safety net.
    """
    return transactions  # extractor handles user rules when provided; keep for future adjustments

@app.post("/process-pdf")
async def process_pdf(
    request: Request,
    file: UploadFile = File(...),
    authorization: Optional[str] = Header(default=None),
):
    try:
        # Resolve user to fetch rules
        user_id = None
        if authorization and authorization.startswith("Bearer "):
            user_id = get_user_id_from_bearer(authorization.replace("Bearer ", "").strip())
        user_rules = fetch_user_rules(user_id) if user_id else []

        pdf_bytes = await file.read()
        transactions, meta = extract_transactions(pdf_bytes, user_rules=user_rules)

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
        return JSONResponse(status_code=500, content={"success": False, "error": "PROCESS_PDF_FAILED", "details": str(e)})

@app.post("/process-document")
async def process_document(body: ProcessDocumentRequest):
    try:
        user_rules = fetch_user_rules(body.user_id) if body.user_id else []
        pdf_bytes = download_file_from_supabase(body.file_path, bucket=body.bucket_name)
        if not pdf_bytes:
            return JSONResponse(status_code=404, content={"success": False, "error": "FILE_NOT_FOUND", "file_path": body.file_path})

        transactions, meta = extract_transactions(pdf_bytes, user_rules=user_rules)
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
        return JSONResponse(status_code=500, content={"success": False, "error": "PROCESS_DOCUMENT_FAILED", "details": str(e)})

@app.post("/confirm-transactions")
async def confirm_transactions(
    body: ConfirmTransactionsRequest,
    authorization: Optional[str] = Header(default=None),
):
    try:
        resolved_user_id = body.user_id
        if not resolved_user_id and authorization and authorization.startswith("Bearer "):
            token = authorization.replace("Bearer ", "").strip()
            resolved_user_id = get_user_id_from_bearer(token)
        if not resolved_user_id:
            return JSONResponse(status_code=401, content={"success": False, "error": "UNAUTHORIZED", "details": "User id not resolved"})

        inserted = save_transactions_to_db(
            [t.model_dump() for t in body.transactions],
            file_path=(body.file_path or ""),
            user_id=resolved_user_id,
            document_id=body.document_id,
        )
        return {"success": True, "inserted_count": inserted, "user_id": resolved_user_id}
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "error": "CONFIRM_TRANSACTIONS_FAILED", "details": str(e)})

@app.post("/category-feedback")
async def category_feedback(
    body: CategoryFeedbackRequest,
    authorization: Optional[str] = Header(default=None),
):
    """
    App calls this after a user edits categories in Review or later views.
    We upsert simple pattern rules so future extractions auto-categorize correctly.
    """
    try:
        user_id = body.user_id
        if not user_id and authorization and authorization.startswith("Bearer "):
            user_id = get_user_id_from_bearer(authorization.replace("Bearer ", "").strip())
        if not user_id:
            return JSONResponse(status_code=401, content={"success": False, "error": "UNAUTHORIZED", "details": "User id not resolved"})

        count = upsert_user_rules(user_id, [p.model_dump() for p in body.patterns])
        return {"success": True, "upserted": count}
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "error": "CATEGORY_FEEDBACK_FAILED", "details": str(e)})
