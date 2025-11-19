import io
import logging
import os
from typing import Any, Dict, List, Optional

from fastapi import Depends, FastAPI, File, Header, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, validator

from pdf_extractor import extract_transactions_from_pdf
from categorizer import categorize
from supabase_utils import (
    download_file_from_supabase,
    get_user_id_from_bearer,
    save_transactions_to_db,
)

logger = logging.getLogger("budgy-document-processor")
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="Budgi Document Processor", version="1.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)


# ---------- MODELS ----------

class TransactionRow(BaseModel):
    date: str  # ISO: YYYY-MM-DD
    description: str
    amount: float
    currency: str = "TRY"
    # FIX: Changed 'regex' to 'pattern' for Pydantic V2 compatibility
    type: str = Field(..., pattern="^(income|expense)$") 
    category_main: Optional[str] = "Miscellaneous"
    category_sub: Optional[str] = "Unplanned Purchases"
    source: Optional[str] = "credit_card_statement"

    # Meta fields for Budgi AI
    # Keep as Optional[str] for cleaner input parsing
    bank_id: Optional[str] = None
    account_id: Optional[str] = None
    card_id: Optional[str] = None
    document_id: Optional[str] = None
    file_path: Optional[str] = None
    user_profile_id: Optional[str] = None

    @validator("date")
    def validate_date(cls, v: str) -> str:
        if len(v) != 10 or v[4] != "-" or v[7] != "-":
            raise ValueError("date must be YYYY-MM-DD")
        return v


class ProcessDocumentRequest(BaseModel):
    file_path: str
    document_id: Optional[str] = None
    bank_id: Optional[str] = None
    account_id: Optional[str] = None
    card_id: Optional[str] = None
    user_profile_id: Optional[str] = None


class ProcessedDocumentResponse(BaseModel):
    transactions: List[TransactionRow]
    processor_version: str = "1.1.0"
    file_path: Optional[str] = None
    document_id: Optional[str] = None


class ConfirmTransactionsRequest(BaseModel):
    transactions: List[TransactionRow]
    file_path: Optional[str] = None
    document_id: Optional[str] = None
    user_profile_id: Optional[str] = None


# ---------- HELPERS ----------

# FIX: Helper function to convert None to "" for downstream safety
def _safe_str(val: Optional[str]) -> str:
    """Converts None to empty string to prevent downstream JS/TS errors from 'null' JSON values."""
    return val if val is not None else ""

def _extract_and_enrich(
    pdf_bytes: bytes,
    file_path: Optional[str] = None,
    meta: Optional[Dict[str, Optional[str]]] = None,
) -> List[TransactionRow]:
    """
    Extracts, Categorizes, and Enriches.
    """
    meta = meta or {}
    
    # 1. Extract raw data
    try:
        raw_rows: List[Dict[str, Any]] = extract_transactions_from_pdf(pdf_bytes)
    except Exception as exc:
        logger.exception("Failed to extract transactions from PDF")
        raise HTTPException(status_code=500, detail=f"PDF extraction error: {exc}")

    transactions: List[TransactionRow] = []
    
    for r in raw_rows:
        try:
            # 2. Auto-Categorize based on Description and Amount
            cat_main, cat_sub = categorize(r["description"], r["amount"])
            
            # 3. Create Model
            # APPLY FIX: Use _safe_str for all meta fields
            tx = TransactionRow(
                date=r["date"],
                description=r["description"],
                amount=r["amount"], 
                currency=r.get("currency", "TRY"),
                type=r["type"], 
                category_main=cat_main,
                category_sub=cat_sub,
                source=r.get("source", "credit_card_statement"),
                
                bank_id=_safe_str(meta.get("bank_id")),
                account_id=_safe_str(meta.get("account_id")),
                card_id=_safe_str(meta.get("card_id")),
                document_id=_safe_str(meta.get("document_id")),
                file_path=_safe_str(file_path),
                user_profile_id=_safe_str(meta.get("user_profile_id")),
            )
            transactions.append(tx)
        except Exception as e:
            logger.warning("Skipping malformed extracted row %s: %s", r, e)

    return transactions


# ---------- ROUTES ----------

@app.get("/")
async def root() -> Dict[str, str]:
    return {"service": "budgy-document-processor", "status": "ok"}

@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok"}

@app.post("/process-pdf", response_model=ProcessedDocumentResponse)
async def process_pdf(
    request: Request,
    file: UploadFile = File(...),
    bank_id: Optional[str] = None,
    account_id: Optional[str] = None,
    card_id: Optional[str] = None,
    document_id: Optional[str] = None,
    user_profile_id: Optional[str] = None,
) -> ProcessedDocumentResponse:
    try:
        contents = await file.read()
        logger.info("Received PDF upload '%s' (%d bytes)", file.filename, len(contents))
    except Exception as exc:
        logger.exception("Error reading uploaded file")
        raise HTTPException(status_code=400, detail=f"Failed to read file: {exc}")

    meta = {
        "bank_id": bank_id,
        "account_id": account_id,
        "card_id": card_id,
        "document_id": document_id,
        "user_profile_id": user_profile_id,
    }

    transactions = _extract_and_enrich(
        contents, file_path=file.filename, meta=meta
    )

    return ProcessedDocumentResponse(
        transactions=transactions,
        processor_version="1.1.0",
        file_path=_safe_str(file.filename),
        document_id=_safe_str(document_id),
    )

@app.post("/process-document", response_model=ProcessedDocumentResponse)
async def process_document(
    request: Request,
    body: ProcessDocumentRequest,
) -> ProcessedDocumentResponse:
    logger.info("Processing document from file_path=%s", body.file_path)

    pdf_bytes = download_file_from_supabase(body.file_path)
    if pdf_bytes is None:
        raise HTTPException(
            status_code=404,
            detail=f"Could not download PDF from Supabase at '{body.file_path}'",
        )

    meta = {
        "bank_id": body.bank_id,
        "account_id": body.account_id,
        "card_id": body.card_id,
        "document_id": body.document_id,
        "user_profile_id": body.user_profile_id,
    }

    transactions = _extract_and_enrich(
        pdf_bytes, file_path=body.file_path, meta=meta
    )

    return ProcessedDocumentResponse(
        transactions=transactions,
        processor_version="1.1.0",
        file_path=_safe_str(body.file_path),
        document_id=_safe_str(body.document_id),
    )

@app.post("/confirm-transactions")
async def confirm_transactions(
    request: Request,
    body: ConfirmTransactionsRequest,
    authorization: Optional[str] = Header(None),
):
    logger.info(
        "Confirming %d transactions for file_path=%s",
        len(body.transactions),
        body.file_path,
    )

    user_id = get_user_id_from_bearer(authorization)
    if not user_id:
        logger.warning("User ID could not be resolved from Authorization header")
        raise HTTPException(status_code=401, detail="Unauthorized: invalid or missing token")

    transactions_payload: List[Dict[str, Any]] = []
    for tx in body.transactions:
        # tx.dict() will now contain string defaults if fields were None
        tx_dict = tx.dict()
        tx_dict["user_id"] = user_id
        tx_dict["file_path"] = _safe_str(body.file_path)
        # Ensure IDs are passed to DB
        tx_dict["document_id"] = _safe_str(body.document_id) or tx_dict.get("document_id")
        tx_dict["user_profile_id"] = _safe_str(body.user_profile_id) or tx_dict.get("user_profile_id")
        transactions_payload.append(tx_dict)

    try:
        inserted_count = save_transactions_to_db(transactions_payload)
    except Exception as exc:
        logger.exception("Error while saving transactions to Supabase")
        raise HTTPException(status_code=500, detail=f"Failed to save transactions: {exc}")

    return JSONResponse(
        {
            "status": "ok",
            "inserted": inserted_count,
            "file_path": _safe_str(body.file_path),
            "document_id": _safe_str(body.document_id),
        }
    )
