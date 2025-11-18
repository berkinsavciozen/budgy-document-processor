import io
import logging
import os
from typing import Any, Dict, List, Optional

from fastapi import Depends, FastAPI, File, Header, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, validator

from pdf_extractor import extract_transactions_from_pdf
from supabase_utils import (
    download_file_from_supabase,
    get_user_id_from_bearer,
    save_transactions_to_db,
)

logger = logging.getLogger("budgy-document-processor")
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="Budgi Document Processor", version="1.0.0")

# --- CORS (loose, can be tightened in prod) ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)


# ---------- MODELS ----------


class TransactionRow(BaseModel):
    """
    Transaction row model used both for:
    - output of /process-pdf and /process-document
    - input of /confirm-transactions
    """

    date: str  # ISO: YYYY-MM-DD
    description: str
    amount: float
    currency: str = "TRY"
    type: str = Field(..., regex="^(income|expense)$")
    category_main: Optional[str] = None
    category_sub: Optional[str] = None
    source: Optional[str] = "credit_card_statement"

    # Optional meta for later DB usage
    bank_id: Optional[str] = None
    account_id: Optional[str] = None
    card_id: Optional[str] = None
    document_id: Optional[str] = None
    file_path: Optional[str] = None
    user_profile_id: Optional[str] = None

    @validator("date")
    def validate_date(cls, v: str) -> str:
        # Very light guard; frontend / DB can enforce stricter rules
        if len(v) != 10 or v[4] != "-" or v[7] != "-":
            raise ValueError("date must be YYYY-MM-DD")
        return v


class ProcessDocumentRequest(BaseModel):
    """
    JSON-based processing request used by Budgi backend.
    file_path is typically a Supabase storage path like 'statements/xxx.pdf'
    """

    file_path: str
    document_id: Optional[str] = None
    bank_id: Optional[str] = None
    account_id: Optional[str] = None
    card_id: Optional[str] = None
    user_profile_id: Optional[str] = None


class ProcessedDocumentResponse(BaseModel):
    """
    Response for /process-pdf and /process-document.
    """

    transactions: List[TransactionRow]
    processor_version: str = "1.0.0"
    file_path: Optional[str] = None
    document_id: Optional[str] = None


class ConfirmTransactionsRequest(BaseModel):
    """
    Body for /confirm-transactions.
    Frontend sends back the (possibly edited) list of transactions.
    """

    transactions: List[TransactionRow]
    file_path: Optional[str] = None
    document_id: Optional[str] = None
    user_profile_id: Optional[str] = None


# ---------- HELPERS ----------


def _extract_transactions_from_bytes(
    pdf_bytes: bytes,
    file_path: Optional[str] = None,
    meta: Optional[Dict[str, Optional[str]]] = None,
) -> List[TransactionRow]:
    """
    Core glue between raw PDF bytes and our TransactionRow model.
    Uses pdf_extractor.extract_transactions_from_pdf() and enriches
    the results with meta fields (file_path, bank_id, etc.).
    """
    meta = meta or {}
    try:
        raw_rows: List[Dict[str, Any]] = extract_transactions_from_pdf(pdf_bytes)
    except Exception as exc:
        logger.exception("Failed to extract transactions from PDF")
        raise HTTPException(status_code=500, detail=f"PDF extraction error: {exc}")

    transactions: List[TransactionRow] = []
    for r in raw_rows:
        try:
            tx = TransactionRow(
                date=r["date"],
                description=r["description"],
                amount=r["amount"],
                currency=r.get("currency", "TRY"),
                type=r["type"],
                category_main=r.get("category_main"),
                category_sub=r.get("category_sub"),
                source=r.get("source", "credit_card_statement"),
                bank_id=meta.get("bank_id"),
                account_id=meta.get("account_id"),
                card_id=meta.get("card_id"),
                document_id=meta.get("document_id"),
                file_path=file_path,
                user_profile_id=meta.get("user_profile_id"),
            )
            transactions.append(tx)
        except Exception as e:
            logger.warning("Skipping malformed extracted row %s: %s", r, e)

    # Sort newest first (what you already show on the UI)
    transactions.sort(key=lambda t: t.date, reverse=True)
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
    """
    Direct file-upload endpoint.
    Useful for local debugging or if frontend ever posts files directly.
    """
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

    transactions = _extract_transactions_from_bytes(
        contents, file_path=file.filename, meta=meta
    )

    return ProcessedDocumentResponse(
        transactions=transactions,
        processor_version="1.0.0",
        file_path=file.filename,
        document_id=document_id,
    )


@app.post("/process-document", response_model=ProcessedDocumentResponse)
async def process_document(
    request: Request,
    body: ProcessDocumentRequest,
) -> ProcessedDocumentResponse:
    """
    JSON endpoint used by Budgi backend:
    - body.file_path is a Supabase Storage path or full URL
    - we download the PDF and extract transactions
    """
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

    transactions = _extract_transactions_from_bytes(
        pdf_bytes, file_path=body.file_path, meta=meta
    )

    return ProcessedDocumentResponse(
        transactions=transactions,
        processor_version="1.0.0",
        file_path=body.file_path,
        document_id=body.document_id,
    )


@app.post("/confirm-transactions")
async def confirm_transactions(
    request: Request,
    body: ConfirmTransactionsRequest,
    authorization: Optional[str] = Header(None),
):
    """
    Final step in your current flow:
    - Frontend shows the extracted transactions and lets user edit them
    - Then calls this endpoint to persist everything in Supabase
    """
    logger.info(
        "Confirming %d transactions for file_path=%s",
        len(body.transactions),
        body.file_path,
    )

    user_id = get_user_id_from_bearer(authorization)
    if not user_id:
        logger.warning("User ID could not be resolved from Authorization header")
        raise HTTPException(status_code=401, detail="Unauthorized: invalid or missing token")

    # Convert Pydantic objects to raw dicts and enrich with user_id and file_path
    transactions_payload: List[Dict[str, Any]] = []
    for tx in body.transactions:
        tx_dict = tx.dict()
        tx_dict["user_id"] = user_id
        tx_dict["file_path"] = body.file_path
        tx_dict["document_id"] = body.document_id or tx_dict.get("document_id")
        tx_dict["user_profile_id"] = body.user_profile_id or tx_dict.get(
            "user_profile_id"
        )
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
            "file_path": body.file_path,
            "document_id": body.document_id,
        }
    )
