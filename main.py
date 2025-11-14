import os
import time
import json
import logging
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, UploadFile, File, Form, Header, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field

from pdf_extractor import extract_transactions

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("budgy-document-processor")

app = FastAPI(
    title="Budgy Document Processor",
    description="External microservice for PDF → transactions extraction",
)

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "POST, GET, OPTIONS",
    "Access-Control-Allow-Headers": (
        "Authorization, X-Auth-Token, Content-Type, X-Requested-With, "
        "X-Client-Info, ApiKey, Origin, Accept"
    ),
    "Access-Control-Max-Age": "86400",
}


# --- Middleware: append CORS to responses ----------------------------------

@app.middleware("http")
async def add_cors_headers(request: Request, call_next):
    response = await call_next(request)
    path = request.url.path
    if path in ("/process-pdf", "/health"):
        for k, v in CORS_HEADERS.items():
            response.headers[k] = v
    return response


# --- Models ----------------------------------------------------------------

class ProcessingResponse(BaseModel):
    success: bool
    message: Optional[str] = None
    document_id: Optional[str] = None
    transactions: Optional[List[Dict[str, Any]]] = None
    transaction_count: int = 0
    extraction_method: str = Field(default="automatic")
    extraction_quality: Optional[str] = None
    processing_time_ms: Optional[int] = None


# --- Health ----------------------------------------------------------------

@app.get("/health", response_model=Dict[str, Any])
def health():
    return {
        "ok": True,
        "service": "budgy-document-processor",
        "version": os.getenv("SERVICE_VERSION", "v1.0.0"),
    }


# --- PDF Processor ---------------------------------------------------------

@app.post("/process-pdf", response_model=ProcessingResponse)
async def process_pdf(
    file: UploadFile = File(...),
    document_id: str = Form(...),
    metadata: Optional[str] = Form(None),
    x_auth_token: Optional[str] = Header(None),
):
    """
    Main endpoint called by Supabase Edge functions.

    Request (multipart/form-data):
    - file: PDF file (required)
    - document_id: string (required)
    - metadata: JSON string with optional:
        - currency
        - bankId
        - accountId
        - cardId

    Response (200):
    {
      "success": true/false,
      "document_id": "...",
      "transactions": [...],
      "transaction_count": N,
      "extraction_method": "automatic",
      "extraction_quality": "high|low",
      "processing_time_ms": 1234,
      "message": "..."
    }
    """
    start_time = time.time()
    temp_file_path: Optional[str] = None

    try:
        logger.info(f"Received /process-pdf for document_id={document_id}")
        logger.info(f"File: {file.filename}, content_type={file.content_type}")

        if not file.filename.lower().endswith(".pdf"):
            logger.warning(f"Invalid file type: {file.filename}")
            return JSONResponse(
                status_code=400,
                content={
                    "success": False,
                    "document_id": document_id,
                    "transactions": [],
                    "transaction_count": 0,
                    "extraction_method": "automatic",
                    "extraction_quality": "low",
                    "processing_time_ms": int((time.time() - start_time) * 1000),
                    "message": "Only PDF files are supported",
                },
                headers=CORS_HEADERS,
            )

        # Parse metadata JSON if provided
        metadata_dict: Dict[str, Any] = {}
        if metadata:
            try:
                metadata_dict = json.loads(metadata)
            except json.JSONDecodeError:
                logger.warning(f"Invalid metadata JSON: {metadata}")
                metadata_dict = {}

        # Save to a temp file for pdfplumber / PyMuPDF
        temp_dir = os.getenv("TEMP_DIR", "/tmp")
        os.makedirs(temp_dir, exist_ok=True)
        temp_file_path = os.path.join(temp_dir, f"upload_{document_id}.pdf")

        contents = await file.read()
        with open(temp_file_path, "wb") as f_out:
            f_out.write(contents)
        logger.info(f"Saved temporary PDF to {temp_file_path} ({len(contents)} bytes)")

        # Extract transactions
        transactions = extract_transactions(temp_file_path, metadata_dict)
        num_transactions = len(transactions)

        processing_time = int((time.time() - start_time) * 1000)
        quality = "high" if num_transactions > 0 else "low"

        logger.info(
            f"Extraction completed for document_id={document_id}: "
            f"{num_transactions} transactions in {processing_time} ms"
        )

        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "document_id": document_id,
                "transactions": transactions,
                "transaction_count": num_transactions,
                "extraction_method": "automatic",
                "extraction_quality": quality,
                "processing_time_ms": processing_time,
                "message": f"Successfully extracted {num_transactions} transactions"
                if num_transactions
                else "Document processed but no transactions could be extracted",
            },
            headers=CORS_HEADERS,
        )

    except Exception as exc:
        processing_time = int((time.time() - start_time) * 1000)
        logger.exception(f"Error while processing document_id={document_id}: {exc}")

        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "document_id": document_id,
                "transactions": [],
                "transaction_count": 0,
                "extraction_method": "automatic",
                "extraction_quality": "low",
                "processing_time_ms": processing_time,
                "message": f"Failed to process document: {str(exc)}",
            },
            headers=CORS_HEADERS,
        )

    finally:
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.remove(temp_file_path)
            except Exception as e:
                logger.warning(f"Failed to remove temp file {temp_file_path}: {e}")


# --- Root route (optional) -------------------------------------------------

@app.get("/", response_class=PlainTextResponse)
def root():
    return "Budgy Document Processor – see /health and POST /process-pdf"


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
