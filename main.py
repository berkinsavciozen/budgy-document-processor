import os
import time
import logging
import traceback
import json
from fastapi import FastAPI, UploadFile, File, Form, Header, HTTPException, BackgroundTasks, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field

# Import custom modules (assuming they are in the same directory)
from pdf_extractor import extract_transactions
from supabase_utils import update_document_record, get_document_details

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("budgy-document-processor")

app = FastAPI(title="Budgy Document Processor", description="API for processing financial documents")

# Configure CORS - Allow all origins for now to fix the immediate issue
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins temporarily
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS", "PUT", "DELETE"],
    allow_headers=["*"],
    expose_headers=["*"]
)

class ProcessingResponse(BaseModel):
    success: bool
    message: Optional[str] = None
    document_id: Optional[str] = None
    extraction_method: Optional[str] = None
    transactions: Optional[List[Dict[str, Any]]] = None
    transaction_count: Optional[int] = None
    processing_time_ms: Optional[int] = None
    extraction_quality: Optional[str] = None
    error: Optional[str] = None

class ConfirmTransactionsRequest(BaseModel):
    file_path: str
    transactions: List[Dict[str, Any]]

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": time.time(),
        "cors_enabled": True
    }

@app.post("/process-pdf", response_model=ProcessingResponse)
async def process_pdf(
    file: UploadFile = File(...),
    document_id: str = Form(...),
    x_auth_token: Optional[str] = Header(None)
):
    start_time = time.time()
    temp_file_path = None
    
    try:
        logger.info(f"Received document ID: {document_id}")
        logger.info(f"File: {file.filename}, size: {file.size}, content_type: {file.content_type}")
        
        if not file.filename.lower().endswith('.pdf'):
            logger.warning(f"Invalid file type: {file.filename}")
            return JSONResponse(
                status_code=400,
                content={"success": False, "error": "Only PDF files are supported"},
                headers={
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
                    "Access-Control-Allow-Headers": "*"
                }
            )
        
        temp_file_path = f"/tmp/upload_{document_id}.pdf"
        
        with open(temp_file_path, "wb") as temp_file:
            content = await file.read()
            temp_file.write(content)
            
        logger.info(f"File saved to: {temp_file_path}")
        
        transactions = extract_transactions(temp_file_path)
        num_transactions = len(transactions)
        
        processing_time = int((time.time() - start_time) * 1000)
        
        logger.info(f"Extracted {num_transactions} transactions in {processing_time}ms")
        
        update_document_record(document_id, "completed", transactions)
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "document_id": document_id,
                "transactions": transactions,
                "transaction_count": num_transactions,
                "extraction_method": "automatic",
                "extraction_quality": "high" if num_transactions > 0 else "low",
                "processing_time_ms": processing_time,
                "message": f"Successfully extracted {num_transactions} transactions"
            },
            headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
                "Access-Control-Allow-Headers": "*"
            }
        )
        
    except Exception as e:
        processing_time = int((time.time() - start_time) * 1000)
        logger.exception(f"Error processing PDF: {str(e)}")
        
        error_message = str(e)
        error_traceback = traceback.format_exc()
        logger.error(f"Traceback: {error_traceback}")
        
        if document_id:
            update_document_record(document_id, "error", [])
        
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": error_message,
                "document_id": document_id,
                "processing_time_ms": processing_time,
                "message": "Failed to process document"
            },
            headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
                "Access-Control-Allow-Headers": "*"
            }
        )
    finally:
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.unlink(temp_file_path)
                logger.debug(f"Temporary file removed in finally block: {temp_file_path}")
            except Exception as cleanup_error:
                logger.error(f"Error removing temp file in finally block: {cleanup_error}")

# Explicit OPTIONS handler for confirm-transactions
@app.options("/confirm-transactions")
async def options_confirm_transactions():
    return PlainTextResponse(
        "",
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Authorization, Content-Type, X-Requested-With, X-Client-Info, ApiKey, Origin, Accept",
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Max-Age": "86400"
        }
    )

@app.post("/confirm-transactions")
async def confirm_transactions(request: ConfirmTransactionsRequest):
    cors_headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Authorization, Content-Type, X-Requested-With, X-Client-Info, ApiKey, Origin, Accept",
        "Access-Control-Allow-Credentials": "true",
        "Access-Control-Max-Age": "86400"
    }
    try:
        logger.info(f"Confirming {len(request.transactions)} transactions for file: {request.file_path}")

        if not request.transactions:
            return JSONResponse(
                status_code=400,
                content={
                    "success": False,
                    "error": "No transactions provided"
                },
                headers=cors_headers
            )

        for i, tx in enumerate(request.transactions):
            logger.info(f"Transaction {i+1}: {tx.get('date')} - {tx.get('description')} - {tx.get('amount')}")

        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "message": f"Successfully confirmed {len(request.transactions)} transactions",
                "transaction_count": len(request.transactions)
            },
            headers=cors_headers
        )

    except Exception as e:
        logger.exception(f"Error confirming transactions: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": str(e),
                "message": "Failed to confirm transactions"
            },
            headers=cors_headers
        )

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
