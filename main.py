
from fastapi import FastAPI, File, UploadFile, HTTPException, Query, Header, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import tempfile
import shutil
import os
import logging
import time
from typing import Optional, List, Dict, Any
from pdf_extractor import extract_transactions
from supabase_utils import initialize_documents_bucket, update_document_record, get_document_details

# Configure logging
logging.basicConfig(
    level=logging.DEBUG if os.getenv("DEBUG_MODE", "false").lower() == "true" else logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("budgy-document-processor")

app = FastAPI(
    title="Budgy Document Processor",
    description="PDF document processing API for extracting financial transactions",
    version="1.0.0",
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Customize this for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Define allowed file types
ALLOWED_MIME_TYPES = ["application/pdf"]
MAX_FILE_SIZE_MB = int(os.getenv("MAX_FILE_SIZE", "50"))  # 50MB default

@app.on_event("startup")
async def startup_event():
    """Initialize resources when the app starts"""
    logger.info("Starting Budgy Document Processor service")
    initialize_documents_bucket()

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "ok", "timestamp": time.time()}

def process_document_task(temp_file_path: str, document_id: Optional[str] = None):
    """Background task to process a document
    
    Args:
        temp_file_path: Path to the temporary file
        document_id: ID of the document record (optional)
    """
    try:
        logger.info(f"Processing document in background task: {document_id}")
        
        # Extract transactions from the PDF
        transactions = extract_transactions(temp_file_path)
        num_transactions = len(transactions)
        
        logger.info(f"Extracted {num_transactions} transaction(s) from document {document_id}")
        
        # Update document status based on extraction results
        if document_id:
            status = "completed" if num_transactions > 0 else "error"
            update_successful = update_document_record(document_id, status, transactions)
            
            if update_successful:
                logger.info(f"Document {document_id} updated with status: {status}")
            else:
                logger.error(f"Failed to update document record {document_id}")
    except Exception as e:
        logger.exception(f"Error in background processing task: {str(e)}")
        
        # Update document with error status
        if document_id:
            update_document_record(document_id, "error", [])
    finally:
        # Clean up the temporary file
        if os.path.exists(temp_file_path):
            os.unlink(temp_file_path)
            logger.debug(f"Temporary file removed: {temp_file_path}")

@app.post("/process-pdf")
async def process_pdf(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    document_id: Optional[str] = Query(None, description="Optional document record ID"),
    x_auth_token: Optional[str] = Header(None, description="Authentication token")
):
    """Process a PDF file and extract transactions
    
    Args:
        file: The uploaded PDF file
        document_id: ID of the document record in Supabase
        x_auth_token: Authentication token
        
    Returns:
        JSON response with status and message
    """
    # Validate file type
    if file.content_type not in ALLOWED_MIME_TYPES:
        raise HTTPException(status_code=400, detail=f"File must be a PDF. Got: {file.content_type}")
    
    # Check document ID if provided
    if document_id:
        # Update document status to processing
        update_document_record(document_id, "processing", [])
    
    try:
        # Save uploaded file to temporary location
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            shutil.copyfileobj(file.file, tmp)
            tmp_path = tmp.name
            logger.debug(f"Saved temporary PDF file to: {tmp_path}")
        
        # Process document in background task
        background_tasks.add_task(process_document_task, tmp_path, document_id)
        
        return JSONResponse(content={
            "status": "processing",
            "message": "Document processing started",
            "document_id": document_id
        })
    
    except Exception as e:
        logger.exception("Error during PDF processing:")
        
        # Update document with error status
        if document_id:
            update_document_record(document_id, "error", [])
            
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/document/{document_id}/status")
async def get_document_status(document_id: str):
    """Get the processing status of a document
    
    Args:
        document_id: ID of the document to check
        
    Returns:
        JSON response with document status
    """
    document = get_document_details(document_id)
    
    if not document:
        raise HTTPException(status_code=404, detail=f"Document {document_id} not found")
        
    return JSONResponse(content={
        "document_id": document_id,
        "status": document.get("status", "unknown"),
        "updated_at": document.get("updated_at")
    })

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
