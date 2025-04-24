
import os
import time
import logging
import traceback
import json
from fastapi import FastAPI, UploadFile, File, Form, Header, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field

# Import custom modules (assuming they are in the same directory)
from pdf_extractor import extract_transactions

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("budgy-document-processor")

# Initialize FastAPI app
app = FastAPI(title="Budgy Document Processor", description="API for processing financial documents")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For production, specify exact domains
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Define response model for document processing
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

# Health check endpoint
@app.get("/health")
async def health_check():
    """Simple health check endpoint to verify the service is running"""
    return {
        "status": "healthy", 
        "timestamp": time.time()
    }

def process_document_task(temp_file_path: str, document_id: str):
    """Background task to process a document
    
    Args:
        temp_file_path: Path to the temporary PDF file
        document_id: ID of the document record
    """
    try:
        logger.info(f"Processing document in background task: {document_id}")
        logger.info(f"File path: {temp_file_path}")
        
        # Check if file exists and is readable
        if not os.path.exists(temp_file_path):
            logger.error(f"Temp file not found: {temp_file_path}")
            return
        
        # Extract transactions from the PDF
        transactions = extract_transactions(temp_file_path)
        
        logger.info(f"Extraction complete: Found {len(transactions)} transactions")
        
        # Clean up the temporary file
        try:
            os.unlink(temp_file_path)
            logger.debug(f"Temporary file removed: {temp_file_path}")
        except Exception as e:
            logger.error(f"Error removing temp file: {e}")
    
    except Exception as e:
        logger.exception(f"Error in background processing task: {str(e)}")

@app.post("/process-pdf", response_model=ProcessingResponse)
async def process_pdf(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    document_id: str = Form(...),
    x_auth_token: Optional[str] = Header(None)
):
    """
    Process a PDF document to extract transaction data
    
    Args:
        file: The PDF file to process
        document_id: The ID of the document in the database
        x_auth_token: Optional authentication token
        
    Returns:
        JSON response with processing results
    """
    start_time = time.time()
    temp_file_path = None
    
    try:
        logger.info(f"Received document ID: {document_id}")
        logger.info(f"File: {file.filename}, size: {file.size}, content_type: {file.content_type}")
        
        # Validate file
        if not file.filename.lower().endswith('.pdf'):
            logger.warning(f"Invalid file type: {file.filename}")
            return JSONResponse(
                status_code=400,
                content={"success": False, "error": "Only PDF files are supported"}
            )
        
        # Create a temporary file to store the uploaded content
        temp_file_path = f"/tmp/upload_{document_id}.pdf"
        
        # Write the file to disk
        with open(temp_file_path, "wb") as temp_file:
            content = await file.read()
            temp_file.write(content)
            
        logger.info(f"File saved to: {temp_file_path}")
        
        # Extract transactions immediately (not in background)
        transactions = extract_transactions(temp_file_path)
        num_transactions = len(transactions)
        
        processing_time = int((time.time() - start_time) * 1000)  # ms
        
        logger.info(f"Extracted {num_transactions} transactions in {processing_time}ms")
        
        # Return the response with transaction data
        return {
            "success": True,
            "document_id": document_id,
            "transactions": transactions,
            "transaction_count": num_transactions,
            "extraction_method": "automatic",
            "extraction_quality": "high" if num_transactions > 0 else "low",
            "processing_time_ms": processing_time,
            "message": f"Successfully extracted {num_transactions} transactions"
        }
        
    except Exception as e:
        processing_time = int((time.time() - start_time) * 1000)
        logger.exception(f"Error processing PDF: {str(e)}")
        
        error_message = str(e)
        error_traceback = traceback.format_exc()
        logger.error(f"Traceback: {error_traceback}")
        
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": error_message,
                "document_id": document_id,
                "processing_time_ms": processing_time,
                "message": "Failed to process document"
            }
        )
    finally:
        # Clean up the temp file in case it wasn't done in the background task
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.unlink(temp_file_path)
                logger.debug(f"Temporary file removed in finally block: {temp_file_path}")
            except Exception as cleanup_error:
                logger.error(f"Error removing temp file in finally block: {cleanup_error}")

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
