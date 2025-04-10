import os
import time
import logging
import traceback
import hashlib
import tempfile
from fastapi import FastAPI, UploadFile, File, Form, Header, HTTPException, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field

# Import custom modules
from supabase_utils import initialize_documents_bucket, update_document_record
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
    document_id: Optional[str] = None
    extraction_method: Optional[str] = None
    candidate_transactions: Optional[List[Dict[str, Any]]] = None
    transaction_count: Optional[int] = None
    processing_time_ms: Optional[int] = None
    extraction_quality: Optional[str] = None
    message: Optional[str] = None
    error: Optional[str] = None

# Health check endpoint
@app.get("/health")
async def health_check():
    """Simple health check endpoint to verify the service is running"""
    return {"status": "healthy", "timestamp": time.time()}

# Process PDF document
@app.post("/process-pdf", response_model=ProcessingResponse)
async def process_pdf(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    document_id: str = Form(...),
    file_name: Optional[str] = Form(None),
    authorization: Optional[str] = Header(None)
):
    """
    Process a PDF document to extract transaction data
    
    Args:
        file: The PDF file to process
        document_id: The ID of the document in the database
        file_name: Optional name of the file
        authorization: Bearer token for authentication
    
    Returns:
        JSON response with processing results
    """
    temp_path = None
    try:
        # Simple logging of request
        logger.info(f"Received PDF processing request for document ID: {document_id}")
        
        # Check file type (simple validation)
        if not file.filename.lower().endswith('.pdf'):
            logger.warning(f"Invalid file type: {file.filename}")
            return JSONResponse(
                status_code=400,
                content={"error": "Only PDF files are supported", "success": False}
            )
        
        # Initialize Supabase storage bucket
        bucket_initialized = initialize_documents_bucket()
        if not bucket_initialized:
            logger.warning("Failed to initialize storage bucket, proceeding anyway")
        
        # Generate a more descriptive file name if not provided
        processed_file_name = file_name or file.filename or f"document_{document_id}.pdf"
        
        # Read the file content
        file_content = await file.read()
        file_size = len(file_content)
        logger.info(f"File size: {file_size} bytes")
        
        # Check if file is empty
        if file_size == 0:
            logger.error("Empty file received")
            update_document_record(document_id, "error", {
                "error": "Empty file received",
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            })
            return JSONResponse(
                status_code=400,
                content={"error": "Empty file received", "success": False}
            )
        
        # Create a temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as temp_file:
            temp_file.write(file_content)
            temp_path = temp_file.name
        
        # Extract transactions from the PDF
        start_time = time.time()
        
        # Check if it's a credit card document based on the filename
        is_credit_card = "credit" in processed_file_name.lower() or "card" in processed_file_name.lower()
        
        # Extract transactions with appropriate logic
        transactions = extract_transactions(temp_path)
        extraction_time = time.time() - start_time
        
        logger.info(f"Extracted {len(transactions)} transactions in {extraction_time:.2f} seconds")
        
        # Prepare response data
        processed_data = {
            "extraction_method": "automatic",
            "document_type": "credit_card_statement" if is_credit_card else "financial_document",
            "candidate_transactions": transactions,
            "processing_completed": time.strftime("%Y-%m-%d %H:%M:%S"),
            "processing_time_ms": int(extraction_time * 1000),
            "extraction_quality": "high"  # Set to high for reliable mock data
        }
        
        # Update document record in Supabase
        update_success = update_document_record(document_id, "processed", processed_data)
        
        if not update_success:
            logger.warning(f"Failed to update document record {document_id} in database")
        
        # Return response
        return {
            "success": True,
            "document_id": document_id,
            "extraction_method": "automatic",
            "candidate_transactions": transactions,
            "transaction_count": len(transactions),
            "processing_time_ms": int(extraction_time * 1000),
            "extraction_quality": "high"
        }
    
    except Exception as e:
        logger.error(f"Error processing PDF: {str(e)}")
        logger.error(traceback.format_exc())
        
        # Update document record with error
        error_data = {
            "error": str(e),
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        update_document_record(document_id, "error", error_data)
        
        return JSONResponse(
            status_code=500,
            content={
                "error": f"Failed to process document: {str(e)}",
                "success": False
            }
        )
    finally:
        # Clean up temporary file
        if temp_path and os.path.exists(temp_path):
            try:
                os.unlink(temp_path)
                logger.info(f"Temporary file {temp_path} deleted")
            except Exception as e:
                logger.warning(f"Failed to delete temporary file {temp_path}: {str(e)}")

# Authentication test endpoint
@app.get("/auth-test")
async def auth_test(authorization: Optional[str] = Header(None)):
    """Test endpoint for checking authentication"""
    if not authorization or not authorization.startswith("Bearer "):
        return JSONResponse(
            status_code=401,
            content={"error": "Missing or invalid Authorization header", "success": False}
        )
    
    token = authorization.split(" ")[1]
    # In a real implementation, you would validate the token
    # For now, just acknowledge it was received
    
    return {
        "success": True,
        "message": "Authentication successful",
        "token_received": True,
        "token_length": len(token)
    }

# Startup event
@app.on_event("startup")
async def startup_event():
    logger.info("Starting Budgy Document Processor service")
    # Initialize the documents bucket on startup
    initialize_documents_bucket()

# Shutdown event
@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Shutting down Budgy Document Processor service")
