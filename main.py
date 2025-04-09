from fastapi import FastAPI, File, UploadFile, HTTPException, Query, Header
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import tempfile
import shutil
import logging
import os
from typing import Optional
from pdf_extractor import extract_transactions
from supabase_utils import initialize_documents_bucket, update_document_record

app = FastAPI()

# Configure logging (adjust level as needed)
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("budgy-document-processor")

# Enable CORS; adjust allowed origins as needed for production
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
async def health_check():
    """Health check endpoint required by Render deployment."""
    return {"status": "healthy", "timestamp": import time; time.time()}

@app.post("/process-pdf")
async def process_pdf(
    file: UploadFile = File(...), 
    document_id: str = Query(None, description="Optional document ID to update record"),
    authorization: Optional[str] = Header(None)
):
    logger.info(f"Received process-pdf request for document_id: {document_id}")
    
    if file.content_type != "application/pdf":
        logger.warning(f"Invalid content type: {file.content_type}")
        raise HTTPException(status_code=400, detail="File must be a PDF")

    # Extract token from Authorization header if present
    token = None
    if authorization and authorization.startswith("Bearer "):
        token = authorization.replace("Bearer ", "")
        logger.debug("Authorization token received")

    # Optionally, ensure the documents bucket is initialized before processing the file.
    bucket_initialized = initialize_documents_bucket()
    if not bucket_initialized:
        logger.error("Failed to initialize documents bucket")
        # We'll continue processing even if bucket initialization fails,
        # as we want to at least extract the transactions

    try:
        # Save the uploaded PDF to a temporary file.
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            shutil.copyfileobj(file.file, tmp)
            tmp_path = tmp.name
            logger.debug(f"Saved temporary PDF file to: {tmp_path}")

        # Process the PDF to extract transactions.
        transactions = extract_transactions(tmp_path)
        logger.info(f"Extracted {len(transactions)} transactions from the PDF.")

        # Transform transactions to match expected frontend structure
        processed_transactions = []
        for tx in transactions:
            processed_tx = {
                "date": tx["date"],
                "description": tx["explanation"],
                "amount": tx["amount"].replace("TL", "").strip(),
                "category": "Other",  # Default category
                "confidence": 0.85    # Default confidence score
            }
            processed_transactions.append(processed_tx)

        # If a document_id was provided, update the document record in Supabase.
        update_success = False
        if document_id:
            logger.info(f"Updating document record {document_id} with {len(processed_transactions)} transactions")
            
            # Prepare processed data with proper format
            processed_data = {
                "success": True,
                "extraction_method": "pdf_extractor",
                "document_type": "bank_statement",
                "candidate_transactions": processed_transactions
            }
            
            update_success = update_document_record(document_id, "processed", processed_data)
            if not update_success:
                logger.error(f"Failed to update document record {document_id} after extraction")
        
        # Return the extracted transaction data promptly.
        return JSONResponse(content={
            "success": True,
            "transaction_count": len(processed_transactions),
            "processing_time": 0,  # Placeholder
            "document_id": document_id,
            "update_success": update_success,
            "extraction_method": "pdf_extractor",
            "document_type": "bank_statement",
            "candidate_transactions": processed_transactions
        })
    except Exception as e:
        logger.exception(f"Error during PDF processing: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        # Clean up the temporary file if it exists
        try:
            if 'tmp_path' in locals() and os.path.exists(tmp_path):
                os.unlink(tmp_path)
                logger.debug(f"Deleted temporary file: {tmp_path}")
        except Exception as e:
            logger.error(f"Error deleting temporary file: {str(e)}")
