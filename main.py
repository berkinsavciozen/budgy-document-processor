import os
import base64
import json
import logging
from typing import Optional
from fastapi import FastAPI, File, UploadFile, Form, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Budgy Document Processor")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict this to specific domains
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class DocumentRequest(BaseModel):
    file_bytes: Optional[str] = None
    file_path: str
    bucket_name: Optional[str] = "documents"
    document_id: str

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "document-processor"}

@app.post("/process-document")
async def process_document(request: DocumentRequest):
    """Process a document and extract transactions"""
    try:
        logger.info(f"Processing document: {request.document_id}")
        
        # Here you would implement the actual document processing
        # This is a placeholder that returns sample data
        # You'll replace this with your real document processing logic
        
        # Sample extracted transactions
        sample_transactions = [
            {
                "date": "2025-01-15",
                "description": "Grocery Store Purchase",
                "amount": "-75.20",
                "category": "Groceries",
                "confidence": 0.95
            },
            {
                "date": "2025-01-18",
                "description": "Monthly Salary",
                "amount": "3500.00",
                "category": "Income",
                "confidence": 0.98
            }
        ]
        
        return {
            "success": True,
            "document_id": request.document_id,
            "file_path": request.file_path,
            "extraction_method": "text-extraction",
            "candidate_transactions": sample_transactions,
            "transaction_count": len(sample_transactions)
        }
    except Exception as e:
        logger.error(f"Error processing document: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    # For local development
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
