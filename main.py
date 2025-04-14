from fastapi import FastAPI, File, UploadFile, HTTPException, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import tempfile
import shutil
import logging
from pdf_extractor import extract_transactions
from supabase_utils import initialize_documents_bucket, update_document_record

app = FastAPI()

logging.basicConfig(level=logging.DEBUG)

# Allow CORS – update origins as required.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

@app.post("/process-pdf")
async def process_pdf(
    file: UploadFile = File(...), 
    document_id: str = Query(None, description="Optional document record ID to update")
):
    if file.content_type != "application/pdf":
        raise HTTPException(status_code=400, detail="Uploaded file must be a PDF.")
    
    # Initialize storage bucket before processing
    if not initialize_documents_bucket():
        logging.error("Bucket initialization failed. Continuing without it.")
        # Optionally: raise an error if bucket initialization is critical

    try:
        # Save the uploaded PDF to a temporary file.
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            shutil.copyfileobj(file.file, tmp)
            tmp_path = tmp.name
            logging.debug(f"Temporary PDF saved to: {tmp_path}")

        # Extract transactions from the PDF.
        transactions = extract_transactions(tmp_path)
        logging.info(f"Extracted {len(transactions)} transactions.")

        # Optionally, if a document ID is provided, update that record in Supabase.
        if document_id:
            success = update_document_record(document_id, "completed", transactions)
            if not success:
                logging.error(f"Failed to update document record {document_id}.")
        
        return JSONResponse(content={"transactions": transactions})
    except Exception as e:
        logging.exception("Error processing PDF:")
        raise HTTPException(status_code=500, detail=str(e))
