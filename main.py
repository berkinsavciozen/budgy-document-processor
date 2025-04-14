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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/process-pdf")
async def process_pdf(
    file: UploadFile = File(...), 
    document_id: str = Query(None, description="Optional document record ID to update")
):
    if file.content_type != "application/pdf":
        raise HTTPException(status_code=400, detail="Uploaded file must be a PDF.")
    
    if not initialize_documents_bucket():
        logging.error("Bucket initialization failed. Proceeding anyway.")
        # Optionally, you might stop here by raising an HTTP 500 error.

    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            shutil.copyfileobj(file.file, tmp)
            tmp_path = tmp.name
            logging.debug(f"Saved temporary file: {tmp_path}")

        transactions = extract_transactions(tmp_path)
        num_transactions = len(transactions)
        logging.info(f"Extracted {num_transactions} transaction(s) from the PDF.")

        # Determine new status based on extraction result
        new_status = "completed" if num_transactions > 0 else "error"

        if document_id:
            if not update_document_record(document_id, new_status, transactions):
                logging.error(f"Failed to update document record {document_id}.")
            else:
                logging.info(f"Document record {document_id} updated with status '{new_status}'.")

        # Return the extracted transaction data
        return JSONResponse(content={"transactions": transactions})
    except Exception as e:
        logging.exception("Error during PDF processing:")
        raise HTTPException(status_code=500, detail=str(e))
