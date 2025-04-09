from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import tempfile
import shutil
import logging
from pdf_extractor import extract_transactions
from supabase_utils import initialize_documents_bucket

app = FastAPI()

# Configure logging (adjust level as needed)
logging.basicConfig(level=logging.DEBUG)

# Enable CORS; adjust allowed origins for production
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/process-pdf")
async def process_pdf(file: UploadFile = File(...)):
    if file.content_type != "application/pdf":
        raise HTTPException(status_code=400, detail="File must be a PDF")

    # Optional: Ensure the documents bucket is initialized before processing the file.
    if not initialize_documents_bucket():
        logging.error("Failed to initialize documents bucket. Proceeding without bucket initialization.")
        # Depending on your flow, you might choose to raise an error here:
        # raise HTTPException(status_code=500, detail="Storage initialization failed.")

    try:
        # Save the uploaded PDF to a temporary file.
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            shutil.copyfileobj(file.file, tmp)
            tmp_path = tmp.name
            logging.debug(f"Saved temporary PDF file to: {tmp_path}")

        # Process the PDF to extract transactions.
        transactions = extract_transactions(tmp_path)
        logging.info(f"Extracted {len(transactions)} transactions from the PDF.")

        # Return the extracted transaction data promptly.
        return JSONResponse(content={"transactions": transactions})
    except Exception as e:
        logging.exception("Error during PDF processing:")
        raise HTTPException(status_code=500, detail=str(e))
