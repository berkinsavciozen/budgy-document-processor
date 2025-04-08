import os
import base64
import json
import logging
import time
from typing import Optional, Dict, List, Any, Union
from datetime import datetime
from fastapi import FastAPI, HTTPException, Body, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from supabase import create_client, Client
import pdfplumber
import pdf2image
import pytesseract
from PIL import Image
import io
import numpy as np
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("budgy-processor")

app = FastAPI(title="Budgy Document Processor")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class Transaction(BaseModel):
    date: str
    Description: str
    amount: float

# Initialize Supabase client
def get_supabase_client() -> Client:
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not supabase_url or not supabase_key:
        raise RuntimeError("Supabase credentials are missing in environment variables.")
    return create_client(supabase_url, supabase_key)

supabase = get_supabase_client()

@app.post("/extract", response_model=List[Transaction])
async def extract_transactions(file: UploadFile = File(...)):
    if not file.filename.endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only PDF files are supported")

    contents = await file.read()
    transactions = []

    try:
        with pdfplumber.open(io.BytesIO(contents)) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    for row in table:
                        try:
                            date, desc, amount = row[:3]
                            date = date.strip()
                            desc = desc.strip()
                            amount = float(str(amount).replace(",", ".").replace("TL", "").strip())
                            transaction = {
                                "date": date,
                                "Description": desc,
                                "amount": amount
                            }
                            transactions.append(transaction)
                        except Exception as e:
                            logger.warning(f"Skipping row {row} due to error: {e}")

    except Exception as e:
        logger.warning(f"pdfplumber failed, falling back to OCR: {e}")
        try:
            images = pdf2image.convert_from_bytes(contents)
            for img in images:
                text = pytesseract.image_to_string(img)
                for line in text.splitlines():
                    parts = line.strip().split()
                    if len(parts) >= 3:
                        try:
                            date = parts[0]
                            amount = float(parts[-1].replace(",", ".").replace("TL", "").strip())
                            desc = " ".join(parts[1:-1])
                            transaction = {
                                "date": date,
                                "Description": desc,
                                "amount": amount
                            }
                            transactions.append(transaction)
                        except Exception as e:
                            logger.warning(f"Skipping OCR line due to error: {e}")
        except Exception as e:
            logger.error(f"Failed to process PDF via OCR: {e}")
            raise HTTPException(status_code=500, detail="Failed to extract transactions from PDF")

    # Deduplicate transactions by custom key
    unique_transactions = []
    seen_keys = set()

    for transaction in transactions:
        date = transaction.get('date', '')
        amount = transaction.get('amount', '')
        description = transaction.get('Description', '')[:10].lower()
        comparison_key = f"{date}_{amount}_{description}"

        if comparison_key not in seen_keys:
            seen_keys.add(comparison_key)
            unique_transactions.append(transaction)

    # Upload to Supabase
    try:
        data, count = supabase.table("transactions").insert(unique_transactions).execute()
        logger.info(f"Inserted {len(unique_transactions)} transactions into Supabase.")
    except Exception as e:
        logger.error(f"Failed to upload to Supabase: {e}")
        raise HTTPException(status_code=500, detail="Upload to Supabase failed")

    return unique_transactions

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
