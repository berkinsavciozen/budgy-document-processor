import io
import re
from typing import List, Dict

import pdfplumber
import pytesseract
from pdf2image import convert_from_path


def extract_text(pdf_path: str) -> str:
    """
    Extracts text from a PDF by rendering each page to an image
    and running OCR, falling back to pdfplumber text extraction
    where possible.
    """
    text_chunks = []

    # First, try pdfplumber’s built-in text extraction
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text_chunks.append(page_text)

    # Next, for any pages that look empty, run OCR
    images = convert_from_path(pdf_path, dpi=300)
    for i, img in enumerate(images):
        # simple heuristic: if the corresponding text_chunk is very short, OCR
        if i >= len(text_chunks) or len(text_chunks[i].strip()) < 50:
            ocr_text = pytesseract.image_to_string(img, lang="tur")
            text_chunks.append(ocr_text)

    return "\n".join(text_chunks)


def parse_transactions(full_text: str) -> List[Dict[str, str]]:
    """
    Parses out transaction lines of the form:
      DD/MM/YYYY  Description  Amount  Balance
    You’ll need to tweak the regex to match your exact PDF format.
    """
    transactions = []
    # Example regex; adjust as needed for your statement layout:
    line_re = re.compile(
        r"(\d{2}/\d{2}/\d{4})\s+(.+?)\s+(-?\d{1,3}(?:\.\d{3})*,\d{2})\s+(-?\d{1,3}(?:\.\d{3})*,\d{2})"
    )

    for line in full_text.splitlines():
        m = line_re.search(line)
        if m:
            date, desc, amount, balance = m.groups()
            transactions.append({
                "date": date,
                "description": desc.strip(),
                "amount": amount.strip(),
                "balance": balance.strip(),
            })
    return transactions


# ------------------------------------------------------------------------------
def extract_transactions(pdf_path: str) -> List[Dict[str, str]]:
    """
    High-level entry point for the FastAPI app:
    given a PDF file path, return the list of parsed transactions.
    """
    text = extract_text(pdf_path)
    return parse_transactions(text)


__all__ = [
    "extract_text",
    "parse_transactions",
    "extract_transactions",
]
