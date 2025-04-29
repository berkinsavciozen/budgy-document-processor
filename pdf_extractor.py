import re
import logging

from pdfminer.high_level import extract_text as pdf_extract_text
from pdf2image import convert_from_path
from PIL import Image
import pytesseract

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def looks_garbled(text: str) -> bool:
    """
    Check if the extracted text is likely garbled by looking for a valid date pattern.
    Returns True if no valid dates found.
    """
    return not bool(re.search(r"\b\d{2}/\d{2}/\d{4}\b", text))


def extract_text_pdf(path: str) -> str:
    """
    Extract text using PDFMiner (fast, but may produce garbled output on some PDFs).
    """
    try:
        return pdf_extract_text(path)
    except Exception as e:
        logger.warning(f"PDFMiner extraction failed: {e}")
        return ""


def extract_text_ocr(path: str) -> str:
    """
    Fallback to OCR-based extraction using Tesseract via pdf2image + PIL.
    """
    text = []
    try:
        # Convert PDF pages to images
        images = convert_from_path(path, dpi=300)
        for img in images:
            # Use Turkish language pack; adjust 'lang' if needed
            page_text = pytesseract.image_to_string(img, lang='tur', config='--psm 4')
            text.append(page_text)
    except Exception as e:
        logger.error(f"OCR extraction failed: {e}")
    return '\n'.join(text)


def extract_text(path: str) -> str:
    """
    Unified text extraction: try PDF text first; if garbled, fallback to OCR.
    """
    raw = extract_text_pdf(path)
    if looks_garbled(raw):
        logger.info("Detected garbled PDF text; falling back to OCR extraction.")
        raw = extract_text_ocr(path)
    return raw


def parse_transactions(raw_text: str) -> list:
    """
    Parse transaction lines from the raw extracted text using existing logic.
    This function assumes raw_text now contains valid Turkish date/amounts.
    """
    transactions = []
    # Example regex: matches lines like '31/10/2024  DESCRIPTION  502,00'
    pattern = re.compile(r"(?P<date>\d{2}/\d{2}/\d{4})\s+(?P<desc>.*?)\s+(?P<amount>[-+]?\d{1,3}(?:[\.\s]\d{3})*,\d{2})")
    for line in raw_text.splitlines():
        m = pattern.search(line)
        if m:
            date = m.group('date')
            desc = m.group('desc').strip()
            amt = m.group('amount').replace('.', '').replace(' ', '')
            transactions.append({
                'date': date,
                'description': desc,
                'amount': amt
            })
    return transactions


def process_document(path: str) -> list:
    """
    Main entry point: extract text and parse transactions.
    """
    raw = extract_text(path)
    transactions = parse_transactions(raw)
    if not transactions:
        logger.warning("No transactions found in document after OCR/text extraction.")
    return transactions


if __name__ == '__main__':
    import sys
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <pdf_path>")
        sys.exit(1)
    pdf_path = sys.argv[1]
    txs = process_document(pdf_path)
    for tx in txs:
        print(tx)
