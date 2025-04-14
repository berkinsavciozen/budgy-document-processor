import re
import pdfplumber
from datetime import datetime
import logging

# Ensure logging is configured to capture debug information.
logging.basicConfig(level=logging.DEBUG)

def extract_transactions(pdf_path: str):
    """
    Extracts transaction data from a PDF file.
    Each transaction consists of a date (converted to yyyy/mm/dd),
    an explanation, and the amount (with TL appended) extracted from each line.
    
    :param pdf_path: File path to the PDF.
    :return: A list of dictionaries for each transaction.
    """
    transactions = []
    # Pattern to match lines starting with dd/mm/yyyy
    date_pattern = re.compile(r'^(\d{2}/\d{2}/\d{4})')
    # Pattern to capture an amount ending with 'TL'
    amount_pattern = re.compile(r'([\d\.,]+\s?TL)$')
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            num_pages = len(pdf.pages)
            logging.debug(f"Opened PDF: {pdf_path} with {num_pages} pages.")
            for i, page in enumerate(pdf.pages):
                text = page.extract_text()
                if not text:
                    logging.debug(f"Page {i + 1} has no extractable text.")
                    continue
                # Log a snippet from the page to confirm its content.
                snippet = text[:300].replace('\n', ' ')
                logging.debug(f"Page {i + 1} snippet: {snippet}")
                lines = text.split('\n')
                for line in lines:
                    line = line.strip()
                    if not line:
                        continue
                    # Only process lines starting with a valid date
                    if not date_pattern.match(line):
                        continue
                    amt_match = amount_pattern.search(line)
                    if not amt_match:
                        logging.debug(f"Skipped line (no amount found): {line}")
                        continue
                    amount_str = amt_match.group(1).strip()
                    line_without_amount = line[:amt_match.start()].strip()
                    parts = line_without_amount.split(maxsplit=1)
                    if len(parts) < 2:
                        logging.debug(f"Skipped line (insufficient parts): {line}")
                        continue
                    date_str, explanation = parts
                    try:
                        dt = datetime.strptime(date_str, "%d/%m/%Y")
                        formatted_date = dt.strftime("%Y/%m/%d")
                    except Exception as e:
                        logging.warning(f"Failed to parse date '{date_str}' in line: {line} | Error: {e}")
                        formatted_date = date_str  # Fallback
                    transactions.append({
                        "date": formatted_date,
                        "explanation": explanation,
                        "amount": amount_str,
                    })
    except Exception as e:
        logging.exception(f"Error processing PDF '{pdf_path}': {e}")
    
    logging.debug(f"Extraction complete: {len(transactions)} transactions extracted.")
    return transactions
