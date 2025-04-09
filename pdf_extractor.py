import re
import pdfplumber
from datetime import datetime
import logging

# Ensure that the logging level is set to debug so we capture all messages
logging.basicConfig(level=logging.DEBUG)

def extract_transactions(pdf_path: str):
    transactions = []
    # Regex pattern: line must begin with dd/mm/yyyy
    date_pattern = re.compile(r'^(\d{2}/\d{2}/\d{4})')
    # Amount pattern: match a number (with periods, commas, etc.) followed by an optional space and 'TL'
    amount_pattern = re.compile(r'([\d\.,]+\s?TL)$')

    try:
        with pdfplumber.open(pdf_path) as pdf:
            num_pages = len(pdf.pages)
            logging.debug(f"Opened PDF: {pdf_path} with {num_pages} page(s).")
            for i, page in enumerate(pdf.pages):
                text = page.extract_text()
                if not text:
                    logging.debug(f"Page {i+1}/{num_pages} contains no extractable text.")
                    continue

                # Log a snippet of the text for review
                snippet = text[:300].replace('\n', ' ')
                logging.debug(f"Page {i+1} snippet: {snippet}")

                lines = text.split('\n')
                for line in lines:
                    line = line.strip()
                    if not line:
                        continue

                    # Check if the line starts with a valid date (dd/mm/yyyy)
                    if not date_pattern.match(line):
                        continue

                    amount_match = amount_pattern.search(line)
                    if not amount_match:
                        logging.debug(f"Line skipped (no amount match): {line}")
                        continue

                    amount_str = amount_match.group(1).strip()
                    # Remove the amount part from the line to isolate date and description
                    line_without_amount = line[:amount_match.start()].strip()

                    parts = line_without_amount.split(maxsplit=1)
                    if len(parts) < 2:
                        logging.debug(f"Line skipped (insufficient parts): {line}")
                        continue

                    date_str, explanation = parts
                    try:
                        dt = datetime.strptime(date_str, "%d/%m/%Y")
                        formatted_date = dt.strftime("%Y/%m/%d")
                    except Exception as e:
                        logging.warning(f"Date parsing failed for '{date_str}' in line: {line} | Error: {e}")
                        formatted_date = date_str  # Fallback to original date string

                    # Append the extracted transaction
                    transactions.append({
                        "date": formatted_date,
                        "explanation": explanation,
                        "amount": amount_str,
                    })

    except Exception as e:
        logging.exception(f"Failed to process PDF '{pdf_path}': {e}")

    logging.debug(f"Extraction complete: {len(transactions)} transaction(s) found.")
    return transactions
