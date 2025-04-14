import re
import pdfplumber
from datetime import datetime
import logging

# Configure logging to capture debug information
logging.basicConfig(level=logging.DEBUG)

def extract_transactions(pdf_path: str):
    transactions = []
    # Regex to capture an optional numeric index followed by a date in dd/mm/yyyy
    # e.g., "1 02/07/2024" or "02/07/2024"
    date_regex = re.compile(r'^(?:\d+\s+)?(\d{2}/\d{2}/\d{4})')
    # Regex for amount: if the last token ends with "TL"
    amount_pattern = re.compile(r'([\d\.,]+\s?TL)$')
    
    processing_lines = False
    all_page_snippets = []

    try:
        with pdfplumber.open(pdf_path) as pdf:
            num_pages = len(pdf.pages)
            logging.debug(f"Opened PDF '{pdf_path}' with {num_pages} page(s).")
            for i, page in enumerate(pdf.pages):
                text = page.extract_text()
                if not text:
                    logging.debug(f"Page {i+1} has no extractable text.")
                    continue

                # Save a snippet for debugging purposes.
                snippet = text[:300].replace('\n', ' ')
                all_page_snippets.append(f"Page {i+1} snippet: {snippet}")
                logging.debug(f"Page {i+1} snippet: {snippet}")

                # Look for the table header to start processing transaction lines.
                if not processing_lines and "TARİH" in text and "AÇIKLAMA" in text and "MİKTAR" in text:
                    processing_lines = True
                    logging.debug("Found table header; starting to process transaction lines.")
                    continue  # Skip header line itself

                if not processing_lines:
                    continue

                # Process each line in the page
                for line in text.split('\n'):
                    line = line.strip()
                    if not line:
                        continue

                    # Match a date at the beginning (with optional index)
                    m = date_regex.match(line)
                    if not m:
                        continue
                    date_str = m.group(1)

                    # Split line into tokens for further processing
                    tokens = line.split()
                    try:
                        date_index = tokens.index(date_str)
                    except ValueError:
                        logging.debug(f"Date '{date_str}' not found in tokens: {tokens}")
                        continue

                    # Determine the transaction amount.
                    if tokens[-1].upper().endswith("TL"):
                        amount = tokens[-1]
                        explanation_tokens = tokens[date_index+1:-1]
                    else:
                        # Assume the last two tokens are [transaction_amount, balance]
                        if len(tokens) - (date_index+1) >= 2:
                            amount = tokens[-2]
                            explanation_tokens = tokens[date_index+1:-2]
                        else:
                            logging.debug(f"Insufficient tokens after date in line: {line}")
                            continue

                    explanation = " ".join(explanation_tokens)
                    
                    # Parse and reformat the date
                    try:
                        dt = datetime.strptime(date_str, "%d/%m/%Y")
                        formatted_date = dt.strftime("%Y/%m/%d")
                    except Exception as e:
                        logging.warning(f"Failed to parse date '{date_str}' in line: {line} | Error: {e}")
                        formatted_date = date_str  # Fallback

                    transactions.append({
                        "date": formatted_date,
                        "explanation": explanation,
                        "amount": amount,
                    })
    except Exception as e:
        logging.exception(f"Error processing PDF '{pdf_path}': {e}")

    if not transactions:
        logging.error(f"No transactions extracted from '{pdf_path}'. Page snippets: {all_page_snippets}")
    else:
        logging.debug(f"Extraction complete: {len(transactions)} transaction(s) found.")
    return transactions
