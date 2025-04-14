import re
import pdfplumber
from datetime import datetime
import logging

# Configure logging to capture debug information
logging.basicConfig(level=logging.DEBUG)

def extract_transactions(pdf_path: str):
    transactions = []
    # Regex that accepts an optional index at beginning, then captures a date in dd/mm/yyyy format.
    date_regex = re.compile(r'^(?:\d+\s+)?(\d{2}/\d{2}/\d{4})')
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            num_pages = len(pdf.pages)
            logging.debug(f"Opened PDF '{pdf_path}' with {num_pages} page(s).")
            for i, page in enumerate(pdf.pages):
                text = page.extract_text()
                if not text:
                    logging.debug(f"Page {i+1} has no extractable text.")
                    continue

                snippet = text[:300].replace('\n', ' ')
                logging.debug(f"Page {i+1} snippet: {snippet}")
                
                for line in text.split('\n'):
                    line = line.strip()
                    if not line:
                        continue

                    # Try to extract the date using the regex with optional index.
                    m = date_regex.match(line)
                    if not m:
                        continue
                    date_str = m.group(1)
                    
                    # Split the line into tokens.
                    tokens = line.split()
                    
                    # Find the index of the token that exactly matches the date.
                    try:
                        date_index = tokens.index(date_str)
                    except ValueError:
                        logging.debug(f"Date '{date_str}' not found in tokens: {tokens}")
                        continue
                    
                    # Now determine the transaction amount.
                    amount = None
                    explanation = ""
                    
                    # Case 1: If the last token ends with "TL" (as in QNB PDF)
                    if tokens[-1].upper().endswith("TL"):
                        amount = tokens[-1]
                        # Explanation is tokens between date and the amount
                        explanation_tokens = tokens[date_index+1:-1]
                    else:
                        # Case 2: Assume the last two tokens are [transaction_amount, balance]
                        if len(tokens) - (date_index+1) >= 2:
                            amount = tokens[-2]
                            explanation_tokens = tokens[date_index+1:-2]
                        else:
                            logging.debug(f"Insufficient tokens after date for line: {line}")
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
        logging.error(f"No transactions extracted from '{pdf_path}'.")
    else:
        logging.debug(f"Extraction complete: {len(transactions)} transaction(s) found.")
    
    return transactions
