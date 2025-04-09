import re
import pdfplumber
from datetime import datetime

def extract_transactions(pdf_path: str):
    transactions = []
    # Lines must start with a date in the format dd/mm/yyyy
    date_pattern = re.compile(r'^(\d{2}/\d{2}/\d{4})')
    # Transaction amount is matched as any number ending with "TL"
    amount_pattern = re.compile(r'([\d\.,]+\s?TL)$')

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                continue
            for line in text.split('\n'):
                line = line.strip()
                if not line:
                    continue
                # Proceed only if line starts with a valid date.
                if not date_pattern.match(line):
                    continue
                amount_match = amount_pattern.search(line)
                if not amount_match:
                    continue
                amount_str = amount_match.group(1).strip()
                # Remove the amount to isolate the date and explanation.
                line_without_amount = line[:amount_match.start()].strip()
                # Split on the first whitespace to separate date from explanation.
                parts = line_without_amount.split(maxsplit=1)
                if len(parts) < 2:
                    continue
                date_str, explanation = parts
                try:
                    dt = datetime.strptime(date_str, "%d/%m/%Y")
                    formatted_date = dt.strftime("%Y/%m/%d")
                except Exception:
                    formatted_date = date_str  # Fallback if conversion fails
                transactions.append({
                    "date": formatted_date,
                    "explanation": explanation,
                    "amount": amount_str,
                })
    return transactions
