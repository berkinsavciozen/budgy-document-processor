```python
# unified parser by PDF layout type, not by bank
import re
from io import BytesIO
import pdfplumber

# generic table extraction by detecting transaction blocks
def extract_transactions_from_layout(lines, date_pattern):
    txns = []
    buffer = []
    for line in lines:
        # accumulate lines until next date
        parts = re.split(r"\s{2,}", line.strip())
        if re.match(date_pattern, parts[0]):
            if buffer:
                txns.append(buffer)
            buffer = [parts]
        else:
            if buffer:
                buffer.append(parts)
    if buffer:
        txns.append(buffer)
    # flatten and normalize
    parsed = []
    for group in txns:
        date = group[0][0]
        desc = ' '.join([g[1] for g in group if len(g)>1])
        amt = group[0][-2] if len(group[0])>2 else ''
        bal = group[0][-1] if len(group[0])>3 else ''
        parsed.append({'date':date, 'description':desc, 'amount':amt, 'balance':bal})
    return parsed

# detect layout type
LAYOUTS = [
    {
        'name':'creditcard',
        'marker':'Hesap Özeti',
        'date_pattern':r"\d{2}/\d{2}/\d{4}",
    },
    {
        'name':'account',
        'marker':'Hesap Numarası',
        'date_pattern':r"\d{2}/\d{2}/\d{4}",
    }
]

def parse_pdf(pdf_bytes):
    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        text = pdf.pages[0].extract_text()
        for layout in LAYOUTS:
            if layout['marker'] in text:
                # extract all lines after header
                lines = []
                for page in pdf.pages:
                    txt = page.extract_text().splitlines()
                    lines += txt
                # find start of transactions table
                start = next((i for i,l in enumerate(lines) if re.match(layout['date_pattern'], l)), None)
                if start is None:
                    raise ValueError('No transactions found')
                tx_lines = lines[start:]
                return extract_transactions_from_layout(tx_lines, layout['date_pattern'])
    raise ValueError('Unknown PDF type')

# entry point
def parse_credit_card(path):
    with open(path, 'rb') as f:
        data = f.read()
    return parse_pdf(data)

if __name__ == '__main__':
    import sys
    path = sys.argv[1]
    for txn in parse_credit_card(path):
        print(txn)
```
