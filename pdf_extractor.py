# File: models.py
from enum import Enum, auto
from dataclasses import dataclass

class PdfType(Enum):
    CREDIT_CARD_ISBANK = auto()
    CREDIT_CARD_QNB = auto()
    CREDIT_CARD_GARANTI = auto()
    ACCOUNT_YAPIKREDI = auto()
    UNKNOWN = auto()

@dataclass
class Transaction:
    date: str
    description: str
    amount: float
    balance: float = None

# File: detect_pdf_type.py
from models import PdfType

def detect_pdf_type(text: str) -> PdfType:
    if "Maximum Visa Dijital" in text:
        return PdfType.CREDIT_CARD_ISBANK
    if "Ekstre tarihi" in text and "QNB Bank" in text:
        return PdfType.CREDIT_CARD_QNB
    if "T. Garanti Bankası" in text or "Bonus Trink" in text:
        return PdfType.CREDIT_CARD_GARANTI
    if "Hesap Numarası" in text and "IBAN:" in text:
        return PdfType.ACCOUNT_YAPIKREDI
    return PdfType.UNKNOWN

# File: parsers/isbank_parser.py
import re
from models import Transaction

class IsbankParser:
    def extract_transactions(self, text: str):
        txns = []
        for line in text.splitlines():
            m = re.search(r"(\d{2}/\d{2}/\d{4})\s+(.+?)\s+(-?[\d.,]+)\s+(-?[\d.,]+)", line)
            if m:
                date, desc, amt, bal = m.groups()
                amt = float(amt.replace('.', '').replace(',', '.'))
                bal = float(bal.replace('.', '').replace(',', '.'))
                txns.append(Transaction(date, desc.strip(), amt, bal))
        return txns

# File: parsers/qnb_parser.py
import re
from models import Transaction

class QnbParser:
    def extract_transactions(self, text: str):
        txns = []
        for line in text.splitlines():
            m = re.search(r"(\d{2}/\d{2}/\d{4})\s+(.+?)\s+(-?[\d.,]+)", line)
            if m:
                date, desc, amt = m.groups()
                amt = float(amt.replace('.', '').replace(',', '.'))
                txns.append(Transaction(date, desc.strip(), amt, None))
        return txns

# File: parsers/garanti_parser.py
import re
from models import Transaction

class GarantiParser:
    def extract_transactions(self, text: str):
        txns = []
        for line in text.splitlines():
            m = re.search(r"(\d{2} \w+ \d{4})\s+(.+?)\s+(-?[\d.,]+)", line)
            if m:
                date, desc, amt = m.groups()
                # parse date e.g. '10 Kasım 2024' into DD/MM/YYYY if needed
                amt = float(amt.replace('.', '').replace(',', '.'))
                txns.append(Transaction(date, desc.strip(), amt, None))
        return txns

# File: parsers/yapikredi_parser.py
import re
from models import Transaction

class YapiKrediParser:
    def extract_transactions(self, text: str):
        txns = []
        for line in text.splitlines():
            m = re.search(r"(\d{2}/\d{2}/\d{4})\s+(.+?)\s+(-?[\d.,]+)\s+(-?[\d.,]+)", line)
            if m:
                date, desc, amt, bal = m.groups()
                amt = float(amt.replace('.', '').replace(',', '.'))
                bal = float(bal.replace('.', '').replace(',', '.'))
                txns.append(Transaction(date, desc.strip(), amt, bal))
        return txns

# File: parsers/generic_parser.py
import re
from models import Transaction

class GenericParser:
    def extract_transactions(self, text: str):
        txns = []
        for line in text.splitlines():
            m = re.search(r"(\d{2}/\d{2}/\d{4})\s+(.+?)\s+(-?[\d.,]+)", line)
            if m:
                date, desc, amt = m.groups()
                amt = float(amt.replace('.', '').replace(',', '.'))
                txns.append(Transaction(date, desc.strip(), amt, None))
        return txns

# File: extract_pipeline.py
from pdfminer.high_level import extract_text
from detect_pdf_type import detect_pdf_type
from models import PdfType
from parsers.isbank_parser import IsbankParser
from parsers.qnb_parser import QnbParser
from parsers.garanti_parser import GarantiParser
from parsers.yapikredi_parser import YapiKrediParser

PARSERS = {
    PdfType.CREDIT_CARD_ISBANK: IsbankParser(),
    PdfType.CREDIT_CARD_QNB:     QnbParser(),
    PdfType.CREDIT_CARD_GARANTI: GarantiParser(),
    PdfType.ACCOUNT_YAPIKREDI:   YapiKrediParser(),
}

def extract_from_pdf(pdf_path: str):
    text = extract_text(pdf_path)
    pdf_type = detect_pdf_type(text)
    parser = PARSERS.get(pdf_type)
    if not parser:
        from parsers.generic_parser import GenericParser
        parser = GenericParser()
    return parser.extract_transactions(text)
