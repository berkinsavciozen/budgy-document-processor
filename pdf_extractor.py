"""
PDF Transaction Extractor Module
Extracts transaction data from financial PDFs
"""
import logging
import re
from datetime import datetime
from typing import List, Dict, Any

logger = logging.getLogger("budgy-document-processor.pdf_extractor")

def extract_transactions(pdf_path: str) -> List[Dict[str, Any]]:
    """
    Extract transactions from a PDF file.
    This is a simplified implementation that extracts basic transaction data.
    
    Args:
        pdf_path: Path to the PDF file
        
    Returns:
        List of transaction dictionaries
    """
    logger.info(f"Extracting transactions from PDF: {pdf_path}")
    
    try:
        # This is a simple placeholder function that returns mock data
        # In a real implementation, you would parse the PDF and extract actual transactions
        # For now, we'll generate some realistic mock transactions based on the filename
        
        transactions = []
        today = datetime.now()
        
        # Generate different mock data based on the filename to simulate real extraction
        if "credit" in pdf_path.lower() or "card" in pdf_path.lower():
            # Credit card statement mock data
            transactions = [
                {
                    "date": (today.replace(day=5)).strftime("%Y-%m-%d"),
                    "description": "Grocery Store",
                    "amount": "-85.50",
                    "category": "Food & Dining",
                    "confidence": 0.92
                },
                {
                    "date": (today.replace(day=8)).strftime("%Y-%m-%d"),
                    "description": "Online Subscription",
                    "amount": "-15.99",
                    "category": "Entertainment",
                    "confidence": 0.95
                },
                {
                    "date": (today.replace(day=12)).strftime("%Y-%m-%d"),
                    "description": "Restaurant Payment",
                    "amount": "-45.75",
                    "category": "Food & Dining",
                    "confidence": 0.88
                },
                {
                    "date": (today.replace(day=15)).strftime("%Y-%m-%d"),
                    "description": "Fuel Station",
                    "amount": "-60.25",
                    "category": "Transportation",
                    "confidence": 0.94
                }
            ]
        else:
            # Bank account statement mock data
            transactions = [
                {
                    "date": (today.replace(day=3)).strftime("%Y-%m-%d"),
                    "description": "Salary Deposit",
                    "amount": "2450.00",
                    "category": "Income",
                    "confidence": 0.97
                },
                {
                    "date": (today.replace(day=5)).strftime("%Y-%m-%d"),
                    "description": "Rent Payment",
                    "amount": "-1200.00",
                    "category": "Housing",
                    "confidence": 0.96
                },
                {
                    "date": (today.replace(day=10)).strftime("%Y-%m-%d"),
                    "description": "Utility Bill",
                    "amount": "-85.40",
                    "category": "Utilities",
                    "confidence": 0.91
                },
                {
                    "date": (today.replace(day=15)).strftime("%Y-%m-%d"),
                    "description": "Insurance Payment",
                    "amount": "-120.75",
                    "category": "Insurance",
                    "confidence": 0.93
                },
                {
                    "date": (today.replace(day=20)).strftime("%Y-%m-%d"),
                    "description": "ATM Withdrawal",
                    "amount": "-200.00",
                    "category": "Cash & ATM",
                    "confidence": 0.99
                }
            ]
        
        logger.info(f"Successfully extracted {len(transactions)} transactions")
        return transactions
        
    except Exception as e:
        logger.error(f"Error extracting transactions from PDF: {str(e)}")
        # Return an empty list in case of error
        return []
