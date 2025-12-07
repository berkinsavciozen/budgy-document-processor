import base64
import json
import logging
import os
from typing import Any, Dict, List

import fitz  # PyMuPDF
from openai import OpenAI

logger = logging.getLogger("budgy-document-processor.llm_extractor")

# System prompt for consistent, bank-agnostic extraction
SYSTEM_PROMPT = """
You are a bank statement transaction extraction engine.

Your task:
- Read the bank or credit card statement shown in the images.
- Extract ONLY real transaction rows (no summaries, no totals, no limits).
- Return a JSON object with a single key "transactions" whose value is a list of transactions.

Each transaction MUST have this schema:
{
  "date": "YYYY-MM-DD",
  "description": "string",
  "amount": number,              // POSITIVE = money going out (expense/charge)
                                 // NEGATIVE = money coming in (income/refund/payment)
  "currency": "TRY",
  "source": "credit_card" | "current_account"
}

Rules:
- Include all pages of the statement.
- Ignore summary values such as previous balance, total debt, total payments, limits, interest summaries.
- For CREDIT CARDS:
    - Purchases, interest, and fees -> positive amount (expense).
    - Card payments and refunds that REDUCE card debt -> negative amount (income).
- For BANK ACCOUNTS:
    - Outgoing transfers, card charges, fees -> positive amount (expense).
    - Incoming salary, transfers, refunds -> negative amount (income).
- Normalize date to ISO format YYYY-MM-DD (e.g., 02/11/2024 -> 2024-11-02).
- Normalize Turkish numbers like "1.234,56" to 1234.56.
- If currency is not visible, default to "TRY".
- Use "credit_card" as source for card statements, "current_account" for account statements.
- If you are not sure whether a row is a transaction, SKIP IT.
- Do NOT return any additional keys besides "transactions".
"""

def _pdf_to_base64_images(pdf_bytes: bytes, max_pages: int | None = None, dpi: int = 200) -> List[str]:
    """
    Render each page of the PDF to a PNG image and return as base64-encoded strings.
    """
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    images_b64: List[str] = []

    for page_index, page in enumerate(doc):
        if max_pages is not None and page_index >= max_pages:
            break
        pix = page.get_pixmap(dpi=dpi)
        img_bytes = pix.tobytes("png")
        images_b64.append(base64.b64encode(img_bytes).decode("ascii"))

    logger.info(f"Rendered {len(images_b64)} page(s) to images for LLM extraction")
    return images_b64


def _call_llm_for_transactions(images_b64: List[str]) -> List[Dict[str, Any]]:
    """
    Call OpenAI vision-enabled chat model with the rendered images
    and return the parsed transactions list from the JSON response.
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError(
            "OPENAI_API_KEY environment variable is required for LLM-based PDF parsing"
        )

    # Use any vision-enabled chat model you prefer (e.g. gpt-4o)
    model_name = os.getenv("OPENAI_VISION_MODEL", "gpt-4o")
    client = OpenAI(api_key=api_key)

    # Build multimodal content: one text instruction + all pages as images
    content: List[Dict[str, Any]] = [
        {
            "type": "text",
            "text": (
                "Extract all bank/credit card transactions in JSON format as specified. "
                "Return ONLY a JSON object with a 'transactions' array."
            ),
        }
    ]

    for b64 in images_b64:
        content.append(
            {
                "type": "image_url",
                "image_url": {
                    "url": f"data:image/png;base64,{b64}",
                    "detail": "low",
                },
            }
        )

    logger.info(f"Sending {len(images_b64)} image(s) to LLM model {model_name}")

    response = client.chat.completions.create(
        model=model_name,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": content},
        ],
        max_tokens=4096,
        response_format={"type": "json_object"},
    )

    message = response.choices[0].message
    if not message or not message.content:
        raise RuntimeError("Empty response from LLM when extracting transactions")

    try:
        data = json.loads(message.content)
    except json.JSONDecodeError as exc:
        logger.error("Failed to decode LLM JSON response", exc_info=exc)
        raise RuntimeError(f"Invalid JSON from LLM: {message.content[:200]}") from exc

    txs = data.get("transactions")
    if not isinstance(txs, list):
        raise RuntimeError("LLM response JSON must contain a 'transactions' list")

    logger.info(f"LLM returned {len(txs)} raw transaction(s)")
    return txs


def _normalize_amount(raw_amount: Any) -> float:
    """
    Normalize amount from the LLM to a float.
    Accepts numbers or strings like '1.234,56' or '- 1.234,56'.
    """
    if isinstance(raw_amount, (int, float)):
        return float(raw_amount)

    if isinstance(raw_amount, str):
        s = raw_amount.strip()
        # Remove currency symbols and spaces
        s = s.replace("TL", "").replace("₺", "").replace("TRY", "").strip()
        # Handle spaces before minus: "- 1.234,56"
        s = s.replace(" ", "")
        # Replace thousand separators and decimal comma
        s = s.replace(".", "").replace(",", ".")
        try:
            return float(s)
        except ValueError:
            logger.warning(f"Could not parse amount from string: {raw_amount!r}")
            raise

    raise TypeError(f"Unsupported amount type from LLM: {type(raw_amount)}")


def _normalize_date(raw_date: Any) -> str:
    """
    Expect the LLM to output ISO dates 'YYYY-MM-DD'.
    If not, just return the string and let higher layers validate or reject.
    """
    if raw_date is None:
        raise ValueError("Missing date")

    s = str(raw_date).strip()
    # Very light sanity check
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        return s

    # Fallback: return as-is, frontend or later logic may handle
    logger.warning(f"Non-ISO date received from LLM, returning as-is: {s!r}")
    return s


def extract_transactions_from_pdf_llm(pdf_bytes: bytes) -> List[Dict[str, Any]]:
    """
    Public entrypoint used by main.py:
    - Render PDF pages to images
    - Call vision-enabled LLM
    - Normalize to Budgi transaction schema used by TransactionRow in main.py
    """
    max_pages_env = os.getenv("LLM_PARSER_MAX_PAGES")
    max_pages = None
    if max_pages_env:
        try:
            max_pages = int(max_pages_env)
        except ValueError:
            logger.warning(
                "Invalid LLM_PARSER_MAX_PAGES value %r, ignoring", max_pages_env
            )

    images_b64 = _pdf_to_base64_images(pdf_bytes, max_pages=max_pages)
    if not images_b64:
        logger.warning("No pages rendered from PDF, returning empty transaction list")
        return []

    raw_txs = _call_llm_for_transactions(images_b64)

    normalized: List[Dict[str, Any]] = []
    for idx, tx in enumerate(raw_txs):
        try:
            date = _normalize_date(tx.get("date"))
            description = str(tx.get("description", "")).strip()
            if not description:
                logger.debug("Skipping transaction %d due to empty description", idx)
                continue

            amount = _normalize_amount(tx.get("amount"))
            # Map sign to Budgi type: positive => expense, negative => income
            tx_type = "expense" if amount >= 0 else "income"

            currency = str(tx.get("currency") or "TRY").strip() or "TRY"
            source = str(tx.get("source") or "bank_statement").strip()

            normalized.append(
                {
                    "date": date,
                    "description": description,
                    "amount": amount,
                    "currency": currency,
                    "type": tx_type,
                    "source": source,
                }
            )
        except Exception as exc:
            logger.warning(
                "Skipping LLM transaction at index %d due to error: %s", idx, exc
            )

    logger.info("Normalized %d transaction(s) from LLM output", len(normalized))
    return normalized
