import os
import time
import requests
import logging
import json

def initialize_documents_bucket(bucket_name="documents", max_attempts=3, base_delay=1):
    """
    Initialize the documents bucket via Supabase's .init endpoint, with exponential backoff.
    
    :param bucket_name: The name of the bucket.
    :param max_attempts: Maximum retry attempts.
    :param base_delay: Base delay in seconds.
    :return: True on success, False otherwise.
    """
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")
    if not supabase_url or not supabase_key:
        logging.error("SUPABASE_URL or SUPABASE_KEY not set.")
        return False

    init_url = f"{supabase_url}/storage/v1/object/{bucket_name}/.init"
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}"
    }
    attempt = 0
    while attempt < max_attempts:
        try:
            logging.debug(f"Bucket Init Attempt {attempt+1} for {init_url}")
            response = requests.post(init_url, headers=headers, timeout=10)
            logging.debug(f"Response: {response.status_code} - {response.text}")
            if response.status_code == 200:
                logging.info("Bucket initialized successfully.")
                return True
            else:
                raise Exception(f"Status code: {response.status_code}")
        except Exception as e:
            attempt += 1
            delay = base_delay * (2 ** (attempt - 1))
            logging.warning(f"Attempt {attempt} failed: {e}. Retrying in {delay} seconds...")
            time.sleep(delay)
    logging.error("Failed to initialize bucket after multiple attempts.")
    return False

def update_document_record(document_id, status, transactions):
    """
    Update the document record in Supabase with a new status and attach extracted transaction data.
    
    :param document_id: The ID of the document record.
    :param status: New status string (e.g., "completed" or "error").
    :param transactions: List of extracted transactions.
    :return: True if updated successfully, False otherwise.
    """
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_service_key = os.getenv("SUPABASE_SERVICE_KEY")
    if not supabase_url or not supabase_service_key:
        logging.error("SUPABASE_URL or SUPABASE_SERVICE_KEY not set.")
        return False

    url = f"{supabase_url}/rest/v1/documents?id=eq.{document_id}"
    headers = {
        "apikey": supabase_service_key,
        "Authorization": f"Bearer {supabase_service_key}",
        "Content-Type": "application/json",
        "Prefer": "return=representation"
    }
    payload = {
        "status": status,
        "extracted_data": json.dumps(transactions)
    }
    try:
        response = requests.patch(url, headers=headers, json=payload, timeout=10)
        if response.status_code in [200, 201]:
            logging.info(f"Document {document_id} updated to status '{status}'.")
            return True
        else:
            logging.error(f"Update failed for document {document_id}: {response.status_code} {response.text}")
            return False
    except Exception as e:
        logging.exception(f"Exception while updating document {document_id}: {e}")
        return False
