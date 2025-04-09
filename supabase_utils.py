import os
import time
import requests
import logging
import json

def initialize_documents_bucket(bucket_name="documents", max_attempts=3, base_delay=1):
    """
    Attempts to initialize the documents bucket by calling Supabase's .init endpoint.
    Retries with exponential backoff if there is a failure (e.g., 504 timeout).

    :param bucket_name: Name of the Supabase storage bucket.
    :param max_attempts: Maximum number of retry attempts.
    :param base_delay: Base delay in seconds for exponential backoff.
    :return: True if initialization succeeded; otherwise, False.
    """
    supabase_url = os.getenv("SUPABASE_URL")  # Example: https://<project-ref>.supabase.co
    supabase_key = os.getenv("SUPABASE_KEY")
    if not supabase_url or not supabase_key:
        logging.error("SUPABASE_URL or SUPABASE_KEY environment variables are not set.")
        return False

    init_url = f"{supabase_url}/storage/v1/object/{bucket_name}/.init"
    headers = {
         "apikey": supabase_key,
         "Authorization": f"Bearer {supabase_key}"
    }
    attempt = 0
    while attempt < max_attempts:
         try:
             logging.debug(f"Attempt {attempt + 1}: Calling {init_url}")
             response = requests.post(init_url, headers=headers, timeout=10)
             logging.debug(f"Attempt {attempt + 1} response: {response.status_code} - {response.text}")
             if response.status_code == 200:
                 logging.info("Successfully initialized the documents bucket.")
                 return True
             else:
                 raise Exception(f"Unexpected status code: {response.status_code}")
         except Exception as e:
             attempt += 1
             delay = base_delay * (2 ** (attempt - 1))
             logging.warning(f"Attempt {attempt} failed: {e}. Retrying in {delay} seconds...")
             time.sleep(delay)
    logging.error("All attempts to initialize the documents bucket failed.")
    return False

def update_document_record(document_id, status, transactions):
    """
    Update a document record in the Supabase documents table with the given status and extracted transaction data.
    
    :param document_id: The ID of the document record to update.
    :param status: The new status ('completed' or 'error').
    :param transactions: The extracted transaction data (as a list).
    :return: True if the update succeeded; False otherwise.
    """
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_service_key = os.getenv("SUPABASE_SERVICE_KEY")
    if not supabase_url or not supabase_service_key:
        logging.error("SUPABASE_URL or SUPABASE_SERVICE_KEY environment variables are not set.")
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
        if response.status_code in (200, 201):
            logging.info(f"Successfully updated document record {document_id} to status {status}")
            return True
        else:
            logging.error(f"Failed to update document record {document_id}: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        logging.exception(f"Exception when updating document record {document_id}:")
        return False
