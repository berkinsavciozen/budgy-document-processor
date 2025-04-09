
import os
import time
import requests
import logging
import json

logger = logging.getLogger("budgy-document-processor.supabase")

def initialize_documents_bucket(bucket_name="documents", max_attempts=3, base_delay=1):
    """
    Attempts to initialize the documents bucket by calling Supabase's .init endpoint.
    Retries with exponential backoff if there is a failure (e.g., 504 timeout).

    :param bucket_name: Name of the Supabase storage bucket.
    :param max_attempts: Maximum number of retry attempts.
    :param base_delay: Base delay in seconds for exponential backoff.
    :return: True if initialization succeeded; otherwise, False.
    """
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_KEY")  # Changed from SUPABASE_KEY to match render.yaml
    
    if not supabase_url or not supabase_key:
        logger.error("SUPABASE_URL or SUPABASE_SERVICE_KEY environment variables are not set.")
        return False

    logger.info(f"Initializing bucket: {bucket_name} at {supabase_url}")
    init_url = f"{supabase_url}/storage/v1/bucket/{bucket_name}"
    headers = {
         "apikey": supabase_key,
         "Authorization": f"Bearer {supabase_key}",
         "Content-Type": "application/json"
    }
    
    # First check if bucket exists
    attempt = 0
    while attempt < max_attempts:
        try:
            logger.debug(f"Attempt {attempt + 1}: Checking if bucket {bucket_name} exists")
            response = requests.get(init_url, headers=headers, timeout=10)
            logger.debug(f"Bucket check response: {response.status_code}")
            
            if response.status_code == 200:
                logger.info(f"Bucket {bucket_name} already exists")
                return True
            elif response.status_code == 404:
                # Bucket doesn't exist, try to create it
                logger.info(f"Bucket {bucket_name} doesn't exist, creating it...")
                create_response = requests.post(
                    f"{supabase_url}/storage/v1/buckets", 
                    headers=headers,
                    json={"name": bucket_name, "public": True},
                    timeout=10
                )
                if create_response.status_code in (200, 201):
                    logger.info(f"Successfully created bucket {bucket_name}")
                    return True
                else:
                    logger.error(f"Failed to create bucket: {create_response.status_code} - {create_response.text}")
            else:
                logger.warning(f"Unexpected status when checking bucket: {response.status_code}")
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
        
        attempt += 1
        delay = base_delay * (2 ** (attempt - 1))
        if attempt < max_attempts:
            logger.debug(f"Retrying in {delay} seconds...")
            time.sleep(delay)
    
    logger.error("All attempts to initialize the documents bucket failed")
    return False

def update_document_record(document_id, status, processed_data):
    """
    Update a document record in the Supabase documents table with the given status and processed data.
    
    :param document_id: The ID of the document record to update.
    :param status: The new status ('processed' or 'error').
    :param processed_data: The extracted data (can be a dictionary or list).
    :return: True if the update succeeded; False otherwise.
    """
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_KEY")
    
    if not supabase_url or not supabase_key:
        logger.error("SUPABASE_URL or SUPABASE_SERVICE_KEY environment variables are not set.")
        return False

    url = f"{supabase_url}/rest/v1/documents?id=eq.{document_id}"
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "return=representation"
    }
    
    # Use processed_data field instead of extracted_data to match your schema
    payload = {
        "status": status,
        "processed_data": processed_data,  # This matches your DB schema
        "updated_at": "now()"  # Update the timestamp
    }
    
    try:
        logger.debug(f"Updating document {document_id} with status {status}")
        response = requests.patch(url, headers=headers, json=payload, timeout=15)
        
        if response.status_code in (200, 201, 204):
            logger.info(f"Successfully updated document record {document_id} to status {status}")
            return True
        else:
            logger.error(f"Failed to update document record {document_id}: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        logger.exception(f"Exception when updating document record {document_id}: {str(e)}")
        return False
</lov-write>
