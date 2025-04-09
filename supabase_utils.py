import os
import time
import requests
import logging

def initialize_documents_bucket(bucket_name="documents", max_attempts=3, base_delay=1):
    """
    Attempts to initialize the documents bucket by calling Supabase's .init endpoint.
    Retries with exponential backoff if there is a failure (e.g., 504 timeout).

    :param bucket_name: Name of the Supabase storage bucket.
    :param max_attempts: Maximum number of retry attempts.
    :param base_delay: Base delay in seconds for exponential backoff.
    :return: True if initialization succeeded; otherwise, False.
    """
    supabase_url = os.getenv("SUPABASE_URL")  # e.g. https://njjfycredoojnauidutp.supabase.co
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
