
import os
import time
import requests
import logging
import json
from typing import Dict, List, Any, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("budgy-document-processor")

def initialize_documents_bucket(bucket_name="documents", max_attempts=3, base_delay=1) -> bool:
    """Initialize a Supabase storage bucket for documents
    
    Args:
        bucket_name: Name of the bucket to initialize
        max_attempts: Maximum number of retry attempts
        base_delay: Base delay between retries (exponential backoff)
        
    Returns:
        bool: True if successful, False otherwise
    """
    # Get Supabase credentials
    supabase_url = os.getenv("SUPABASE_URL", "https://njjfycredoojnauidutp.supabase.co")
    supabase_key = os.getenv("SUPABASE_SERVICE_KEY", 
                            os.getenv("SUPABASE_KEY", 
                                     "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im5qamZ5Y3JlZG9vam5hdWlkdXRwIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTczOTUxOTY1NSwiZXhwIjoyMDU1MDk1NjU1fQ.7-emsS37XbwTj9vQMDH1lMDk1NjJH_fQ-8szb8d6Yoo"))
    
    if not supabase_url or not supabase_key:
        logger.error("Supabase credentials not found in environment variables")
        return False

    logger.info(f"Initializing storage bucket: {bucket_name}")
    init_url = f"{supabase_url}/storage/v1/object/{bucket_name}/.init"
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}"
    }
    
    # Try initialization with exponential backoff
    attempt = 0
    while attempt < max_attempts:
        try:
            logger.info(f"Attempt {attempt+1}: Initializing bucket '{bucket_name}'")
            response = requests.post(init_url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                logger.info(f"Successfully initialized the '{bucket_name}' bucket")
                return True
            elif response.status_code == 400 and "already exists" in response.text.lower():
                logger.info(f"Bucket '{bucket_name}' already exists")
                return True
            else:
                logger.warning(f"Unexpected status: {response.status_code} - {response.text}")
                
        except Exception as e:
            logger.error(f"Initialization error: {str(e)}")
            
        # Implement exponential backoff
        attempt += 1
        if attempt < max_attempts:
            delay = base_delay * (2 ** (attempt - 1))
            logger.info(f"Retrying in {delay} seconds (attempt {attempt+1}/{max_attempts})...")
            time.sleep(delay)
    
    logger.error(f"Failed to initialize bucket '{bucket_name}' after {max_attempts} attempts")
    return False

def update_document_record(document_id: str, status: str, transactions: List[Dict[str, Any]]) -> bool:
    """Update a document record in the Supabase database
    
    Args:
        document_id: ID of the document to update
        status: New status value (completed, error, etc.)
        transactions: List of extracted transactions
        
    Returns:
        bool: True if successful, False otherwise
    """
    # Get Supabase credentials
    supabase_url = os.getenv("SUPABASE_URL", "https://njjfycredoojnauidutp.supabase.co")
    supabase_service_key = os.getenv("SUPABASE_SERVICE_KEY", 
                                    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im5qamZ5Y3JlZG9vam5hdWlkdXRwIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTczOTUxOTY1NSwiZXhwIjoyMDU1MDk1NjU1fQ.7-emsS37XbwTj9vQMDH1lMDk1NjJH_fQ-8szb8d6Yoo")
    
    if not supabase_url or not supabase_service_key:
        logger.error("Supabase credentials not found in environment variables")
        return False
    
    if not document_id:
        logger.error("Missing document ID for update")
        return False

    logger.info(f"Updating document record: {document_id} with status: {status}")
    
    # Prepare the API request
    url = f"{supabase_url}/rest/v1/documents"
    headers = {
        "apikey": supabase_service_key,
        "Authorization": f"Bearer {supabase_service_key}",
        "Content-Type": "application/json",
        "Prefer": "return=representation"
    }
    
    # Prepare payload with processed data
    payload = {
        "status": status,
        "processed_data": transactions,  # Store as JSON directly
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    
    try:
        # Update the document record
        response = requests.patch(
            f"{url}?id=eq.{document_id}", 
            headers=headers, 
            json=payload, 
            timeout=15
        )
        
        if response.status_code in (200, 201, 204):
            logger.info(f"Document {document_id} updated successfully")
            return True
        else:
            logger.error(f"Failed to update document: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"Error updating document {document_id}: {str(e)}")
        return False

def get_document_details(document_id: str) -> Optional[Dict[str, Any]]:
    """Get document details from Supabase
    
    Args:
        document_id: ID of the document to fetch
        
    Returns:
        dict: Document details or None if not found
    """
    # Get Supabase credentials
    supabase_url = os.getenv("SUPABASE_URL", "https://njjfycredoojnauidutp.supabase.co")
    supabase_service_key = os.getenv("SUPABASE_SERVICE_KEY", 
                                    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im5qamZ5Y3JlZG9vam5hdWlkdXRwIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTczOTUxOTY1NSwiZXhwIjoyMDU1MDk1NjU1fQ.7-emsS37XbwTj9vQMDH1lMDk1NjJH_fQ-8szb8d6Yoo")
    
    if not supabase_url or not supabase_service_key:
        logger.error("Supabase credentials not found in environment variables")
        return None
    
    try:
        # Prepare the API request
        url = f"{supabase_url}/rest/v1/documents"
        headers = {
            "apikey": supabase_service_key,
            "Authorization": f"Bearer {supabase_service_key}"
        }
        
        # Fetch the document
        response = requests.get(
            f"{url}?id=eq.{document_id}&select=*", 
            headers=headers,
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            if data and len(data) > 0:
                return data[0]
            else:
                logger.warning(f"Document {document_id} not found")
                return None
        else:
            logger.error(f"Failed to fetch document: {response.status_code}")
            return None
            
    except Exception as e:
        logger.error(f"Error fetching document {document_id}: {str(e)}")
        return None
