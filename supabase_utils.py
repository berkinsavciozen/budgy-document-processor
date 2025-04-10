"""
Utility functions for working with Supabase storage and database
"""
import os
import time
import logging
import json
from typing import Dict, Any, Optional
from supabase import create_client, Client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("budgy-document-processor.supabase_utils")

# Get Supabase credentials from environment variables
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
DEFAULT_BUCKET_NAME = "documents"

# Initialize Supabase client
def get_supabase_client() -> Client:
    """Get or create a Supabase client"""
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.error("Missing Supabase credentials - SUPABASE_URL or SUPABASE_KEY environment variables not set")
        raise ValueError("Supabase credentials not set")
    
    try:
        # Create a new client instance
        client = create_client(SUPABASE_URL, SUPABASE_KEY)
        logger.info("Supabase client initialized successfully")
        return client
    except Exception as e:
        logger.error(f"Error initializing Supabase client: {str(e)}")
        raise

def initialize_documents_bucket(bucket_name: str = DEFAULT_BUCKET_NAME) -> bool:
    """
    Ensure the documents bucket exists
    
    Args:
        bucket_name: Name of the bucket to initialize
        
    Returns:
        bool: True if bucket was initialized or already exists, False otherwise
    """
    try:
        supabase = get_supabase_client()
        
        # List existing buckets
        logger.info(f"Checking if bucket '{bucket_name}' exists")
        buckets_response = supabase.storage.list_buckets()
        
        # Check if the response has data attribute (different versions of the client have different response formats)
        if hasattr(buckets_response, 'data'):
            buckets = buckets_response.data
        else:
            buckets = buckets_response
            
        # Check if bucket already exists
        bucket_exists = any(bucket.get('name') == bucket_name for bucket in buckets if isinstance(bucket, dict))
        
        if bucket_exists:
            logger.info(f"Bucket '{bucket_name}' already exists")
            return True
            
        # Create the bucket if it doesn't exist
        logger.info(f"Creating bucket '{bucket_name}'")
        bucket_response = supabase.storage.create_bucket(bucket_name, {'public': False, 'file_size_limit': 10485760})
        
        if bucket_response:
            logger.info(f"Bucket '{bucket_name}' created successfully")
            return True
        else:
            logger.warning(f"Failed to create bucket '{bucket_name}'")
            return False
    except Exception as e:
        logger.error(f"Error initializing bucket: {str(e)}")
        return False

def update_document_record(document_id: str, status: str, data: Dict[str, Any]) -> bool:
    """
    Update a document record in the database
    
    Args:
        document_id: The ID of the document to update
        status: New status for the document (processed, error, etc.)
        data: Additional data to add to the document record
    
    Returns:
        bool: True if update was successful, False otherwise
    """
    try:
        if not document_id:
            logger.error("Missing document_id for update")
            return False
            
        logger.info(f"Updating document record {document_id} with status: {status}")
        supabase = get_supabase_client()
        
        # Add timestamp to the update
        update_data = {
            "status": status,
            "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "processed_data": data
        }
        
        # Update the document record
        result = supabase.table("documents").update(update_data).eq("id", document_id).execute()
        
        if hasattr(result, 'data') and result.data:
            logger.info(f"Document record updated successfully: {document_id}")
            return True
        else:
            logger.warning(f"No document record found to update: {document_id}")
            return False
    except Exception as e:
        logger.error(f"Error updating document record: {str(e)}")
        return False

def check_document_status(document_id: str) -> Optional[Dict[str, Any]]:
    """
    Check the current status of a document
    
    Args:
        document_id: The ID of the document to check
        
    Returns:
        dict: Document record data or None if not found
    """
    try:
        supabase = get_supabase_client()
        result = supabase.table("documents").select("*").eq("id", document_id).limit(1).execute()
        
        if hasattr(result, 'data') and result.data and len(result.data) > 0:
            return result.data[0]
        return None
    except Exception as e:
        logger.error(f"Error checking document status: {str(e)}")
        return None
