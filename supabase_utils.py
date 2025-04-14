
"""
Supabase utility functions for storage and database operations
"""
import os
import logging
import json
import time
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
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://njjfycredoojnauidutp.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

# Initialize Supabase client
try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    logger.info("Supabase client initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize Supabase client: {str(e)}")
    supabase = None

def initialize_documents_bucket() -> bool:
    """
    Initialize the documents storage bucket if it doesn't exist
    
    Returns:
        True if successful, False otherwise
    """
    if not supabase:
        logger.error("Supabase client not initialized")
        return False
    
    try:
        # List all buckets
        response = supabase.storage.list_buckets()
        buckets = response if isinstance(response, list) else []
        
        # Check if documents bucket exists
        bucket_exists = any(bucket.get('name') == 'documents' for bucket in buckets if isinstance(bucket, dict))
        
        if not bucket_exists:
            logger.info("Creating documents bucket")
            try:
                supabase.storage.create_bucket(
                    'documents', 
                    {'public': False, 'file_size_limit': 10485760}  # 10MB limit
                )
                logger.info("Documents bucket created successfully")
                return True
            except Exception as create_error:
                logger.error(f"Failed to create documents bucket: {str(create_error)}")
                return False
        else:
            logger.info("Documents bucket already exists")
            return True
    except Exception as e:
        logger.error(f"Error checking/creating documents bucket: {str(e)}")
        return False

def update_document_record(document_id: str, status: str, processed_data: Dict[str, Any]) -> bool:
    """
    Update document record in the database
    
    Args:
        document_id: The ID of the document
        status: New status (processed, error, etc.)
        processed_data: Data from processing
        
    Returns:
        True if successful, False otherwise
    """
    if not supabase:
        logger.error("Supabase client not initialized")
        return False
    
    try:
        # Update document record
        logger.info(f"Updating document {document_id} status to {status}")
        
        update_data = {
            "status": status,
            "updated_at": "now()",
            "processed_data": processed_data
        }
        
        response = supabase.table("documents").update(update_data).eq("id", document_id).execute()
        
        # Check if the update was successful
        if not response.data:
            logger.warning(f"Document {document_id} update returned no data")
            return False
            
        logger.info(f"Document {document_id} updated successfully")
        return True
    except Exception as e:
        logger.error(f"Error updating document record: {str(e)}")
        return False

def get_document_info(document_id: str) -> Optional[Dict[str, Any]]:
    """
    Get document information from the database
    
    Args:
        document_id: The ID of the document
        
    Returns:
        Document information if found, None otherwise
    """
    if not supabase:
        logger.error("Supabase client not initialized")
        return None
    
    try:
        # Get document record
        response = supabase.table("documents").select("*").eq("id", document_id).execute()
        
        # Check if document exists
        if not response.data:
            logger.warning(f"Document {document_id} not found")
            return None
            
        document = response.data[0]
        logger.info(f"Document {document_id} retrieved successfully")
        return document
    except Exception as e:
        logger.error(f"Error retrieving document: {str(e)}")
        return None

def upload_file_to_storage(file_content: bytes, file_path: str) -> bool:
    """
    Upload a file to Supabase storage
    
    Args:
        file_content: Binary content of the file
        file_path: Path to store the file
        
    Returns:
        True if successful, False otherwise
    """
    if not supabase:
        logger.error("Supabase client not initialized")
        return False
    
    try:
        # Extract directory and filename
        parts = file_path.split('/')
        filename = parts[-1]
        
        # Get the directory path
        directory = '/'.join(parts[:-1]) if len(parts) > 1 else ''
        
        # Upload file
        response = supabase.storage.from_('documents').upload(
            file_path, 
            file_content,
            {'content-type': 'application/pdf'}
        )
        
        success = bool(response)
        if success:
            logger.info(f"File uploaded successfully to {file_path}")
        else:
            logger.error(f"File upload failed for {file_path}")
            
        return success
    except Exception as e:
        logger.error(f"Error uploading file: {str(e)}")
        return False
