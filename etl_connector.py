#!/usr/bin/env python3
"""
Custom ETL Data Connector for JSONPlaceholder API
File: etl_connector.py
Author: Seetharam Killivalavan
Reg No. 3122 22 5001 124
Description: ETL pipeline to extract posts data from JSONPlaceholder API,
             transform it for MongoDB compatibility, and load into MongoDB collection.
"""

import os
import sys
import json
import time
import logging
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
import requests
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, BulkWriteError
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_connector.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class ETLConnector:
    """
    ETL Connector class for extracting data from JSONPlaceholder API
    and loading it into MongoDB
    """
    
    def __init__(self):
        """Initialize the ETL connector with configuration"""
        # Load environment variables
        load_dotenv()
        
        # API Configuration
        self.base_url = os.getenv('API_BASE_URL', 'https://jsonplaceholder.typicode.com')
        self.api_key = os.getenv('API_KEY')  # Not required for JSONPlaceholder
        self.rate_limit_delay = float(os.getenv('RATE_LIMIT_DELAY', '1.0'))
        
        # MongoDB Configuration
        self.mongo_uri = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
        self.database_name = os.getenv('MONGO_DATABASE', 'etl_database')
        self.collection_name = os.getenv('MONGO_COLLECTION', 'jsonplaceholder_posts_raw')
        
        # Initialize MongoDB client
        self.mongo_client = None
        self.database = None
        self.collection = None
        
        # Request session for connection pooling
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'ETL-Connector/1.0',
            'Content-Type': 'application/json'
        })
        
        # Add API key to headers if provided
        if self.api_key:
            self.session.headers.update({
                'Authorization': f'Bearer {self.api_key}'
            })
    
    def connect_to_mongodb(self) -> bool:
        """
        Establish connection to MongoDB
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.mongo_client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=5000)
            # Test the connection
            self.mongo_client.admin.command('ping')
            self.database = self.mongo_client[self.database_name]
            self.collection = self.database[self.collection_name]
            logger.info(f"Successfully connected to MongoDB: {self.database_name}.{self.collection_name}")
            return True
        except ConnectionFailure as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            return False
    
    def extract_data(self, endpoint: str, params: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """
        Extract data from API endpoint
        
        Args:
            endpoint (str): API endpoint to call
            params (dict, optional): Query parameters
            
        Returns:
            List[Dict]: Extracted data or empty list on failure
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        all_data = []
        
        try:
            logger.info(f"Extracting data from: {url}")
            
            # Handle pagination if needed (JSONPlaceholder doesn't paginate, but this is a template)
            page = 1
            while True:
                current_params = params.copy() if params else {}
                current_params.update({'_page': page, '_limit': 20})
                
                response = self.session.get(url, params=current_params, timeout=30)
                
                # Handle rate limiting
                if response.status_code == 429:
                    logger.warning("Rate limit hit, waiting...")
                    time.sleep(self.rate_limit_delay * 2)
                    continue
                
                response.raise_for_status()
                data = response.json()
                
                # Break if no more data (for APIs that support pagination)
                if not data or len(data) == 0:
                    break
                
                all_data.extend(data if isinstance(data, list) else [data])
                
                # For JSONPlaceholder, we get all data in one request, so break after first iteration
                if endpoint == 'posts' and page == 1:
                    break
                
                page += 1
                time.sleep(self.rate_limit_delay)
            
            logger.info(f"Successfully extracted {len(all_data)} records")
            return all_data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            return []
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON response: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error during extraction: {e}")
            return []
    
    def transform_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform raw data for MongoDB compatibility
        
        Args:
            raw_data (List[Dict]): Raw data from API
            
        Returns:
            List[Dict]: Transformed data
        """
        transformed_data = []
        current_timestamp = datetime.now(timezone.utc)
        
        for record in raw_data:
            try:
                # Create transformed record
                transformed_record = {
                    # Original data
                    'original_data': record,
                    
                    # ETL metadata
                    'etl_metadata': {
                        'ingestion_timestamp': current_timestamp,
                        'source': 'jsonplaceholder_api',
                        'version': '1.0',
                        'record_id': record.get('id'),
                        'data_quality_score': self._calculate_data_quality(record)
                    },
                    
                    # Flatten and clean important fields
                    'post_id': record.get('id'),
                    'user_id': record.get('userId'),
                    'title': record.get('title', '').strip(),
                    'body': record.get('body', '').strip(),
                    'title_word_count': len(record.get('title', '').split()),
                    'body_word_count': len(record.get('body', '').split()),
                    'has_content': bool(record.get('title') or record.get('body')),
                }
                
                # Add derived fields
                transformed_record['content_length'] = len(
                    (transformed_record['title'] + ' ' + transformed_record['body']).strip()
                )
                
                transformed_data.append(transformed_record)
                
            except Exception as e:
                logger.warning(f"Failed to transform record {record}: {e}")
                continue
        
        logger.info(f"Successfully transformed {len(transformed_data)} records")
        return transformed_data
    
    def _calculate_data_quality(self, record: Dict[str, Any]) -> float:
        """
        Calculate a simple data quality score (0-1)
        
        Args:
            record (Dict): Data record
            
        Returns:
            float: Quality score
        """
        score = 0.0
        total_checks = 4
        
        # Check for required fields
        if record.get('id'):
            score += 0.25
        if record.get('userId'):
            score += 0.25
        if record.get('title') and len(record['title'].strip()) > 0:
            score += 0.25
        if record.get('body') and len(record['body'].strip()) > 0:
            score += 0.25
        
        return score
    
    def load_data(self, transformed_data: List[Dict[str, Any]]) -> bool:
        """
        Load transformed data into MongoDB
        
        Args:
            transformed_data (List[Dict]): Transformed data to load
            
        Returns:
            bool: True if load successful, False otherwise
        """
        if not transformed_data:
            logger.warning("No data to load")
            return True
        
        try:
            # Create indexes for better query performance
            self.collection.create_index("etl_metadata.ingestion_timestamp")
            self.collection.create_index("post_id", unique=True)
            self.collection.create_index("user_id")
            
            # Use direct insert/update operations instead of bulk_write
            inserted_count = 0
            updated_count = 0
            
            for record in transformed_data:
                try:
                    # Try to update existing record, insert if not found
                    result = self.collection.replace_one(
                        {'post_id': record['post_id']},
                        record,
                        upsert=True
                    )
                    
                    if result.upserted_id:
                        inserted_count += 1
                    elif result.modified_count > 0:
                        updated_count += 1
                        
                except Exception as e:
                    logger.warning(f"Failed to process record {record.get('post_id', 'unknown')}: {e}")
                    continue
            
            logger.info(f"Data load completed: {inserted_count} inserted, {updated_count} updated")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            return False
    
    def run_etl_pipeline(self) -> bool:
        """
        Execute the complete ETL pipeline
        
        Returns:
            bool: True if pipeline completed successfully
        """
        logger.info("Starting ETL pipeline execution")
        start_time = time.time()
        
        try:
            # Step 1: Connect to MongoDB
            if not self.connect_to_mongodb():
                return False
            
            # Step 2: Extract data
            raw_data = self.extract_data('posts')
            if not raw_data:
                logger.error("No data extracted, stopping pipeline")
                return False
            
            # Step 3: Transform data
            transformed_data = self.transform_data(raw_data)
            if not transformed_data:
                logger.error("No data transformed, stopping pipeline")
                return False
            
            # Step 4: Load data
            if not self.load_data(transformed_data):
                logger.error("Data load failed, stopping pipeline")
                return False
            
            execution_time = time.time() - start_time
            logger.info(f"ETL pipeline completed successfully in {execution_time:.2f} seconds")
            return True
            
        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            return False
        # Note: MongoDB client cleanup moved to main() function to allow stats collection
    
    def get_pipeline_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the data in MongoDB collection
        
        Returns:
            Dict: Pipeline statistics
        """
        if self.collection is None:
            return {}
        
        try:
            total_records = self.collection.count_documents({})
            
            stats = {
                'total_records': total_records,
                'users_count': len(self.collection.distinct('user_id')),
                'latest_ingestion': None,
                'avg_content_length': 0
            }
            
            # Only get additional stats if we have records
            if total_records > 0:
                # Get latest ingestion record
                latest_record = self.collection.find_one(
                    {}, sort=[('etl_metadata.ingestion_timestamp', -1)]
                )
                if latest_record:
                    stats['latest_ingestion'] = latest_record.get('etl_metadata', {}).get('ingestion_timestamp')
                
                # Get average content length
                avg_result = list(self.collection.aggregate([
                    {'$group': {'_id': None, 'avg_length': {'$avg': '$content_length'}}}
                ]))
                if avg_result:
                    stats['avg_content_length'] = round(avg_result[0].get('avg_length', 0), 2)
            
            return stats
        except Exception as e:
            logger.error(f"Failed to get pipeline stats: {e}")
            return {}


def main():
    """Main function to run the ETL connector"""
    connector = ETLConnector()
    
    try:
        # Run the ETL pipeline
        success = connector.run_etl_pipeline()
        
        if success:
            logger.info("ETL process completed successfully")
            
            # Display stats (before closing connections)
            stats = connector.get_pipeline_stats()
            if stats:
                logger.info(f"Pipeline Statistics: {json.dumps(stats, indent=2, default=str)}")
            
            sys.exit(0)
        else:
            logger.error("ETL process failed")
            sys.exit(1)
    
    finally:
        # Cleanup connections after everything is done
        if connector.mongo_client:
            connector.mongo_client.close()
        if connector.session:
            connector.session.close()


if __name__ == "__main__":
    main()