#!/usr/bin/env python3
"""
Custom ETL Data Connector for NVD CVE Feed
File: etl_connector.py
Author: Seetharam Killivalavan - 3122 22 5001 124 - CSE-C
Description: ETL pipeline to extract CVE data from NVD (National Vulnerability Database),
             transform it for MongoDB compatibility, and load into MongoDB collection.
Data Source: Entry #13 from provided connector list - NVD CVE Feed
"""

import os
import sys
import json
import time
import logging
from datetime import datetime, timezone, timedelta
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

class NVDETLConnector:
    """
    ETL Connector class for extracting CVE data from NVD (National Vulnerability Database)
    and loading it into MongoDB - Based on Entry #13 from connector requirements
    """
    
    def __init__(self):
        """Initialize the ETL connector with configuration"""
        # Load environment variables
        load_dotenv()
        
        # API Configuration (NVD CVE Feed - Entry #13 from connector list)
        self.base_url = os.getenv('API_BASE_URL', 'https://services.nvd.nist.gov')
        self.rate_limit_delay = float(os.getenv('RATE_LIMIT_DELAY', '6.0'))  # NVD requires 6 seconds between requests
        
        # MongoDB Configuration
        self.mongo_uri = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
        self.database_name = os.getenv('MONGO_DATABASE', 'etl_database')
        self.collection_name = os.getenv('MONGO_COLLECTION', 'nvd_cve_raw')
        
        # Initialize MongoDB client
        self.mongo_client = None
        self.database = None
        self.collection = None
        
        # Request session for connection pooling
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'ETL-Connector/1.0 (Educational Purpose)',
            'Accept': 'application/json'
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
    
    def extract_data(self, days_back: int = 7) -> List[Dict[str, Any]]:
        """
        Extract CVE data from NVD API (Entry #13 from connector list)
        
        Args:
            days_back (int): Number of days back to fetch CVEs (default: 7)
            
        Returns:
            List[Dict]: Extracted CVE data or empty list on failure
        """
        all_cves = []
        
        try:
            # Calculate date range (NVD requires date filters)
            end_date = datetime.now(timezone.utc)
            start_date = end_date - timedelta(days=days_back)
            
            # Format dates for NVD API (ISO format)
            start_date_str = start_date.strftime('%Y-%m-%dT%H:%M:%S.000')
            end_date_str = end_date.strftime('%Y-%m-%dT%H:%M:%S.000')
            
            # Build API endpoint from connector list
            endpoint = '/rest/json/cves/2.0'
            url = f"{self.base_url}{endpoint}"
            
            logger.info(f"Extracting CVE data from: {url}")
            logger.info(f"Date range: {start_date_str} to {end_date_str}")
            
            # API parameters
            params = {
                'pubStartDate': start_date_str,
                'pubEndDate': end_date_str,
                'resultsPerPage': 20,  # Start with smaller batch for demo
                'startIndex': 0
            }
            
            # Single request for demo purposes (to avoid long wait times)
            logger.info(f"Requesting CVEs from NVD API...")
            
            response = self.session.get(url, params=params, timeout=30)
            
            # Handle rate limiting (NVD is strict about this)
            if response.status_code == 429:
                logger.warning("Rate limit hit, waiting 10 seconds...")
                time.sleep(10)
                response = self.session.get(url, params=params, timeout=30)
            
            response.raise_for_status()
            data = response.json()
            
            # Extract CVEs from response
            cves = data.get('vulnerabilities', [])
            all_cves.extend(cves)
            
            logger.info(f"Successfully extracted {len(all_cves)} CVE records")
            return all_cves
            
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
        Transform raw CVE data for MongoDB compatibility
        
        Args:
            raw_data (List[Dict]): Raw CVE data from NVD API
            
        Returns:
            List[Dict]: Transformed data
        """
        transformed_data = []
        current_timestamp = datetime.now(timezone.utc)
        
        for cve_record in raw_data:
            try:
                # Extract CVE details
                cve = cve_record.get('cve', {})
                cve_id = cve.get('id', 'unknown')
                
                # Extract basic information
                descriptions = cve.get('descriptions', [])
                english_desc = next((d['value'] for d in descriptions if d.get('lang') == 'en'), '')
                
                # Extract CVSS scores
                metrics = cve.get('metrics', {})
                cvss_v3 = metrics.get('cvssMetricV31', [])
                cvss_score = cvss_v3[0].get('cvssData', {}).get('baseScore') if cvss_v3 else None
                cvss_severity = cvss_v3[0].get('cvssData', {}).get('baseSeverity') if cvss_v3 else None
                
                # Extract references
                references = cve.get('references', [])
                reference_urls = [ref.get('url', '') for ref in references]
                
                # Extract CWE information
                weaknesses = cve.get('weaknesses', [])
                cwe_ids = []
                for weakness in weaknesses:
                    for desc in weakness.get('description', []):
                        if desc.get('lang') == 'en':
                            cwe_ids.append(desc.get('value', ''))
                
                # Create transformed record
                transformed_record = {
                    # Original data
                    'original_data': cve_record,
                    
                    # ETL metadata
                    'etl_metadata': {
                        'ingestion_timestamp': current_timestamp,
                        'source': 'nvd_cve_feed',
                        'version': '1.0',
                        'record_id': cve_id,
                        'data_quality_score': self._calculate_cve_quality(cve)
                    },
                    
                    # Flatten and clean important fields
                    'cve_id': cve_id,
                    'description': english_desc,
                    'published_date': cve.get('published'),
                    'last_modified': cve.get('lastModified'),
                    'cvss_score': cvss_score,
                    'cvss_severity': cvss_severity,
                    'reference_count': len(reference_urls),
                    'reference_urls': reference_urls[:10],  # Limit to first 10 references
                    'cwe_ids': cwe_ids,
                    'has_cvss_score': cvss_score is not None,
                    'is_recent': self._is_recent_cve(cve.get('published')),
                    'description_length': len(english_desc),
                    'severity_level': self._map_severity_to_level(cvss_severity)
                }
                
                transformed_data.append(transformed_record)
                
            except Exception as e:
                logger.warning(f"Failed to transform CVE record {cve_record}: {e}")
                continue
        
        logger.info(f"Successfully transformed {len(transformed_data)} CVE records")
        return transformed_data
    
    def _calculate_cve_quality(self, cve: Dict[str, Any]) -> float:
        """
        Calculate a simple data quality score for CVE (0-1)
        
        Args:
            cve (Dict): CVE data
            
        Returns:
            float: Quality score
        """
        score = 0.0
        
        # Check for required fields
        if cve.get('id'):
            score += 0.25
        if cve.get('descriptions'):
            score += 0.25
        if cve.get('metrics', {}).get('cvssMetricV31'):
            score += 0.25
        if cve.get('references'):
            score += 0.25
        
        return score
    
    def _is_recent_cve(self, published_date: str) -> bool:
        """Check if CVE is recent (within last 30 days)"""
        if not published_date:
            return False
        try:
            pub_date = datetime.fromisoformat(published_date.replace('Z', '+00:00'))
            cutoff = datetime.now(timezone.utc) - timedelta(days=30)
            return pub_date > cutoff
        except:
            return False
    
    def _map_severity_to_level(self, severity: str) -> int:
        """Map CVSS severity to numeric level"""
        severity_map = {
            'LOW': 1,
            'MEDIUM': 2,
            'HIGH': 3,
            'CRITICAL': 4
        }
        return severity_map.get(severity, 0)
    
    def load_data(self, transformed_data: List[Dict[str, Any]]) -> bool:
        """
        Load transformed CVE data into MongoDB
        
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
            self.collection.create_index("cve_id", unique=True)
            self.collection.create_index("cvss_severity")
            self.collection.create_index("published_date")
            
            # Use direct insert/update operations
            inserted_count = 0
            updated_count = 0
            
            for record in transformed_data:
                try:
                    # Try to update existing record, insert if not found
                    result = self.collection.replace_one(
                        {'cve_id': record['cve_id']},
                        record,
                        upsert=True
                    )
                    
                    if result.upserted_id:
                        inserted_count += 1
                    elif result.modified_count > 0:
                        updated_count += 1
                        
                except Exception as e:
                    logger.warning(f"Failed to process CVE {record.get('cve_id', 'unknown')}: {e}")
                    continue
            
            logger.info(f"Data load completed: {inserted_count} inserted, {updated_count} updated")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            return False
    
    def run_etl_pipeline(self) -> bool:
        """
        Execute the complete ETL pipeline for NVD CVE data
        
        Returns:
            bool: True if pipeline completed successfully
        """
        logger.info("Starting NVD CVE ETL pipeline execution")
        start_time = time.time()
        
        try:
            # Step 1: Connect to MongoDB
            if not self.connect_to_mongodb():
                return False
            
            # Step 2: Extract data (last 7 days of CVEs)
            raw_data = self.extract_data(days_back=7)
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
    
    def get_pipeline_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the CVE data in MongoDB collection
        
        Returns:
            Dict: Pipeline statistics
        """
        if self.collection is None:
            return {}
        
        try:
            total_records = self.collection.count_documents({})
            
            stats = {
                'total_cves': total_records,
                'critical_count': self.collection.count_documents({'cvss_severity': 'CRITICAL'}),
                'high_count': self.collection.count_documents({'cvss_severity': 'HIGH'}),
                'recent_cves': self.collection.count_documents({'is_recent': True}),
                'latest_ingestion': None,
                'avg_cvss_score': 0
            }
            
            # Only get additional stats if we have records
            if total_records > 0:
                # Get latest ingestion record
                latest_record = self.collection.find_one(
                    {}, sort=[('etl_metadata.ingestion_timestamp', -1)]
                )
                if latest_record:
                    stats['latest_ingestion'] = latest_record.get('etl_metadata', {}).get('ingestion_timestamp')
                
                # Get average CVSS score
                avg_result = list(self.collection.aggregate([
                    {'$match': {'cvss_score': {'$ne': None}}},
                    {'$group': {'_id': None, 'avg_score': {'$avg': '$cvss_score'}}}
                ]))
                if avg_result:
                    stats['avg_cvss_score'] = round(avg_result[0].get('avg_score', 0), 2)
            
            return stats
        except Exception as e:
            logger.error(f"Failed to get pipeline stats: {e}")
            return {}


def main():
    """Main function to run the NVD CVE ETL connector"""
    connector = NVDETLConnector()
    
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