# Custom Python ETL Data Connector

**File**: README.md  
**Author**: Seetharam Killivalavan - 3122 22 5001 124 (CSE-C)
**Assignment**: Software Architecture - Custom Python ETL Data Connector  
**Institution**: Kyureeus EdTech, SSN CSE  
**Data Source**: NVD CVE Feed (Entry #13 from provided connector list)

## Overview

This project implements a robust ETL (Extract, Transform, Load) pipeline that connects to the National Vulnerability Database (NVD) CVE Feed API, processes cybersecurity vulnerability data, and stores it in a MongoDB database. The connector follows secure coding practices and handles real-world vulnerability data.

## Features

- Secure API authentication using environment variables
- Robust error handling and logging
- CVE data transformation with quality scoring
- MongoDB integration with proper indexing
- Rate limiting compliance (NVD requires 6-second delays)
- Comprehensive data validation
- Pipeline statistics and monitoring
- Real cybersecurity vulnerability data processing

## API Details

**Provider**: National Vulnerability Database (NVD)  
**Source**: Entry #13 from provided connector list  
**Base URL**: https://services.nvd.nist.gov  
**Endpoint**: `/rest/json/cves/2.0?...`  
**Method**: GET  
**Authentication**: None required (public API)  
**Rate Limits**: 6 seconds between requests (strictly enforced)  
**Response Format**: JSON with nested CVE vulnerability data

### API Response Structure
```json
{
  "vulnerabilities": [
    {
      "cve": {
        "id": "CVE-2024-XXXX",
        "descriptions": [{"lang": "en", "value": "Description..."}],
        "metrics": {
          "cvssMetricV31": [{
            "cvssData": {
              "baseScore": 7.5,
              "baseSeverity": "HIGH"
            }
          }]
        },
        "references": [{"url": "https://..."}],
        "published": "2024-01-01T00:00:00.000",
        "lastModified": "2024-01-01T00:00:00.000"
      }
    }
  ]
}
```

## Project Structure

```
/your-branch-name/
├── etl_connector.py      # Main ETL pipeline script
├── .env                  # Environment variables (not committed)
├── requirements.txt      # Python dependencies
├── README.md            # This documentation
├── .gitignore           # Git ignore rules
└── etl_connector.log    # Generated log file
```

## Prerequisites

- Python 3.7 or higher
- MongoDB server (local or remote)
- Internet connection for API access

## Installation and Setup

### 1. Clone the Repository
```bash
git clone <repository-url>
cd <repository-name>
git checkout -b your-branch-name
```

### 2. Create Virtual Environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables
Create a `.env` file in the project root and configure the following variables:

```bash
# Copy the example and modify as needed
cp .env.example .env
```

Required environment variables:
- `API_BASE_URL`: Base URL for NVD API (default: https://services.nvd.nist.gov)
- `MONGO_URI`: MongoDB connection string (default: mongodb://localhost:27017/)
- `MONGO_DATABASE`: Database name (default: etl_database)
- `MONGO_COLLECTION`: Collection name (default: nvd_cve_raw)
- `RATE_LIMIT_DELAY`: Delay between API calls in seconds (default: 6.0 - required by NVD)

### 5. Start MongoDB
Ensure MongoDB is running on your system:
```bash
# For local MongoDB installation
mongod

# Or use MongoDB Atlas cloud service
```

## Usage

### Run the ETL Pipeline
```bash
python etl_connector.py
```

### Expected Output
The script will:
1. Connect to MongoDB
2. Extract CVE data from NVD API (last 7 days)
3. Transform and validate the vulnerability data
4. Load data into MongoDB with proper indexing
5. Display pipeline statistics

### Example Log Output
```
2024-01-15 10:30:00,123 - INFO - Starting NVD CVE ETL pipeline execution
2024-01-15 10:30:01,456 - INFO - Successfully connected to MongoDB: etl_database.nvd_cve_raw
2024-01-15 10:30:02,789 - INFO - Extracting CVE data from: https://services.nvd.nist.gov/rest/json/cves/2.0
2024-01-15 10:30:08,012 - INFO - Successfully extracted 15 CVE records
2024-01-15 10:30:08,234 - INFO - Successfully transformed 15 CVE records
2024-01-15 10:30:08,567 - INFO - Data load completed: 15 inserted, 0 updated
2024-01-15 10:30:08,890 - INFO - ETL pipeline completed successfully in 8.77 seconds
```

## Data Schema

### Transformed Document Structure
```json
{
  "_id": "ObjectId",
  "original_data": {
    "cve": {
      "id": "CVE-2024-XXXX",
      "descriptions": [...],
      "metrics": {...},
      "references": [...]
    }
  },
  "etl_metadata": {
    "ingestion_timestamp": "2024-01-15T10:30:03.234Z",
    "source": "nvd_cve_feed",
    "version": "1.0",
    "record_id": "CVE-2024-XXXX",
    "data_quality_score": 1.0
  },
  "cve_id": "CVE-2024-XXXX",
  "description": "Vulnerability description...",
  "published_date": "2024-01-01T00:00:00.000",
  "last_modified": "2024-01-01T00:00:00.000",
  "cvss_score": 7.5,
  "cvss_severity": "HIGH",
  "reference_count": 3,
  "reference_urls": ["https://..."],
  "cwe_ids": ["CWE-79"],
  "has_cvss_score": true,
  "is_recent": true,
  "description_length": 245,
  "severity_level": 3
}
```

### MongoDB Indexes
- `etl_metadata.ingestion_timestamp`: For time-based queries
- `cve_id`: Unique index for duplicate prevention
- `cvss_severity`: For severity-based filtering
- `published_date`: For date-based queries

## Error Handling

The connector handles various error scenarios:
- Network connectivity issues
- API rate limiting (429 responses)
- Invalid JSON responses
- MongoDB connection failures
- Data validation errors
- Bulk write errors

## Security Features

- Environment variables for sensitive configuration
- `.env` file excluded from version control
- Secure MongoDB connection handling
- Input validation and sanitization
- Proper session management

## Testing and Validation

### Manual Testing
```bash
# Test MongoDB connection
python -c "from etl_connector import ETLConnector; ETLConnector().connect_to_mongodb()"

# Test API connectivity
curl https://jsonplaceholder.typicode.com/posts/1
```

### Data Validation
The pipeline includes automatic validation for:
- CVE ID presence and format
- CVSS score availability and validity
- Description completeness
- Reference URL accessibility
- Data freshness (recent vs. older CVEs)
- Content quality scoring

## Monitoring and Logging

- Comprehensive logging to both file and console
- Pipeline execution statistics
- Data quality metrics
- Error tracking and reporting

## Troubleshooting

### Common Issues

**MongoDB Connection Error**
- Ensure MongoDB is running
- Check connection string in `.env`
- Verify network connectivity

**API Request Failures**
- Check internet connectivity
- Verify NVD API endpoint URL
- Ensure 6-second delay between requests
- Check for API maintenance windows

**Import Errors**
- Ensure all dependencies are installed
- Check Python version compatibility
- Activate virtual environment

### Debug Mode
Set `LOG_LEVEL=DEBUG` in `.env` for detailed logging.

## Performance Considerations

- Bulk operations for efficient MongoDB writes
- Connection pooling for API requests
- Indexed MongoDB collections for fast queries
- Configurable rate limiting
- Pagination support for large datasets

## Future Enhancements

- Support for multiple API endpoints
- Real-time data streaming
- Data deduplication strategies
- Advanced error recovery mechanisms
- Monitoring dashboard integration
- Automated data quality reporting

## Dependencies

- `requests`: HTTP library for API calls
- `pymongo`: MongoDB Python driver
- `python-dotenv`: Environment variable management
- `certifi`: SSL certificate verification
- `dnspython`: MongoDB SRV record support

## Contributing

1. Create a feature branch from main
2. Implement changes with proper testing
3. Update documentation as needed
4. Submit pull request with descriptive commit message

## License

This project is created for educational purposes as part of the Software Architecture course at SSN CSE through Kyureeus EdTech.

---

**Submission Details**  
Name: Seetharam Killivalavan  
Reg No. 3122 22 5001 124  
Branch: CSE-C  
Submission Date: 13=08-25