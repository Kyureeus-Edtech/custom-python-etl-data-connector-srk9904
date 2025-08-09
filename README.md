# Custom Python ETL Data Connector

**File**: README.md  
**Author**: SEETHARAM KILLIVALAVAN - 3122 22 5001 124 
**Assignment**: Software Architecture - Custom Python ETL Data Connector  
**Institution**: Kyureeus EdTech, SSN CSE

## Overview

This project implements a robust ETL (Extract, Transform, Load) pipeline that connects to the JSONPlaceholder API, processes the data, and stores it in a MongoDB database. The connector is designed with proper error handling, logging, and security practices.

## Features

- Secure API authentication using environment variables
- Robust error handling and logging
- Data transformation with quality scoring
- MongoDB integration with indexing
- Rate limiting and pagination support
- Comprehensive data validation
- Pipeline statistics and monitoring

## API Details

**Provider**: JSONPlaceholder (https://jsonplaceholder.typicode.com)  
**Endpoint**: `/posts`  
**Method**: GET  
**Authentication**: None required (public API)  
**Rate Limits**: No explicit limits, but connector implements respectful delays  
**Response Format**: JSON array of post objects

### API Response Structure
```json
{
  "userId": 1,
  "id": 1,
  "title": "sample title",
  "body": "sample body text"
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
git clone <repository-url>
cd <repository-name>
git checkout -b your-branch-name

### 2. Create Virtual Environment
python -m venv venv
source venv\Scripts\activate


### 3. Install Dependencies
pip install -r requirements.txt

### 4. Configure Environment Variables
Create a `.env` file in the project root and configure the following variables:
# Copy the example and modify as needed
cp .env.example .env


Required environment variables:
- `API_BASE_URL`: Base URL for the API (default: https://jsonplaceholder.typicode.com)
- `MONGO_URI`: MongoDB connection string (default: mongodb://localhost:27017/)
- `MONGO_DATABASE`: Database name (default: etl_database)
- `MONGO_COLLECTION`: Collection name (default: jsonplaceholder_posts_raw)
- `RATE_LIMIT_DELAY`: Delay between API calls in seconds (default: 1.0)

### 5. Start MongoDB
Ensure MongoDB is running on your system.

### Run the ETL Pipeline
python etl_connector.py


### Expected Output
The script will:
1. Connect to MongoDB
2. Extract data from JSONPlaceholder API
3. Transform and validate the data
4. Load data into MongoDB with proper indexing
5. Display pipeline statistics

### Example Log Output
```
2024-01-15 10:30:00,123 - INFO - Starting ETL pipeline execution
2024-01-15 10:30:01,456 - INFO - Successfully connected to MongoDB: etl_database.jsonplaceholder_posts_raw
2024-01-15 10:30:02,789 - INFO - Extracting data from: https://jsonplaceholder.typicode.com/posts
2024-01-15 10:30:03,012 - INFO - Successfully extracted 100 records
2024-01-15 10:30:03,234 - INFO - Successfully transformed 100 records
2024-01-15 10:30:03,567 - INFO - Data load completed: 100 inserted, 0 updated, 0 matched
2024-01-15 10:30:03,890 - INFO - ETL pipeline completed successfully in 3.77 seconds
```

## Data Schema

### Transformed Document Structure
```json
{
  "_id": "ObjectId",
  "original_data": {
    "userId": 1,
    "id": 1,
    "title": "original title",
    "body": "original body"
  },
  "etl_metadata": {
    "ingestion_timestamp": "2024-01-15T10:30:03.234Z",
    "source": "jsonplaceholder_api",
    "version": "1.0",
    "record_id": 1,
    "data_quality_score": 1.0
  },
  "post_id": 1,
  "user_id": 1,
  "title": "cleaned title",
  "body": "cleaned body",
  "title_word_count": 5,
  "body_word_count": 20,
  "has_content": true,
  "content_length": 125
}
```

### MongoDB Indexes
- `etl_metadata.ingestion_timestamp`: For time-based queries
- `post_id`: Unique index for duplicate prevention
- `user_id`: For user-based filtering

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
# Test MongoDB connection
python -c "from etl_connector import ETLConnector; ETLConnector().connect_to_mongodb()"

# Test API connectivity
curl https://jsonplaceholder.typicode.com/posts/1


### Data Validation
The pipeline includes automatic validation for:
- Required field presence
- Data type consistency
- Content quality scoring
- Duplicate record handling

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
- Verify API endpoint URL
- Review rate limiting settings

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


---

**Submission Details**  
Name: SEETHARAM KILLIVALAVAN  
Reg No. 3122 22 5001 124 
Submission Date: 10th Aug 2025