```markdown name=README.md url=https://github.com/jicorporate/stedi-human-balance-analytics/edit/main/README.md
# STEDI Human Balance Analytics - Data Lakehouse

A data lakehouse project for analyzing human balance metrics using accelerometer data from STEDI (Sensors, Telematics, Electronics, Data Integration). This project leverages AWS cloud services to process, store, and analyze streaming accelerometer data.

## 📋 Project Overview

This project builds a scalable data lakehouse architecture to ingest, process, and analyze accelerometer sensor data from STEDI devices. The system captures real-time balance metrics and provides analytics capabilities through SQL queries on Amazon Athena.

### Key Features
- **Real-time Data Ingestion**: Stream accelerometer data to S3 landing zones
- **ETL Processing**: AWS Glue jobs for data transformation and enrichment
- **Data Lakehouse Architecture**: Multi-layer data organization (landing, trusted, verified)
- **SQL Analytics**: Query acceleration data using Amazon Athena
- **Spark Processing**: Distributed data processing with Apache Spark

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   STEDI Devices                         │
│              (Accelerometer Sensors)                    │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
         ┌───────────────────────┐
         │   Amazon S3           │
         │  Landing Zone         │
         │ (Raw Data)            │
         └───────┬───────────────┘
                 │
                 ▼
         ┌───────────────────────┐
         │   AWS Glue            │
         │   ETL Jobs            │
         │  (Transform Data)     │
         └───────┬───────────────┘
                 │
        ┌────────┴────────┐
        ▼                 ▼
    ┌────────┐       ┌────────┐
    │ Trusted│       │Verified│
    │ Zone   │       │ Zone   │
    └────────┘       └────────┘
        │                 │
        └────────┬────────┘
                 ▼
         ┌───────────────────────┐
         │  Amazon Athena        │
         │  SQL Analytics        │
         └───────────────────────┘
```

## 📁 Project Structure

```
stedi-human-balance-analytics/
├── sql/                        # SQL DDL/DML queries
│   └── accelerometer_landing.sql   # Create landing table
├── glue_jobs/                  # AWS Glue ETL scripts
├── screenshots/                # Project documentation screenshots
└── README.md                   # This file
```

## 🔧 Components

### SQL Scripts (`sql/`)
- **accelerometer_landing.sql**: Defines the external table for raw accelerometer data
  - Schema: `user`, `timeStamp`, `x`, `y`, `z` coordinates
  - Format: JSON with error handling
  - Location: S3 landing zone

### Glue Jobs (`glue_jobs/`)
ETL jobs for transforming and enriching accelerometer data:
- Data validation and cleansing
- Schema enforcement
- Data partitioning
- Quality checks

## 🚀 Getting Started

### Prerequisites
- AWS Account with appropriate permissions
- S3 bucket for data storage
- AWS Glue and Athena access
- Python 3.7+ (for Spark/Glue jobs)

### Setup Instructions

1. **Create S3 Bucket Structure**
   ```bash
   aws s3 mb s3://stedi-human-balance-analytics-<your-username>/
   aws s3 mb s3://stedi-human-balance-analytics-<your-username>/accelerometer/landing/
   aws s3 mb s3://stedi-human-balance-analytics-<your-username>/accelerometer/trusted/
   aws s3 mb s3://stedi-human-balance-analytics-<your-username>/accelerometer/verified/
   ```

2. **Create Glue Database**
   ```sql
   CREATE DATABASE IF NOT EXISTS stedi;
   ```

3. **Create Landing Table**
   - Run the SQL script in Athena:
   ```bash
   -- Copy contents of sql/accelerometer_landing.sql
   ```

4. **Deploy Glue Jobs**
   - Upload Glue scripts to S3
   - Create Glue jobs in AWS Console
   - Configure triggers for data processing

## 📊 Data Schema

### Accelerometer Landing Table
| Column | Type | Description |
|--------|------|-------------|
| user | string | User identifier |
| timeStamp | bigint | Unix timestamp (milliseconds) |
| x | double | X-axis acceleration (m/s²) |
| y | double | Y-axis acceleration (m/s²) |
| z | double | Z-axis acceleration (m/s²) |

## 🔍 Usage Examples

### Query Recent Accelerometer Data
```sql
SELECT 
    user,
    from_unixtime(timeStamp/1000) as event_time,
    x, y, z,
    sqrt(x*x + y*y + z*z) as total_acceleration
FROM stedi.accelerometer_landing
WHERE timeStamp > unix_timestamp(current_date - interval '1 day') * 1000
ORDER BY timeStamp DESC
LIMIT 100;
```

### Aggregate by User
```sql
SELECT 
    user,
    COUNT(*) as readings_count,
    AVG(x) as avg_x,
    AVG(y) as avg_y,
    AVG(z) as avg_z
FROM stedi.accelerometer_landing
GROUP BY user;
```

## 🔐 AWS Permissions Required

Ensure your AWS user/role has permissions for:
- `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` on S3 buckets
- `glue:CreateJob`, `glue:StartJobRun`
- `athena:StartQueryExecution`
- `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents`

## 📈 Data Flow

1. **Ingestion**: STEDI devices send accelerometer data
2. **Landing**: Raw JSON data stored in S3 landing zone
3. **Transformation**: Glue jobs validate, cleanse, and enrich data
4. **Storage**: Processed data stored in trusted/verified zones
5. **Analytics**: Query data via Athena for insights

## 🛠️ Development

### Adding New Tables
1. Create SQL DDL in `sql/` directory
2. Name following pattern: `table_name.sql`
3. Include external table configuration for S3 location
4. Test with sample data before production

### Creating Glue Jobs
1. Add Python script to `glue_jobs/`
2. Test locally with test data
3. Upload to S3 and create Glue job
4. Set appropriate concurrency and timeout values

## 📚 AWS Services Used

- **Amazon S3**: Data lake storage
- **AWS Glue**: ETL and data cataloging
- **Amazon Athena**: SQL query engine
- **Apache Spark**: Distributed processing (via Glue)

## 📝 License

This project is part of the STEDI platform.

## 🤝 Contributing

Contributions are welcome! Please:
1. Create a feature branch
2. Test changes thoroughly
3. Submit a pull request with clear description

## 📧 Support

For issues or questions:
- Check existing GitHub issues
- Create a new issue with detailed information
- Include relevant logs and error messages

---

**Last Updated**: May 2026  
**Project Status**: Active Development
```

This README provides:
- ✅ Clear project overview and architecture
- ✅ Directory structure explanation
- ✅ Setup instructions for AWS services
- ✅ Data schema documentation
- ✅ SQL query examples
- ✅ IAM permissions guidance
- ✅ Development guidelines
- ✅ Service descriptions
