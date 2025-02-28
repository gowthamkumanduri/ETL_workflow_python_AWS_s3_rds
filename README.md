# ETL_workflow_python_AWS_s3_rds

**Project Overview**

This project implements an Enhanced ETL (Extract, Transform, Load) pipeline using Python and AWS services. The pipeline extracts data from local files (CSV, JSON, XML), uploads them to an S3 bucket, downloads them back for processing, transforms the data, and loads the final transformed dataset into an RDS database.

**Features**

1. Extracts data from CSV, JSON, and XML formats.

2. Uploads raw files to AWS S3.

3. Downloads files from S3 for processing.

4. Transforms data, including unit conversions (inches to meters, pounds to kilograms).

5. Loads the transformed data into AWS RDS.

6. Implements logging at each step for monitoring.

**AWS Services Used**

Amazon S3: Stores raw and transformed data files.

Amazon RDS: Stores processed and transformed data.

AWS SDK (boto3): Facilitates interaction with AWS services.

**Prerequisites**

Python 3.x installed

AWS account with IAM credentials configured

**Required Python libraries:**

pip install boto3 pandas sqlalchemy pymysql

Configure AWS credentials in the environment or use aws configure.


**Steps to Run the ETL Pipeline**

Extract Data: Reads files from a ZIP archive, unzips them, and uploads them to S3.

Upload to S3: The extracted files are stored in an AWS S3 bucket under a specified folder.

Download from S3: The raw files are retrieved from S3 for processing.

Transform Data: Cleans and modifies data (e.g., unit conversions, null value handling).

Load Data into RDS: The transformed dataset is stored in an AWS RDS instance.

**Execution**

Run the ETL pipeline script:

python etl_pipeline.py

**Logging**

The pipeline logs extraction, transformation, and loading operations with timestamps.

Logs are stored in a dedicated log file for monitoring and debugging.

**Conclusion**

This ETL pipeline integrates AWS S3 and RDS to create a scalable cloud-based data processing workflow. It ensures efficient data handling, transformation, and persistence, making it a robust solution for real-world ETL use cases.

Author

[Gowtham Raghava K]
