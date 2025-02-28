import boto3
import pandas as pd
import sqlalchemy
import logging
import os
import zipfile
import xml.etree.ElementTree as ET
import glob
from datetime import datetime

# Configure Logging
logging.basicConfig(filename='C:/Users/91798/Desktop/ME36/Projects/DE_Enhanced ETL Workflow with Python, AWS S3, RDS, and Glue/log/etl_pipeline.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# AWS Credentials Configuration (Use environment variables or AWS CLI configured credentials)
AWS_ACCESS_KEY = ''
AWS_SECRET_KEY = ''
S3_BUCKET = 'deminiproj3'
RAW_DATA_FOLDER = 'Source/'  # Folder in S3 for raw files
TRANSFORMED_DATA_FOLDER = 'Destination/'  # Folder in S3 for transformed files
RDS_ENDPOINT = 'deminiproj3.crkwqcomy7as.eu-north-1.rds.amazonaws.com'
RDS_PORT = '3306'
RDS_USER = 'admin'
RDS_PASSWORD = 'deminiproj3'
RDS_DB = 'deminiproj3'

# Initialize AWS Clients
s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)

# Database Engine Setup
engine = sqlalchemy.create_engine(f"mysql+pymysql://{RDS_USER}:{RDS_PASSWORD}@{RDS_ENDPOINT}:{RDS_PORT}/{RDS_DB}")

# Step 1: Read and Unzip Data Locally
def unzip_local(zip_path, extract_path):
    logging.info("Extracting data from zip file...")
    start_time = datetime.now()
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)
    logging.info("Data files extracted in %s", datetime.now() - start_time)

# Step 2: Upload Files to S3
def upload_to_s3(file_path, bucket, object_name):
    s3_client.upload_file(file_path, bucket, object_name)
    logging.info(f"Uploaded {file_path} to S3 bucket {bucket} as {object_name}")

# Step 3: Download Files from S3
def download_from_s3(bucket, object_name, file_path):
    logging.info("Downloading %s from S3...", object_name)
    start_time = datetime.now()
    s3_client.download_file(bucket, object_name, file_path)
    logging.info("Downloaded %s in %s", object_name, datetime.now() - start_time)

# Step 4: Extract Data
def extract_csv(file_path):
    logging.info("Extracting CSV: %s", file_path)
    start_time = datetime.now()
    data = pd.read_csv(file_path)
    logging.info("Extracted CSV in %s", datetime.now() - start_time)
    return data.to_dict(orient='records')

def extract_json(file_path):
    logging.info("Extracting JSON: %s", file_path)
    start_time = datetime.now()
    data = pd.read_json(file_path, lines=True)
    logging.info("Extracted JSON in %s", datetime.now() - start_time)
    return data.to_dict(orient='records')

def extract_xml(file_path):
    logging.info("Extracting XML: %s", file_path)
    start_time = datetime.now()
    tree = ET.parse(file_path)
    root = tree.getroot()
    data = [{elem.tag: elem.text for elem in child} for child in root]
    logging.info("Extracted XML in %s", datetime.now() - start_time)
    return data

def extract_files(folder_path):
    logging.info("Extracting files from folder: %s", folder_path)
    combined_data = []
    for file_path in glob.glob(f"{folder_path}/*"):
        if file_path.endswith('.csv'):
            combined_data.extend(extract_csv(file_path))
        elif file_path.endswith('.json'):
            combined_data.extend(extract_json(file_path))
        elif file_path.endswith('.xml'):
            combined_data.extend(extract_xml(file_path))
    return combined_data

# Step 5: Transform Data
def transform(data):
    logging.info("Transforming data...")
    start_time = datetime.now()
    transformed_data = []
    for record in data:
        transformed_record = {}
        for key, value in record.items():
            if key == 'height':
                transformed_record[key] = float(value) * 0.0254  # Convert inches to meters
            elif key == 'weight':
                transformed_record[key] = float(value) * 0.453592  # Convert pounds to kilograms
            else:
                transformed_record[key] = value
        transformed_data.append(transformed_record)
    logging.info("Data transformation completed in %s", datetime.now() - start_time)
    return transformed_data

# Step 6: Load Data into RDS and S3
def load_csv(data, output_file):
    logging.info("Loading data into CSV: %s", output_file)
    pd.DataFrame(data).to_csv(output_file, index=False)
    logging.info("Data successfully loaded into %s", output_file)

def load_to_rds(data, table_name):
    try:
        pd.DataFrame(data).to_sql(table_name, con=engine, if_exists='replace', index=False)
        logging.info("Data loaded into RDS table %s", table_name)
    except Exception as e:
        logging.error("Error loading data into RDS: %s", e)

# Main ETL Execution
if __name__ == '__main__':
    zip_path = "C:/Users/91798/Desktop/ME36/Projects/DE_Enhanced ETL Workflow with Python, AWS S3, RDS, and Glue/Source_files_to_upload_to_s3/source.zip"
    extract_path = "C:/Users/91798/Desktop/ME36/Projects/DE_Enhanced ETL Workflow with Python, AWS S3, RDS, and Glue/Source_files_to_upload_to_s3/source"
    output_file = "final_transformed_data.csv"
    unzip_local(zip_path, extract_path)
    for file in os.listdir(extract_path):
        file_path = os.path.join(extract_path, file)
        upload_to_s3(file_path, S3_BUCKET, RAW_DATA_FOLDER + file)
    s3_processed_path = "downloaded_data"
    print(f"Downloaded files will be saved to: {os.path.abspath(s3_processed_path)}")
    os.makedirs(s3_processed_path, exist_ok=True)
    for file in os.listdir(extract_path):
        download_from_s3(S3_BUCKET, RAW_DATA_FOLDER + file, os.path.join(s3_processed_path, file))
    transformed_data = transform(extract_files(s3_processed_path))
    if transformed_data:
        load_csv(transformed_data, output_file)
        upload_to_s3(output_file, S3_BUCKET, TRANSFORMED_DATA_FOLDER + output_file)
        load_to_rds(transformed_data, 'etl_table')
    logging.info("ETL Pipeline Execution Completed")
