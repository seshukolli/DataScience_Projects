import logging
from pathlib import Path
from rds_connection import connect_to_rds
from rds_data_processor import RDSDataProcessor
import os
from dotenv import load_dotenv
import pandas as pd

# Remove existing handlers and configure logging
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.basicConfig(
    filename="process_log.txt",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Load environment variables
load_dotenv()
bucket_name = os.getenv("bucket_name")
file_name = os.getenv("file_name")
aws_access_key = os.getenv("aws_access_key")
aws_secret_key = os.getenv("aws_secret_key")

connection = None  # Initialize connection variable

try:
    # Connect to the RDS database
    connection = connect_to_rds()
    if connection:
        logging.info("Successfully connected to RDS.")

        # Initialize data processor
        processor = RDSDataProcessor(connection)
        
        # Step 1: Create and load the raw data table
        processor.create_table(file_name)
        processor.load_data(file_name)

        # Step 2: Fetch raw data from RDS for transformation
        table_name = Path(file_name).stem  # Table name derived from file name
        raw_data_query = f"SELECT * FROM `{table_name}`;"
        raw_data = pd.read_sql(raw_data_query, connection)

        # Step 3: Transform the raw data
        transformed_data = processor.transform_data(raw_data)

        # Step 4: Store transformed data in RDS
        transformed_table_name = f"{table_name}_transformed"
        processor.create_table(transformed_table_name, include_transformed_columns=True)
        processor.store_data_to_rds(transformed_data, transformed_table_name)

        # Step 5: Upload transformed data to S3
        transformed_file_name = f"{Path(file_name).stem}_transformed.csv"
        processor.store_data_to_s3(
            transformed_data,
            bucket_name=bucket_name,
            file_name=transformed_file_name,
            aws_access_key=aws_access_key,
            aws_secret_key=aws_secret_key
        )

        logging.info("Data processing pipeline completed successfully.")
    else:
        logging.error("Failed to connect to RDS.")
        print("Database connection unsuccessful. Check your RDS configuration.")
except Exception as e:
    logging.error(f"Unexpected error occurred: {e}")
    print("An error occurred. Check the log file for details.")
finally:
    if connection:
        connection.close()
        logging.info("Database connection closed.")
