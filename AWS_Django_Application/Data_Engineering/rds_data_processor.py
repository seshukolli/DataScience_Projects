import logging
from pathlib import Path
import pymysql
import pandas as pd
import numpy as np
import boto3
from sqlalchemy import create_engine
from io import StringIO
import os
from rds_connection import connect_to_rds

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.basicConfig(
    filename="process_log.txt",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class RDSDataProcessor:
    def __init__(self, connection):
        self.connection = connection
       
    def create_table(self, file_name, include_transformed_columns=False):
        table_name = Path(file_name).stem  # Extract table name without .csv extension
        cursor = self.connection.cursor()
        try:
            cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")  # Drop table if it exists

            if include_transformed_columns:
                cursor.execute(f"""
                    CREATE TABLE `{table_name}` (
                        `Index` INT PRIMARY KEY,
                        `User Id` INT,
                        `First Name` VARCHAR(50),
                        `Last Name` VARCHAR(50),
                        `Sex` VARCHAR(10),
                        `Email` VARCHAR(100),
                        `Phone` VARCHAR(20),
                        `Date of birth` DATE,
                        `Job Title` VARCHAR(100),
                        `Full Name` VARCHAR(150)
                    );
                """)
            else:
                cursor.execute(f"""
                    CREATE TABLE `{table_name}` (
                        `Index` INT PRIMARY KEY,
                        `User Id` INT,
                        `First Name` VARCHAR(50),
                        `Last Name` VARCHAR(50),
                        `Sex` VARCHAR(10),
                        `Email` VARCHAR(100),
                        `Phone` VARCHAR(20),
                        `Date of birth` DATE,
                        `Job Title` VARCHAR(100)
                    );
                """)
            logging.info("Table creation: successful")
        except Exception as e:
            logging.error(f"Error during table creation: {e}")
        finally:
            cursor.close()

    def load_data(self, file_name):
        table_name = Path(file_name).stem  # Extract table name without .csv extension
        cursor = self.connection.cursor()
        try:
            data = pd.read_csv(file_name)  # Use the file name here
            data = data.replace({np.nan: None})

            # Prepare insert query template
            insert_query = f"""
                INSERT INTO `{table_name}` (
                    `Index`, `User Id`, `First Name`, `Last Name`, `Sex`, `Email`,
                    `Phone`, `Date of birth`, `Job Title`
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """

            # Insert each row into the table
            for _, row in data.iterrows():
                values = (
                    row['Index'], row['User Id'], row['First Name'], row['Last Name'],
                    row['Sex'], row['Email'], row['Phone'], row['Date of birth'], row['Job Title']
                )
                cursor.execute(insert_query, values)

            # Commit the transaction
            self.connection.commit()
            logging.info(f"Data loaded into `{table_name}` table successfully")
        except Exception as e:
            logging.error(f"Error loading data into `{table_name}`: {e}")
        finally:
            cursor.close()

    def transform_data(self, data):
        try:
            # Example transformations
            data = data.dropna(subset=['Email'])
            data['Date of birth'] = pd.to_datetime(data['Date of birth'], errors='coerce')
            data['Full Name'] = data['First Name'] + ' ' + data['Last Name']
            logging.info("Data transformed successfully.")
            return data
        except Exception as e:
            logging.error(f"Error during data transformation: {e}")
            raise

    def store_data_to_s3(self, data, bucket_name, file_name, aws_access_key, aws_secret_key):
        csv_buffer = StringIO()
        data.to_csv(csv_buffer, index=False)
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )
        try:
            s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=csv_buffer.getvalue())
            logging.info(f"Data uploaded to S3 bucket '{bucket_name}' as '{file_name}'.")
        except Exception as e:
            logging.error(f"Failed to upload data to S3: {e}")

    def store_data_to_rds(self, data, table_name):
        cursor = self.connection.cursor()
        try:
            insert_query = f"""
                INSERT INTO `{table_name}` (
                    `Index`, `User Id`, `First Name`, `Last Name`, `Sex`, `Email`,
                    `Phone`, `Date of birth`, `Job Title`, `Full Name`
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            for _, row in data.iterrows():
                values = (
                    row['Index'], row['User Id'], row['First Name'], row['Last Name'],
                    row['Sex'], row['Email'], row['Phone'], row['Date of birth'],
                    row['Job Title'], row['Full Name']
                )
                cursor.execute(insert_query, values)
            self.connection.commit()
            logging.info("Transformed data stored in RDS successfully.")
        except Exception as e:
            logging.error(f"Error storing data in RDS: {e}")
        finally:
            cursor.close()
