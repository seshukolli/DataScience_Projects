from dotenv import load_dotenv
import os
import logging
import pymysql
load_dotenv()

def connect_to_rds():
    host = os.getenv('DB_HOST')
    port = int(os.getenv('DB_PORT'))
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    database = os.getenv('DB_NAME')

    try:
        connection = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )
        logging.info("Successfully connected to RDS.")
        return connection
    except Exception as e:
        logging.error(f"Error connecting to RDS: {e}")
        return None
