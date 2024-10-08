import boto3
import os
from airflow.exceptions import AirflowException
import psycopg2
import pandas as pd
import logging
from datetime import datetime
from airflow.models import Variable
from psycopg2 import extras

def upload_to_s3(local_file_path: str, bucket: str, s3_key: str) -> None:
    """
    Upload a file to S3 and remove it from the local filesystem.

    Args:
        local_file_path (str): The path to the local file
        bucket (str): The S3 bucket name
        s3_key (str): The S3 object key (path in the bucket) for the uploaded file

    Raises:
        AirflowException: If there's an error during upload or file removal
    """
    logger = logging.getLogger('S3 Transfer')

    s3_client = boto3.client('s3')

    try:
        if not os.path.exists(local_file_path):
            raise FileNotFoundError(f"Local file not found: {local_file_path}")

        s3_client.upload_file(local_file_path, bucket, s3_key)
        logger.info(f"Successfully uploaded {local_file_path} to s3://{bucket}/{s3_key}")

        os.remove(local_file_path)
        logger.info(f"Successfully removed local file: {local_file_path}")

    except Exception as e:
        raise AirflowException(f"Unexpected error: {str(e)}")
    
    
def retrieve_from_s3(bucket: str, s3_key: str, local_file_path: str) -> None:
    """
    Retrieve a file from S3 and save it to the local filesystem.

    Args:
        bucket (str): The S3 bucket name
        s3_key (str): The S3 object key (path in the bucket) of the file to retrieve
        local_file_path (str): The path where the file should be saved locally

    Raises:
        AirflowException: If there's an error during retrieval or file saving
    """
    logger = logging.getLogger('S3 Transfer')

    s3_client = boto3.client('s3')

    try:
        s3_client.download_file(bucket, s3_key, local_file_path)
        logger.info(f"Successfully retrieved s3://{bucket}/{s3_key} to {local_file_path}")

    except Exception as e:
        raise AirflowException(f"Unexpected error: {str(e)}")


def get_latest_file_from_s3(bucket: str, dag_id: str, task_id: str) -> str:
    """
    Retrieve the latest file from an S3 bucket using a prefix that is constructed 
    based on the DAG ID and Task ID. This function helps to ensure that each task 
    within a DAG retrieves its own respective files without any overlap.

    Args:
        bucket (str): The name of the S3 bucket to search.
        dag_id (str): The ID of the DAG running the task.
        task_id (str): The ID of the task within the DAG.

    Returns:
        str: The key (path) of the latest file in the S3 bucket that matches the 
             specified prefix. If no files are found, returns None.

    Raises:
        AirflowException: If an unexpected error occurs during the S3 operation.
    """
    try:
        s3 = boto3.client('s3')
        prefix = f"{dag_id}/{dag_id}_{task_id}"
        objects = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if 'Contents' not in objects:
            return None

        # Sort the objects by LastModified in descending order
        sorted_objects = sorted(objects['Contents'], key=lambda obj: obj['LastModified'], reverse=True)
        
        # Return the key of the latest object
        return sorted_objects[0]['Key'] if sorted_objects else None

    except Exception as e:
        raise AirflowException(f"Unexpected error: {str(e)}")
    

def load_to_rds(dataset_name: str, input_filename: str, fields: list, mode: str) -> None:
    """Load data from a CSV file into a PostgreSQL database table.

    This function creates or replaces a table in the PostgreSQL database according
    to the provided mode and loads data from the given CSV file into the table.

    Args:
        dataset_name (str): The name of the database table to load the data into.
        input_filename (str): The path to the input CSV file.
        fields (list): A list of dictionaries defining the table schema. Each dictionary
                       should contain 'name' and 'type' keys.
        mode (str): The mode of table creation. Can be 'append' to add data to an existing table
                    or 'replace' to create a new table, dropping the old one.
    
    Raises:
        ValueError: If an invalid mode is provided.
        Exception: If any error occurs during database operations.
    """
    logger = logging.getLogger('load_to_rds')

    host = Variable.get("AWS_DB_ENDPOINT")
    user = Variable.get("AWS_DB_USER")
    password = Variable.get("AWS_DB_PASSWORD")
    dbname = Variable.get("AWS_DB_NAME")
    port = "5432"  

    df = pd.read_csv(input_filename)
    # order column names
    df = df[[field['name'] for field in fields]]

    try:
        connection = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        cursor = connection.cursor()

        if mode == 'append':
            create_table_query = f"CREATE TABLE IF NOT EXISTS {dataset_name} ("
        elif mode == 'replace':
            drop_table_query = f"DROP TABLE IF EXISTS {dataset_name};"
            cursor.execute(drop_table_query)
            connection.commit()
            create_table_query = f"CREATE TABLE {dataset_name} ("
        else:
            raise ValueError("Invalid mode. Choose 'append' or 'replace'.")

        for field in fields:
            field_name = field['name']
            field_type = field['type']
            create_table_query += f"{field_name} {field_type}, "
        create_table_query += "scraping_execution_date TIMESTAMP);"
        
        logger.info(f'Running {mode} on query: {create_table_query}')

        cursor.execute(create_table_query)
        connection.commit()

        columns = ', '.join([field['name'] for field in fields])
        columns += ", scraping_execution_date"
        insert_query = f"INSERT INTO {dataset_name} ({columns}) VALUES %s"
        records = [(tuple(row) + (datetime.now(),)) for row in df.to_numpy()]
        
        logger.info(f'Running insert query: {insert_query}')
        
        extras.execute_values(cursor, insert_query, records)
        connection.commit()

    except Exception as e:
        logger.error(f"Error loading data to RDS: {str(e)}")
        raise

    finally:
        cursor.close()
        connection.close()

    logger.info('Loaded to database successfully.')
