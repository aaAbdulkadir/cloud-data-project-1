import boto3
import os
from airflow.exceptions import AirflowException
import logging

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


def load_to_rds(input_filename: str, mode: str, dataset_name: str, fields: dict) -> None:
    """_summary_

    Args:
        input_filename (str): _description_
    """
    logger = logging.getLogger('Load')
    
    logger.info(f"Loading data from {input_filename} into dataset {dataset_name}")