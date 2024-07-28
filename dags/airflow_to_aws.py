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


def load_to_rds(input_filename: str, mode: str, dataset_name: str, fields: dict) -> None:
    """_summary_

    Args:
        input_filename (str): _description_
    """
    pass