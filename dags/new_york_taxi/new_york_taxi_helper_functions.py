import requests
import pendulum
from airflow.exceptions import AirflowException, AirflowSkipException
import polars as pl


def formulate_url(url: str, taxi_type: str, logical_timestamp: pendulum.DateTime) -> str:
    """Formulates a URL by substituting placeholders with taxi type, year, and month.

    Args:
        url (str): URL template with placeholders for taxi type, year, and month.
        taxi_type (str): The type of taxi data (e.g., yellow, green).
        logical_timestamp (pendulum.DateTime): Timestamp used to determine the year and month.

    Returns:
        str: The formatted URL with the taxi type, year, and month.
    """
    year = logical_timestamp.year
    month = logical_timestamp.month
    month = f"{month:02d}"  # pad with leading zero if single digit    
    
    return url.format(taxi_type=taxi_type, year=year, month=month)
    

def get_response_data(url: str) -> dict:
    """Fetches data from the specified URL and returns it.

    Args:
        url (str): The URL from which to fetch the data.

    Returns:
        dict: The response data as a dictionary.

    Raises:
        AirflowSkipException: If the status code is 403, indicating data might not be available yet.
        AirflowException: For any other status codes indicating a failure to fetch data.
    """
    response = requests.get(url)
    
    if response.status_code == 200:
        return response.content
    elif response.status_code == 403:
        raise AirflowSkipException(
            f"Failed to fetch data from {url}. Status code: {response.status_code}. "
            "URL may not be available yet, skipping."
        )
    else:
        raise AirflowException(
            f"Failed to fetch data from {url}. Status code: {response.status_code}"
        )
    
    
def join_taxi_zone_data(df: pl.DataFrame, taxi_zone_lookup_df: pl.DataFrame) -> pl.DataFrame:
    """Joins taxi data with the taxi zone lookup dataframe on location IDs.

    Args:
        df (pl.DataFrame): The main taxi data dataframe.
        taxi_zone_lookup_df (pl.DataFrame): The dataframe containing taxi zone lookup data.

    Returns:
        pl.DataFrame: The resulting dataframe with taxi zone data joined.
    """
    pickup_dropoff_col_numbers = None
    if len(df['pickup_location_id'].unique()) > len(df['dropoff_location_id'].unique()):
        pickup_dropoff_col_numbers = 'pickup_location_id'
    else:
        pickup_dropoff_col_numbers = 'dropoff_location_id'
        
    taxi_zone_lookup_df = taxi_zone_lookup_df.with_columns(pl.col("location_id").cast(pl.Int32))
    
    df = df.join(
        taxi_zone_lookup_df, 
        left_on=pickup_dropoff_col_numbers, 
        right_on='location_id', 
        how='left'
    )
    df = df.drop(['pickup_location_id', 'dropoff_location_id'])
    
    return df