import requests
from airflow.exceptions import AirflowException
import polars as pl

def formulate_url(url: str, taxi_type: str, logical_timestamp: str) -> str:
    """_summary_

    Args:
        url (str): _description_
        taxi_type (str): _description_
        logical_timestamp (str): _description_

    Returns:
        str: _description_
    """
    year = logical_timestamp.year
    month = logical_timestamp.month
    month = f"{month:02d}"  # pad with leading zero if single digit    
    
    return url.format(taxi_type=taxi_type, year=year, month=month)
    

def get_response_data(url: str) -> dict:
    """Get the response data from the specified URL.

    Args:
        url (str): The URL from which to fetch the data.

    Returns:
        dict: The response data as a dictionary.
    """
    response = requests.get(url)
    
    if response.status_code == 200:
        return response.content
    else:
        raise AirflowException(
            f"Failed to fetch data from {url}. Status code: {response.status_code}"
        )
    
    
def join_taxi_zone_data(df: pl.DataFrame, taxi_zone_lookup_df: pl.DataFrame) -> pl.DataFrame:
    """_summary_

    Args:
        df (pl.DataFrame): _description_
        taxi_zone_lookup_df (pl.DataFrame): _description_

    Returns:
        pl.DataFrame: _description_
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