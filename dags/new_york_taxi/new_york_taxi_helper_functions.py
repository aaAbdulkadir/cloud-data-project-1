import requests
from airflow.exceptions import AirflowException

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
            f"Failed to fetch data from {url}. 
            Status code: {response.status_code}"
        )
    