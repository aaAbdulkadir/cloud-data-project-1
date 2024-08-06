def extract(
    url: str,
    output_filename: str,
    logical_timestamp: "pendulum.datetime", # type: ignore
    config: dict
) -> None:
    import logging
    
    from new_york_taxi.new_york_taxi_helper_functions import (
        formulate_url,
        get_response_data
    )
    
    logger = logging.getLogger('extract')
    
    taxi_type = config['taxi_type']
    logger.info(f'Formulating URL for {taxi_type}')
    url = formulate_url(url, taxi_type, logical_timestamp)
    
    logger.info(f"Fetching data from {url}")
    response_content = get_response_data(url)
    
    logger.info(f"Writing data to {output_filename}")
    with open(output_filename, 'w') as f:
        f.write(response_content)
    
    
    
def transform(input_filename: str, output_filename: str) -> None:

    return 1