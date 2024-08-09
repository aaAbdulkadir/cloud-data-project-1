def extract(
    url: str,
    output_filename: str,
    logical_timestamp: "pendulum.datetime", # type: ignore
    params: dict
) -> None:
    import logging
    
    from new_york_taxi.new_york_taxi_helper_functions import (
        formulate_url,
        get_response_data
    )
    
    logger = logging.getLogger('extract')
    
    logger.info(f'LOGICAL TIMESTAMP: {logical_timestamp}')
        
    taxi_type = params['taxi_type']
    logger.info(f'Formulating URL for {taxi_type}')
    url = formulate_url(url, taxi_type, logical_timestamp)
    
    logger.info(f"Fetching data from {url}")
    response_content = get_response_data(url)
    
    logger.info(f"Writing data to {output_filename}")
    with open(output_filename, 'wb') as f:
        f.write(response_content)
    
    
def transform(input_filename: str, output_filename: str, config: dict, params: dict) -> None:
    import logging
    import numpy as np
    import polars as pl
    import os
    
    from new_york_taxi_helper_functions import join_taxi_zone_data
    
    logger = logging.getLogger('transform')
    
    logger.info('Reading in dataframe')
    df = pl.read_parquet(input_filename)

    logger.info('Renaming column names')
    taxi_type = params['taxi_type']
    df = df.rename(config[f'{taxi_type}_taxi']['columns_mappings'])

    logger.info('Filling in nan columns')
    expected_columns = config['expected_transform_columns']
    for col in expected_columns:
        if col not in df.columns:
            df = df.with_columns(pl.lit(np.nan).alias(col))

    logger.info('Remapping integer values into categorical values')
    categorical_values_mapping = config['categorical_values_mapping']

    for column, mapping in categorical_values_mapping.items():
        df = df.with_columns(
            pl.col(column).cast(pl.Utf8).replace(mapping).alias(column)
        )
    
    logger.info('Reading in taxi zone lookup table')
    taxi_zone_lookup = os.path.join(
        os.path.dirname(__file__), "config/taxi_zone_lookup.csv"
    )
    taxi_zone_lookup_df = pl.read_csv(taxi_zone_lookup)
    
    logger.info('Joining pickup and dropoff location id to its categorical values')
    df = join_taxi_zone_data(df, taxi_zone_lookup_df)
    
    logger.info('Saving dataframe to csv')
    df.write_csv(output_filename)
    
    