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
        
    taxi_type = params['taxi_type']
    logger.info(f'Formulating URL for {taxi_type}')
    url = formulate_url(url, taxi_type, logical_timestamp)
    
    logger.info(f"Fetching data from {url}")
    response_content = get_response_data(url)
    
    logger.info(f"Writing data to {output_filename}")
    with open(output_filename, 'wb') as f:
        f.write(response_content)
        
        
def extract_historical(
    url: str,
    output_filename: str,
    params: dict
):
    import logging
    import pendulum
    from zipfile import ZipFile
    import os
    
    from new_york_taxi.new_york_taxi_helper_functions import (
        formulate_url,
        get_response_data
    )
    
    logger = logging.getLogger('extract_historical')
    
    taxi_type = params['taxi_type']

    start_year = int(params['start_date'].split('-')[0])
    end_year = int(params['end_date'].split('-')[0])
    start_month = int( params['start_date'].split('-')[1])
    end_month = int( params['end_date'].split('-')[1])
    
    with ZipFile(output_filename, 'w') as zipf:
        for year in range(start_year, end_year + 1):
            for month in range(start_month, end_month + 1):
                logger.info(f'Formulating URL for {taxi_type} {year}-{month:02d}')
                url = formulate_url(url, taxi_type, pendulum.datetime(year, month, 1))
                
                logger.info(f"Fetching data from {url}")
                response_content = get_response_data(url)
                
                filename = f'{year}_{month}.parquet'
                logger.info(f'Saving {filename} to zip file')
                zipf.writestr(filename, response_content)
        
    
    
def transform(input_filename: str, output_filename: str, config: dict, params: dict) -> None:
    import logging
    import numpy as np
    import polars as pl
    import os
    import pyarrow.parquet as pq
    from zipfile import ZipFile
    
    from new_york_taxi.new_york_taxi_helper_functions import join_taxi_zone_data
    
    logger = logging.getLogger('transform')
    
    dfs = []
    historical = params['historical']
    logger.info('Reading in taxi zone lookup table')
    taxi_zone_lookup = os.path.join(
            os.path.dirname(__file__), "config/taxi_zone_lookup.csv"
        )
    taxi_zone_lookup_df = pl.read_csv(taxi_zone_lookup)
    
    if historical is True:
        with ZipFile(input_filename, 'r') as zipf:
            for filename in zipf.namelist():
                logger.info(f'Processing file {filename} from ZIP archive')
                
                with zipf.open(filename) as file:
                    table = pq.read_table(file)
                    df = pl.from_arrow(table)
                    df = df.head(10000)
                    dfs.append(df)
                    
    else:
        logger.info('Reading in dataframe')
        df = pl.read_parquet(input_filename)
        df = df.head(10000)
        dfs.append(df)
    

    taxi_type = params['taxi_type']
    
    final_dfs = []
    for df in dfs:
        logger.info('Renaming column names')
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
        df = df.with_columns(
            pl.col('pickup_location_id').cast(pl.Int32),
            pl.col('dropoff_location_id').cast(pl.Int32)
        )
        taxi_zone_lookup_df = taxi_zone_lookup_df.with_columns(
            pl.col('location_id').cast(pl.Int32)
        )
        logger.info('Joining pickup and dropoff location id to its categorical values')
        df = join_taxi_zone_data(df, taxi_zone_lookup_df)
        
        logger.info('Adding taxi type column')
        df = df.with_columns(taxi_type = pl.lit(taxi_type))
        final_dfs.append(df)
        
    df_final = pl.concat(final_dfs)
    
    logger.info('Saving dataframe to csv')
    df_final.write_csv(output_filename)
    
    