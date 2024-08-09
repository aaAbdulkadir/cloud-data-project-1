import logging
import pandas as pd
import pendulum

def extract(
    url: str,
    output_filename: str,
    logical_timestamp: pendulum.DateTime,
    config: dict,
    params: dict
) -> None:
    """Dummy extract for testing purposes.

    Args:
        url (str): URL from configuration.
        output_filename (str): Output filename.
        logical_timestamp (pendulum.DateTime): Logical timestamp for the operation.
        config (Dict): Configuration dictionary.
    """
    logger = logging.getLogger('extract')

    logger.info(url)
    logger.info(output_filename)
    logger.info(logical_timestamp)
    logger.info(config)
    logger.info(params)

    with open(output_filename, 'w') as f:
        f.write('Testing,1,animal')

def extract_2(
    url: str,
    output_filename: str,
    params: dict
) -> None:
    """Dummy extract for testing purposes.

    Args:
        url (str): URL from configuration.
        output_filename (str): Output filename.
    """
    logger = logging.getLogger('extract_2')

    logger.info(url)
    logger.info(output_filename)
    logger.info(params)

    with open(output_filename, 'w') as f:
        f.write('Testing,1,animal')

def transform(input_filename: str, output_filename: str) -> None:
    """Dummy transform for testing purposes.

    Args:
        input_filename (str): Input filename for extract output.
        output_filename (str): File to load to.
    """
    logger = logging.getLogger('transform')

    logger.info(input_filename)
    logger.info(output_filename)

    with open(input_filename, 'r') as f:
        data = f.read()

    logger.info(data)

    rows = [data.split(',')]
    df = pd.DataFrame(rows, columns=['Column1', 'Column2', 'Column3'])

    df.to_csv(output_filename, index=False)
