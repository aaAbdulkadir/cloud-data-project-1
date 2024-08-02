def extract(
    url: str,
    output_filename: str,
    logical_timestamp: "pendulum.datetime",
    config: dict,
) -> int:
    """Dummy extract for testing purposes.

    Args:
        url (str): url from yml
        output_filename (str): output filename
    """
    import logging

    logger = logging.getLogger('extract')

    logger.info(url)
    logger.info(output_filename)
    logger.info(logical_timestamp)
    logger.info(config)
    logger.info(historical)

    with open(output_filename, 'w') as f:
        f.write('Testing,1,animal')


def transform(input_filename: str, output_filename: str) -> int:
    """Dummy transform for testing purposes.

    Args:
        input_filename (str): input filename for extract output
        output_filename (str): file to load to
    """
    import logging
    import pandas as pd

    logger = logging.getLogger('transform')

    logger.info(input_filename)
    logger.info(output_filename)

    with open(input_filename, 'r') as f:
        data = f.read()

    logger.info(data)

    rows = [data.split(',')]
    df = pd.DataFrame(rows, columns=['Column1', 'Column2', 'Column3'])

    df.to_csv(output_filename, index=False)

