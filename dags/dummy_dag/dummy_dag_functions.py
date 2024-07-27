def extract(
    url: str,
    output_filename: str,
    logical_timestamp: "pendulum.datetime",
    config: dict,
    historical: bool,
) -> int:
    """_summary_

    Args:
        url (str): _description_
        output_filename (str): _description_
    """
    import logging

    logger = logging.getLogger('extract')

    logger.info(url)
    logger.info(output_filename)
    logger.info(logical_timestamp)
    logger.info(config)
    logger.info(historical)

    with open(output_filename, 'w') as f:
        f.write('Testing')

    return 1

def transform(input_filename: str, output_filename: str) -> int:
    """_summary_

    Args:
        input_filename (str): _description_
        output_filename (str): _description_
    """
    import logging

    logger = logging.getLogger('transform')

    logger.info(input_filename)
    logger.info(output_filename)

    with open(input_filename, 'r') as f:
        data = f.read()

    logger.info(data)

    return 1