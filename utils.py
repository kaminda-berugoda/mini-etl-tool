import logging

def get_logger(name: str ="mini etl",level: int = logging.INFO ) -> logging.Logger:
    logger = logging.getLogger(name) 

    if not logger.handlers:
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()

        fmt = logging.Formatter(
            "%(asctime)s %(levelname)s %(message)s"
        )
        handler.setFormatter(fmt)

        logger.addHandler(handler)

    return logger