### Log important events ###
import logging
from logging.handlers import RotatingFileHandler

# Function: Logger information
def get_logger(
    name: str,
    level: int = logging.INFO,
    log_file: str | None = None,
) -> logging.Logger:
    """
    Create and return a configured logger.

    Parameters
    ----------
    name : str
        Logger name (usually __name__)
    level : int
        Logging level (e.g. logging.INFO)
    log_file : str | None
        Optional log file path
    """

    logger = logging.getLogger(name)
    logger.setLevel(level)

    if logger.handlers:
        return logger  # avoid duplicate handlers

    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] %(name)s: %(message)s"
    )

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    if log_file:
        file_handler = RotatingFileHandler(
            log_file, maxBytes=5_000_000, backupCount=3
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger