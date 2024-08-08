"""
This module is used to configure the logger
"""

import logging
import os
from datetime import datetime


# pylint: disable=logging-fstring-interpolation
class DailyLogger:
    """
    This class is used to create a logger object that will be used to log
    """

    def __init__(self, log_folder):
        """
        This method is used to initialize the DailyLogger class
        :param log_folder:  The folder where the logs will be stored
        """
        self.log_folder = log_folder
        os.makedirs(self.log_folder, exist_ok=True)

    def configure_logger(self, logger_name, log_file, enable_console=False):
        """
        This method is used to configure the logger
        :param logger_name: The name of the logger
        :param log_file: The name of the log file
        :return:
        """
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.DEBUG)
        log_path = os.path.join(self.log_folder, log_file)
        handler = logging.FileHandler(log_path)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        # console handler
        # Optionally add a console handler
        if enable_console:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.DEBUG)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        return logger


# Create log folder for the current day if it doesn't exist
current_date = datetime.now().strftime("%Y-%m-%d")
log_folder_name = os.path.join("logs", current_date)


# Create an instance of the DailyLogger class
logger_object = DailyLogger(log_folder_name)

# Configure logger for API logs
request_logger = logger_object.configure_logger("request_logger", "request.log")
api_logger = logger_object.configure_logger(
    "api_logger", "api.log", enable_console=True
)


def api_log(level=logging.DEBUG, msg=""):
    """
    This function is used to log messages to the API log file
    :param level: The log level
    :param msg: The message to log
    :return:
    """
    api_logger.log(level, msg)


def request_log(level=logging.DEBUG, msg=""):
    """
    This function is used to log messages to the request log file
    :param level: The log level
    :param msg: The message to log
    :return:
    """
    request_logger.log(level, msg)
