"""Module defining a class for capturing log messages."""

import logging
from typing import List


class LogCollector:
    """
    A class to collect log messages from a specific log channel
    at or above a specified level.
    """

    def __init__(self, channel: str, level: int, keep_console_output: bool = False):
        """
        Initializes the LogCollector.

        Args:
            channel (str): The name of the logger to collect messages from.
            level (int): The logging level threshold (e.g., logging.WARNING).
            keep_console_output (bool): If True, ensure that the logger's
                default behavior to print log messages to the console remains
                unchanged by adding a console handler if no other handlers are
                registered.
        """
        self._collected_logs: List[str] = []

        class CollectLogsHandler(logging.Handler):
            def emit(inner_self, record: logging.LogRecord) -> None:
                self._collected_logs.append(inner_self.format(record))

        handler = CollectLogsHandler()
        handler.setLevel(level)
        handler.setFormatter(logging.Formatter())

        logger = logging.getLogger(channel)

        if keep_console_output and not logger.hasHandlers():
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(logging.Formatter())
            logger.addHandler(console_handler)

        logger.addHandler(handler)

    @property
    def logs(self) -> List[str]:
        """
        Retrieves the collected log messages.

        Returns:
            List[str]: The collected log messages.
        """
        return self._collected_logs
