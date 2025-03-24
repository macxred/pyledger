"""Module defining a class for capturing log messages."""

import logging
from typing import List


class LogCollector:
    """
    A class to collect log messages from a specific log channel
    at or above a specified level.
    """

    def __init__(self, channel: str, level: int):
        """
        Initializes the LogCollector.

        Args:
            channel (str): The name of the logger to collect messages from.
            level (int): The logging level threshold (e.g., logging.WARNING).
        """
        self._collected_logs: List[str] = []

        class CollectLogsHandler(logging.Handler):
            def emit(inner_self, record: logging.LogRecord) -> None:
                self._collected_logs.append(inner_self.format(record))

        handler = CollectLogsHandler()
        handler.setLevel(level)
        handler.setFormatter(logging.Formatter())
        logger = logging.getLogger(channel)
        logger.addHandler(handler)

    @property
    def logs(self) -> List[str]:
        """
        Retrieves the collected log messages.

        Returns:
            List[str]: The collected log messages.
        """
        return self._collected_logs
