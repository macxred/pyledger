"""Test suite for LogCollector class."""

import logging
import pytest
from pyledger import LogCollector


@pytest.mark.parametrize(
    "log_level, messages, expected_logs",
    [
        (
            logging.INFO,
            [(logging.DEBUG, "This should NOT be collected"),
             (logging.INFO, "This should be collected"),
             (logging.WARNING, "Warning message")],
            ["This should be collected", "Warning message"],
        ),
        (
            logging.WARNING,
            [(logging.INFO, "This should NOT be collected"),
             (logging.WARNING, "This should be collected"),
             (logging.ERROR, "Error message")],
            ["This should be collected", "Error message"],
        ),
        (
            logging.ERROR,
            [(logging.DEBUG, "Debug message"),
             (logging.INFO, "Info message"),
             (logging.WARNING, "Warning message")],
            [],
        ),
    ],
)
def test_log_collector_respects_logging_levels(log_level, messages, expected_logs):
    logger = logging.getLogger("test_logger")
    logger.setLevel(logging.DEBUG)
    collector = LogCollector(logger.name, log_level)

    for level, msg in messages:
        logger.log(level, msg)

    assert collector.logs == expected_logs


@pytest.mark.parametrize(
    "level1, level2, messages, expected_logs1, expected_logs2",
    [
        (
            logging.INFO,
            logging.WARNING,
            [
                (logging.DEBUG, "Debug message"),
                (logging.INFO, "Info message"),
                (logging.WARNING, "Warning message"),
                (logging.ERROR, "Error message")
            ],
            ["Info message", "Warning message", "Error message"],
            ["Warning message", "Error message"],
        ),
        (
            logging.DEBUG,
            logging.ERROR,
            [
                (logging.DEBUG, "Debug message"),
                (logging.INFO, "Info message"),
                (logging.WARNING, "Warning message"),
                (logging.ERROR, "Error message")
            ],
            ["Debug message", "Info message", "Warning message", "Error message"],
            ["Error message"],
        ),
    ],
)
def test_log_collector_handles_multiple_instances(
    level1, level2, messages, expected_logs1, expected_logs2
):
    logger = logging.getLogger("test_logger_multi")
    logger.setLevel(logging.DEBUG)
    collector1 = LogCollector(logger.name, level1)
    collector2 = LogCollector(logger.name, level2)

    for level, msg in messages:
        logger.log(level, msg)

    assert collector1.logs == expected_logs1
    assert collector2.logs == expected_logs2


def test_log_collector_with_console_output(caplog):
    logger = logging.getLogger("test_logger_console")
    logger.setLevel(logging.DEBUG)
    collector = LogCollector(channel=logger.name, level=logging.INFO, keep_console_output=True)
    with caplog.at_level(logging.INFO):
        logger.info("Collected and printed")
        logger.warning("Warning also collected")

    assert collector.logs == ["Collected and printed", "Warning also collected"]
    assert "Collected and printed" in caplog.text
    assert "Warning also collected" in caplog.text


def test_log_collector_adds_console_handler_if_no_handlers(monkeypatch):
    logger_name = "test_logger_no_handlers"
    logger = logging.getLogger(logger_name)
    logger.handlers.clear()
    logger.propagate = False
    logger.setLevel(logging.DEBUG)
    assert not logger.hasHandlers(), "Logger should start without handlers"

    # Prevent log messages from appearing in the terminal while still allowing capture
    monkeypatch.setattr(logging.StreamHandler, "emit", lambda self, record: None)

    collector = LogCollector(channel=logger_name, level=logging.DEBUG, keep_console_output=True)
    assert any(isinstance(handler, logging.StreamHandler) for handler in logger.handlers), (
        "LogCollector should have added a StreamHandler"
    )
    logger.debug("This should be collected and printed")
    assert "This should be collected and printed" in collector.logs
