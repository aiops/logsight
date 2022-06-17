from unittest.mock import MagicMock

import pytest

from analytics_core.logs import LogBatch, LogsightLog
from common.patterns.chain_of_responsibility import AbstractHandler, ForkHandler


@pytest.fixture
def log_batch():
    yield LogBatch([LogsightLog("test", "test", "test")], "test")


@pytest.fixture
def handler(log_batch):
    class TestHandler(AbstractHandler):
        def _handle(self, context: LogBatch) -> LogBatch:
            return log_batch

    yield TestHandler()


@pytest.fixture
def fork_handler(log_batch):
    class TestForkHandler(ForkHandler):
        def _handle(self, context: LogBatch) -> LogBatch:
            return log_batch

    yield TestForkHandler()


def test_set_next(handler, fork_handler):
    next_handler = fork_handler
    handler.set_next(next_handler)
    assert handler.next_handler == next_handler


def test_set_next_fork(handler, fork_handler):
    next_handler = handler
    fork_handler.set_next(next_handler)
    assert len(fork_handler.next_handler) == 1
    assert fork_handler.next_handler[0] == next_handler


def test_handle(handler, fork_handler, log_batch):
    handler.set_next(fork_handler)
    fork_handler.handle = MagicMock(return_value=log_batch)
    result = handler.handle(log_batch)

    assert result == log_batch
    fork_handler.handle.assert_called_once()


def test_handle_no_context(handler, fork_handler):
    handler._handle = MagicMock()

    result = handler.handle(None)

    assert result is None
    handler._handle.assert_not_called()


def test_handle_no_next(handler, log_batch):
    result = handler.handle(log_batch)
    assert result == log_batch


def test_handle_fork(handler, fork_handler, log_batch):
    next_handler = handler
    fork_handler.set_next(next_handler)

    result = fork_handler.handle(log_batch)

    assert isinstance(result, list)
    assert result[0] == log_batch


def test_fork_handle_no_next(fork_handler, log_batch):
    result = fork_handler.handle(log_batch)
    assert result == log_batch
