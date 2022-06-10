from unittest.mock import MagicMock

import pytest

from analytics_core.logs import LogBatch, LogsightLog
from pipeline.modules import ForkModule


@pytest.fixture
def fork_module():
    return ForkModule()


@pytest.fixture
def test_input():
    return LogBatch(index="index", logs=[LogsightLog("message", "timestamp", "level")])


def test_process(fork_module, test_input):
    result = fork_module.process(test_input)
    assert result == test_input


def test__handle(fork_module, test_input):
    fork_module.process = MagicMock(side_effect=[test_input])
    result = fork_module._handle(test_input)
    assert result == test_input
    fork_module.process.assert_called_once_with(test_input)
