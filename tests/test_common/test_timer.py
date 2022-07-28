from unittest.mock import MagicMock

import pytest

from logsight.common.patterns.timer import NamedTimer


@pytest.fixture
def callback():
    return "done"


@pytest.fixture
def timer(callback):
    return NamedTimer(callback=callback, timeout_period=2, name="test")


def test_start(timer):
    timer.timer.start = MagicMock()
    timer = timer.start()

    timer.timer.start.assert_called_once()


def test_reset_timer(timer):
    timer.start = MagicMock()
    old_id = id(timer.timer)
    old_timer = timer.timer
    timer.reset_timer()
    assert old_id != id(timer.timer)
    assert old_timer != timer.timer
    assert timer.timer.name == timer.name
    timer.start.assert_called_once()
    timer.reset_timer()


def test_cancel(timer):
    old_id = id(timer.timer)
    old_timer = timer.timer
    timer.cancel()
    assert old_id != id(timer.timer)
    assert old_timer != timer.timer
    assert timer.timer.name == timer.name
