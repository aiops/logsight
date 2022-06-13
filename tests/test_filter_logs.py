from datetime import datetime

import pytest

from analytics_core.logs import LogsightLog
from analytics_core.modules.filter.filter_logs import Filter


@pytest.fixture()
def infos():
    return [LogsightLog(metadata={"param_1": "test"}, timestamp=str(datetime.now()), message=f"info_{x}",
                        level="info") for x in range(10)]


@pytest.fixture()
def errors():
    return [LogsightLog(metadata={"param_2": "errs"}, timestamp=str(datetime.now()), message=f"error_{x}",
                        level="error") for x in range(10)]


@pytest.fixture()
def debugs():
    return [LogsightLog(metadata={"param_3": "debs"}, timestamp=str(datetime.now()), message=f"debug_{x}",
                        level="debug") for x in range(10)]


def test_filter_contains(infos, errors, debugs):
    logs = infos + errors + debugs

    _filter = Filter("metadata.param_1", "contains", "test")
    result = _filter.filter(logs)
    assert result == infos


def test_filter_equals(infos, errors, debugs):
    logs = infos + errors + debugs

    _filter = Filter("level", "equals", "info")
    result = _filter.filter(logs)
    assert result == infos
