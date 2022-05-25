from datetime import datetime
from unittest.mock import MagicMock

import pytest

from results.common.factory import JobDispatcherFactory
from results.persistence.dto import IndexInterval
from tests.utils import random_times


@pytest.fixture
def job_dispatcher():
    return JobDispatcherFactory.get_log_agg_dispatcher(2, 10)


def get_index_intervals(n_intervals):
    return [IndexInterval("index", *random_times("2020-01-01 00:00:00", "2022-01-01 00:00:00", 2)) for _ in
            range(n_intervals)]


def test_submit_job(job_dispatcher):
    n_intervals = 5
    index_intervals = get_index_intervals(n_intervals)
    job_dispatcher.storage = MagicMock()
    job_dispatcher.timer = MagicMock()
    job_dispatcher.sync_index = MagicMock()
    job_dispatcher.manager.submit_job = MagicMock(side_effect=None)
    job_dispatcher.manager.pool = MagicMock(side_effect=[])
    job_dispatcher.storage.__table__ = MagicMock()
    job_dispatcher.storage.get_all = MagicMock(side_effect=[index_intervals])
    job_dispatcher.submit_job()
    assert job_dispatcher.manager.submit_job.call_count == n_intervals


def test_sync_index(job_dispatcher):
    index_intervals = [f"index_{i}" for i in range(5)]
    current = index_intervals[2:]
    idx = set(index_intervals).difference(set(current))

    job_dispatcher.storage = MagicMock()
    job_dispatcher.storage.update_timestamps = MagicMock()
    job_dispatcher.storage.select_all_application_index = MagicMock(side_effect=[index_intervals])
    job_dispatcher.storage.select_all_index = MagicMock(side_effect=[current])

    job_dispatcher.sync_index()

    assert job_dispatcher.storage.update_timestamps.call_count == len(idx)


def test_start(job_dispatcher):
    job_dispatcher.timer.start = MagicMock()
    job_dispatcher.start()
    job_dispatcher.timer.start.assert_called()
