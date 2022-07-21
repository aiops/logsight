from concurrent.futures import Future
from unittest.mock import MagicMock

import pytest

from common.patterns.job import Job
from common.patterns.job_manager import JobManager


@pytest.fixture
def job_manager():
    manager = JobManager()
    manager.pool.submit = MagicMock(return_value=Future())
    yield manager


@pytest.fixture
def test_job():
    class TestJob(Job):
        def _execute(self):
            return True

    return TestJob(print, print, print)


def test_submit_job(job_manager, test_job):
    result = job_manager.submit_job(test_job)
    job_manager.pool.submit.assert_called_once()
    assert isinstance(result, Future)
