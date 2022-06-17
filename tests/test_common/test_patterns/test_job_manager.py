from concurrent.futures import Future
from unittest.mock import MagicMock

import pytest

from common.patterns.job_manager import JobManager
from jobs.jobs.delete_es_data_job import DeleteESDataJob


@pytest.fixture
def job_manager():
    manager = JobManager()
    manager.pool.submit = MagicMock(return_value=Future())
    yield manager


def test_submit_job(job_manager):
    job = DeleteESDataJob()
    result = job_manager.submit_job(job)
    job_manager.pool.submit.assert_called_once()
    assert isinstance(result, Future)
