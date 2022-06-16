from concurrent.futures import Future
from unittest.mock import MagicMock

import pytest

from common.patterns.concurrent_job_manager import NewJobManager
from common.patterns.job import Job


@pytest.fixture
def job_manager():
    return NewJobManager()


@pytest.fixture
def job():
    class TestJob(Job):
        def _execute(self):
            return True

    job = TestJob(name="test")
    yield job


def test_submit_job(job_manager, job):
    job_manager._start_job = MagicMock()
    job_manager.submit_job(job)

    assert len(job_manager.job_queue[job.name]) == 1
    assert "test" in job_manager.job_queue
    job_manager._start_job.assert_called_once_with(job.name)


def test__start_job_not_running(job_manager, job):
    job_manager.pool.submit = MagicMock(return_value=Future())
    job_manager.job_queue[job.name].append(job)

    # when
    job_manager._start_job(job.name)
    # then
    assert len(job_manager.job_queue[job.name]) == 0
    assert "test" in job_manager.running_jobs
    job_manager.pool.submit.assert_called_once_with(job.execute)


def test__start_job_running(job_manager, job):
    job_manager.pool.submit = MagicMock(return_value=Future())
    job_manager.running_jobs.add(job.name)

    # when
    job_manager._start_job(job.name)
    # then
    assert len(job_manager.job_queue[job.name]) == 0
    job_manager.pool.submit.assert_not_called()


def test__job_complete(job_manager, job):
    job_manager._start_job = MagicMock()
    job_manager.running_jobs.add(job.name)
    expected = "result"

    # when
    result = job_manager._job_complete(job.name, expected)

    assert len(job_manager.running_jobs) == 0
    assert expected == result
    job_manager._start_job.assert_called_once_with(job.name)
