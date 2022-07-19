from unittest import mock

import pytest

from common.patterns.job import Job


@pytest.fixture
def job():
    class ExampleJob(Job):
        def _execute(self):
            return True

    def notification_callback(result):
        return "notification" + str(result)

    def done_callback(result):
        return "done" + str(result)

    def error_callback(result):
        return "error" + str(result)

    job = ExampleJob(notification_callback=notification_callback, done_callback=done_callback,
                     error_callback=error_callback, name="test")
    yield job


def test_id(job):
    assert job.id == job._id


def test_name(job):
    assert job.name == "test"


def test_execute(job):
    job_mocked = mock.Mock(wraps=job)
    result = job_mocked.execute()
    assert result is True


def test__send_notification(job):
    args = "test"
    result = job._send_notification(args)
    assert "notification" + args == result


def test__send_done(job):
    args = "test"
    result = job._send_done(args)
    expected = "done" + args
    assert expected == result


def test__send_error(job):
    args = "test"
    result = job._send_error(args)
    assert "error" + args == result
