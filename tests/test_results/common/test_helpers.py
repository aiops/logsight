from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest

from results.common.helpers import update_status
from results.common.index_job import IndexJobResult
from results.persistence.dto import IndexInterval
from results.persistence.timestamp_storage import PostgresTimestampStorage, TimestampStorageProvider


@pytest.fixture
def job_result():
    return IndexJobResult(IndexInterval("index", datetime.min, datetime.min + timedelta(hours=2)), "table")


def test_update_status(job_result):
    TimestampStorageProvider.provide_timestamp_storage = MagicMock()
    result = update_status(job_result)
    TimestampStorageProvider.provide_timestamp_storage.assert_called_once()
    assert result.index_interval == job_result.index_interval
