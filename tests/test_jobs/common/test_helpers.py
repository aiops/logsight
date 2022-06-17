from datetime import datetime, timedelta
import pytest

from jobs.common.index_job import IndexJobResult
from jobs.persistence.dto import IndexInterval


@pytest.fixture
def job_result():
    return IndexJobResult(IndexInterval("index", datetime.min, datetime.min + timedelta(hours=2)), "table")
