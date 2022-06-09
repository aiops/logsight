from datetime import datetime, timedelta
from unittest.mock import MagicMock, Mock, patch

import pytest

from results.common.index_job import IndexJob, IndexJobResult
from results.persistence.dto import IndexInterval
from services.elasticsearch.elasticsearch_service import ElasticsearchService
from services.service_provider import ServiceProvider
from tests.inputs import processed_logs


@pytest.fixture
@patch.multiple(IndexJob, __abstractmethods__=set())
def index_job():
    return IndexJob(IndexInterval("index", datetime.min, datetime.min + timedelta(hours=2)), index_ext="ext",
                    table_name="test")


def test__execute(index_job):
    index_job._perform_aggregation = MagicMock(side_effect=[True, False])
    result = index_job._execute()
    assert isinstance(result, IndexJobResult)
    assert result.table == index_job.table_name
    assert result.index_interval == index_job.index_interval


def test__perform_aggregation(index_job):
    index_job._load_data = MagicMock()
    index_job._load_data.side_effect = (processed_logs, [],)
    index_job._calculate = MagicMock()
    index_job._store_results = MagicMock()
    index_job._update_index_interval = MagicMock()

    result = index_job._perform_aggregation()

    assert result is True
    assert index_job._load_data.call_count == 1
    assert index_job._calculate.call_count == 1
    assert index_job._store_results.call_count == 1

    result2 = index_job._perform_aggregation()

    assert result2 is False
    assert index_job._load_data.call_count == 2
    assert index_job._calculate.call_count == 1
    assert index_job._store_results.call_count == 1


def test__update_index_interval(index_job):
    old_start = index_job.index_interval.start_date
    old_end = index_job.index_interval.end_date
    new_date = datetime.now()
    index_job._update_index_interval(new_date)
    assert index_job.index_interval.start_date != old_start
    assert index_job.index_interval.start_date == new_date
    assert index_job.index_interval.end_date != old_end


def test__load_data(index_job):
    es = ElasticsearchService("scheme", "host", 9201, "user", "password")
    es.connect = MagicMock()
    es.get_all_logs_for_index = MagicMock(return_value=processed_logs[:5])
    ServiceProvider.provide_elasticsearch = MagicMock(return_value=es)
    result = index_job._load_data('index', index_job.index_interval.start_date, index_job.index_interval.end_date)
    assert result == processed_logs[:5]


def test__store_results(index_job):
    es = ElasticsearchService("scheme", "host", 9201, "user", "password")
    es.connect = MagicMock()
    es.save = MagicMock()
    ServiceProvider.provide_elasticsearch = MagicMock(return_value=es)
    index_job._store_results(processed_logs[:4], "index")
    es.save.assert_called_once()
