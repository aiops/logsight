import copy
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import dateutil.parser
import pytest

from configs.global_vars import PIPELINE_INDEX_EXT
from results.common.index_job import IndexJob, IndexJobResult
from results.persistence.dto import IndexInterval
from results.persistence.timestamp_storage import PostgresTimestampStorage, TimestampStorageProvider
from services.elasticsearch.elasticsearch_service import ElasticsearchService
from services.service_provider import ServiceProvider
from tests.inputs import processed_logs


@pytest.fixture
@patch.multiple(IndexJob, __abstractmethods__=set())
def index_job():
    return IndexJob(IndexInterval("index", datetime.min, datetime.min + timedelta(hours=2)), index_ext="ext",
                    table_name="test")


@pytest.fixture
def db():
    db = PostgresTimestampStorage("table", "host", "9000", "username", "password", "db_name")
    db.connect = MagicMock()
    db.update_timestamps = MagicMock()
    db.close = MagicMock()
    return db


def test__execute(index_job, db):
    TimestampStorageProvider.provide_timestamp_storage = MagicMock(return_value=db)
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
    latest_processed_time = index_job.index_interval.latest_processed_time
    latest_ingest_time = index_job.index_interval.latest_ingest_time
    index_job._update_index_interval(latest_processed_time, latest_ingest_time)
    assert index_job.index_interval.latest_processed_time == latest_processed_time + timedelta(milliseconds=1)
    assert index_job.index_interval.latest_ingest_time == latest_ingest_time + timedelta(milliseconds=1)


def test__load_data_new_entries(index_job):
    es = ElasticsearchService("scheme", "host", 9201, "user", "password")
    es._connect = MagicMock()
    es.get_all_logs_for_index = MagicMock(return_value=processed_logs[:5])
    es.get_all_logs_after_ingest = MagicMock(return_value=processed_logs[:5])
    ServiceProvider.provide_elasticsearch = MagicMock(return_value=es)
    result = index_job._load_data('index', index_job.index_interval.latest_ingest_time,
                                  index_job.index_interval.latest_processed_time)
    assert result == processed_logs[:5]


def update_timestamp(log):
    log['ingest_timestamp'] = str(dateutil.parser.isoparse(log['ingest_timestamp']) + timedelta(hours=1))
    return log


def test__load_data_historical_entries(index_job):
    # mock functions
    es = ElasticsearchService("scheme", "host", 9201, "user", "password")
    es.connect = MagicMock()
    ServiceProvider.provide_elasticsearch = MagicMock(return_value=es)

    # update ingest time
    logs_copy = copy.deepcopy(processed_logs)
    new_logs = [update_timestamp(log) for log in logs_copy]

    # Index interval should have old processed times
    index_job.index_interval.latest_ingest_time = dateutil.parser.isoparse(processed_logs[-1]['ingest_timestamp'])
    index_job.index_interval.latest_processed_time = dateutil.parser.isoparse(processed_logs[-1]['timestamp'])

    es.get_all_logs_after_ingest = MagicMock(return_value=new_logs)
    es.get_all_logs_for_index = MagicMock(return_value=new_logs + processed_logs)
    # params
    index = "index"
    lit = index_job.index_interval.latest_ingest_time
    lpt = index_job.index_interval.latest_processed_time

    # when
    result = index_job._load_data(index, lit, lpt)

    # then
    index_res = "_".join([index, PIPELINE_INDEX_EXT])
    es.get_all_logs_after_ingest.assert_called_once_with(index_res, str(lit.isoformat()))
    # assert that the data collected is in the interval from the new updated logs
    es.get_all_logs_for_index.assert_called_once_with(index_res, new_logs[0]['timestamp'], new_logs[-1]['timestamp'])
    # assert that es reads the combined data
    assert result == new_logs + processed_logs


def test__store_results(index_job):
    es = ElasticsearchService("scheme", "host", 9201, "user", "password")
    es.connect = MagicMock()
    es.save = MagicMock()
    es.delete_logs_for_index = MagicMock()
    ServiceProvider.provide_elasticsearch = MagicMock(return_value=es)
    index_job._store_results(processed_logs[:4], "index")
    es.save.assert_called_once()
