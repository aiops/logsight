from unittest.mock import MagicMock, patch

import pytest
from elasticsearch import helpers

from connectors.connectors.elasticsearch import ElasticsearchConfigProperties
from connectors.connectors.elasticsearch.connector import ElasticsearchException
from logsight.tests.inputs import processed_logs
from services.elasticsearch_service.elasticsearch_service import ElasticsearchService


@pytest.fixture
def es_config():
    return ElasticsearchConfigProperties(scheme="scheme", host="host", port=9201, username="username",
                                         password="password")


@pytest.fixture
def es(es_config):
    with patch('elasticsearch.helpers.bulk', return_value="Testing"):
        result = ElasticsearchService(es_config)
        yield result


@pytest.fixture
def es_exception(es_config):
    with patch('elasticsearch.helpers.bulk', side_effect=ElasticsearchException()):
        result = ElasticsearchService(es_config)
        yield result


def get_es_res(data):
    return {"hits": {"hits": [{"_source": x} for x in data]}}


def get_agg_res(data):
    return {"aggregations": {"aggregations": {"buckets": data}}}


def test_get_all_logs_for_index(es):
    es.es.search = MagicMock(return_value=get_es_res(processed_logs))
    result = es.get_all_logs_for_index("index", "start_time", "end_time")
    assert result == processed_logs


def test_all_templates_for_index(es):
    es.es.search = MagicMock(return_value=get_agg_res([]))
    result = es.get_all_templates_for_index("index")
    assert result == []


def test_get_all_logs_after_ingest(es):
    es.es.search = MagicMock(return_value=get_es_res(processed_logs))
    result = es.get_all_logs_after_ingest("index", "start_time", "end_time")
    assert result == processed_logs


def test_get_all_indices(es):
    n_indices = 5
    indices = dict(((f"idx_{i}", f"idx_{i}") for i in range(n_indices)))
    es.es.indices.get = MagicMock(return_value=indices)
    result = es.get_all_indices("index")
    assert result == list(indices.keys())


def test_save(es):
    es.save(processed_logs, "index")


def test_delete_by_ingest_timestamp(es):
    es.es.delete_by_query = MagicMock()
    es.delete_by_ingest_timestamp("index", "now-15m", "now")
    es.es.delete_by_query.assert_called_once()


def test_delete_logs_for_index(es):
    es.es.delete_by_query = MagicMock()
    es.delete_logs_for_index("index", "now-15m", "now")
    es.es.delete_by_query.assert_called_once()


def test_save_string(es):
    helpers.bulk = MagicMock()
    es.save("string", "index", False)
    helpers.bulk.assert_called_once()


def test_es_raises_exc(es_exception):
    pytest.raises(ElasticsearchException, es_exception.save, "data", "index")


def test_enter_exit(es):
    es.es.ping = MagicMock(return_value=True)
    es._create_timestamp_pipeline = MagicMock(return_value=True)
    es.es.close = MagicMock()
    with es as e:
        assert e.es.ping() is True

    e.es.close.assert_called_once()
    es._create_timestamp_pipeline.assert_called_once()
