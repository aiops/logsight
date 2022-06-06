from unittest.mock import MagicMock, patch

import pytest
from elasticsearch import ElasticsearchException, helpers

from services.elasticsearch.elasticsearch_service import ElasticsearchService
from tests.inputs import processed_logs


@pytest.fixture
def es():
    with patch('elasticsearch.helpers.bulk', return_value="Testing"):
        result = ElasticsearchService("host", "port", "username", "password")
        yield result


@pytest.fixture
def es_exception():
    with patch('elasticsearch.helpers.bulk', side_effect=ElasticsearchException()):
        result = ElasticsearchService("host", "port", "username", "password")
        yield result


def get_es_res(data):
    return {"hits": {"hits": [{"_source": x} for x in data]}}


def test_get_all_logs_for_index(es):
    es.es.search = MagicMock(return_value=get_es_res(processed_logs))
    result = es.get_all_logs_for_index("index", "start_time", "end_time")
    assert result == processed_logs


def test_get_all_indices(es):
    n_indices = 5
    indices = dict(((f"idx_{i}", f"idx_{i}") for i in range(n_indices)))
    es.es.indices.get = MagicMock(return_value=indices)
    result = es.get_all_indices("index")
    assert result == list(indices.keys())


def test_save(es):
    es.save(processed_logs, "index")


def test_save_string(es):
    helpers.bulk = MagicMock()
    es.save("string", "index")
    helpers.bulk.assert_called_once()


def test_es_raises_exc(es_exception):
    pytest.raises(ElasticsearchException, es_exception.save, "data", "index")


def test_enter_exit(es):
    es.es.ping = MagicMock(return_value=True)
    es.es.close = MagicMock()
    with es as e:
        assert e.es.ping() is True

    e.es.close.assert_called_once()
