from unittest.mock import MagicMock

import pytest

from logsight.connectors import Connectable
from logsight.connectors.base.mixins import ConnectableSink
from logsight.connectors.connectors.elasticsearch import ElasticsearchConfigProperties, ElasticsearchConnector
from logsight.connectors.sinks import ElasticsearchSink


@pytest.fixture
def es_config():
    return ElasticsearchConfigProperties()


def test_es_sink(es_config):
    sink = ElasticsearchSink(es_config)
    sink.parallel_bulk = MagicMock()

    sink.send(["data"], "target")

    sink.parallel_bulk.assert_called_once()

    assert isinstance(sink, ConnectableSink)
    assert isinstance(sink, ElasticsearchConnector)
    assert isinstance(sink, Connectable)
