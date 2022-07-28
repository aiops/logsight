import pytest

from logsight.connectors.base.adapter import Adapter, SinkAdapter, SourceAdapter
from logsight.connectors.builders.adapter_builder import AdapterBuilder
from logsight.connectors.builders.properties import AdapterConfigProperties, ConnectorConfigProperties
from logsight.connectors.serializers import DefaultSerializer
from logsight.connectors.sinks import PrintSink
from logsight.connectors.sources import StdinSource


@pytest.fixture
def builder():
    yield AdapterBuilder()


@pytest.fixture
def valid_adapter():
    yield AdapterConfigProperties(connector=ConnectorConfigProperties(connector_type="source", connection="stdin"))


def test_build(builder, valid_adapter):
    adapter = builder.build(valid_adapter)

    assert isinstance(adapter,SourceAdapter)
    assert isinstance(adapter.connector, StdinSource)
    assert isinstance(adapter.serializer, DefaultSerializer)


def test__get_adapter(builder):
    source = StdinSource()
    sink = PrintSink()

    assert issubclass(builder._get_adapter(source), SourceAdapter)
    assert issubclass(builder._get_adapter(sink), SinkAdapter)
    assert issubclass(builder._get_adapter(None), Adapter)
