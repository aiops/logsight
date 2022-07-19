import os

import pytest

from common.logsight_classes.configs import AdapterConfigProperties, ConnectorConfigProperties
from connectors import ConnectableConnector, Sink, Source
from connectors.builders.adapter_builder import AdapterBuilder
from connectors.builders.connector_builder import ConnectorBuilder
from pipeline.builders.adapter_builder import PipelineAdapterBuilder
from pipeline.ports.pipeline_adapters import PipelineSourceAdapter


@pytest.fixture(params=['name1', 'name2', 'name3'])
def invalid_names(request):
    yield ConnectorConfigProperties(connection=request.param, connector_type="sink")


@pytest.fixture(ids=["file", "kafka", "socket", "stdin", "zeromq"],
                params=[("file", "source", {"path": os.path.abspath(__file__)}),
                        ("kafka", "source", {"topic": "src"}),
                        ("socket", "source", {}),
                        ("stdin", "source", {}),
                        ("zeromq", "source", {"endpoint": "localhost:9200"})])
def valid_source(request):
    yield ConnectorConfigProperties(connection=request.param[0], connector_type=request.param[1],
                                    params=request.param[2])


@pytest.fixture(ids=["elasticsearch", "file", "kafka", "socket", "stdin", "zeromq"],
                params=[("elasticsearch", "sink", {}),
                        ("file", "sink", {"path": "./test_connection_builder.py"}),
                        ("kafka", "sink", {"topic": "src"}),
                        ("socket", "sink", {}),
                        ("stdout", "sink", {}),
                        ("zeromq", "sink", {"endpoint": "localhost:9200"})])
def valid_sink(request):
    yield ConnectorConfigProperties(connection=request.param[0], connector_type=request.param[1],
                                    params=request.param[2])


def test_build_invalid_name(invalid_names):
    """Successfully builds a new connection based on config"""
    builder = ConnectorBuilder()
    pytest.raises(KeyError, builder.build, invalid_names)


def test_build_valid_sources(valid_source):
    """Successfully builds a new connection based on config"""
    builder = ConnectorBuilder()
    connection = builder.build(valid_source)
    assert isinstance(connection, Source)
    if hasattr(connection, 'connect') and hasattr(connection, 'close'):
        assert isinstance(connection, ConnectableConnector)


def test_build_valid_sinks(valid_sink):
    """Successfully builds a new connection based on config"""
    builder = ConnectorBuilder()
    connection = builder.build(valid_sink)
    assert isinstance(connection, Sink)
    if hasattr(connection, 'connect') and hasattr(connection, 'close'):
        assert isinstance(connection, ConnectableConnector)


def test_build_pipeline_source(valid_source):
    adapter_config = AdapterConfigProperties(connector=valid_source)
    builder = PipelineAdapterBuilder()
    connection = builder.build(adapter_config)

    assert isinstance(connection, PipelineSourceAdapter)
    if hasattr(connection, 'connect') and hasattr(connection, 'close'):
        assert isinstance(connection, ConnectableConnector)
