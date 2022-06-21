import os

import pytest

from common.logsight_classes.configs import ConnectionConfigProperties
from connectors import Connector, Sink, Source
from connectors.connection_builder import ConnectionBuilder


@pytest.fixture(params=['name1', 'name2', 'name3'])
def invalid_names(request):
    yield ConnectionConfigProperties(request.param, "elasticsearch")


@pytest.fixture(ids=["file", "kafka", "socket", "stdin", "zeromq"],
                params=[("file", "FileSource", {"path": os.path.abspath(__file__)}),
                        ("kafka", "KafkaSource", {"topic": "src"}),
                        ("socket", "SocketSource", {}),
                        ("stdin", "StdinSource", {}),
                        ("zeromq", "ZeroMQSubSource", {"endpoint": "localhost:9200"})])
def valid_source(request):
    yield ConnectionConfigProperties(connection=request.param[0], classname=request.param[1], params=request.param[2])


@pytest.fixture(ids=["elasticsearch", "file", "kafka", "socket", "stdin", "zeromq"],
                params=[("elasticsearch", "ElasticsearchSink", {}),
                        ("file", "FileSink", {"path": "./test_connection_builder.py"}),
                        ("kafka", "KafkaSink", {"topic": "src"}),
                        ("socket", "SocketSink", {}),
                        ("print", "PrintSink", {}),
                        ("zeromq", "ZeroMQPubSink", {"endpoint": "localhost:9200"})])
def valid_sink(request):
    yield ConnectionConfigProperties(connection=request.param[0], classname=request.param[1], params=request.param[2])


def test_build_invalid_name(invalid_names):
    """Successfully builds a new connection based on config"""
    builder = ConnectionBuilder()
    pytest.raises(AttributeError, builder.build, invalid_names)


def test_build_valid_sources(valid_source):
    """Successfully builds a new connection based on config"""
    builder = ConnectionBuilder()
    connection = builder.build(valid_source)
    assert connection.__class__.__name__ == valid_source.classname
    assert isinstance(connection, Source)
    if hasattr(connection, 'connect') and hasattr(connection, 'close'):
        assert isinstance(connection, Connector)


def test_build_valid_sinks(valid_sink):
    """Successfully builds a new connection based on config"""
    builder = ConnectionBuilder()
    connection = builder.build(valid_sink)
    assert connection.__class__.__name__ == valid_sink.classname
    assert isinstance(connection, Sink)
    if hasattr(connection, 'connect') and hasattr(connection, 'close'):
        assert isinstance(connection, Connector)
