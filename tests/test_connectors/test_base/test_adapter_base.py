from unittest.mock import MagicMock

from logsight.connectors.base.adapter import SinkAdapter, SourceAdapter


def test_source_adapter():
    value = "result"
    adapter = SourceAdapter()
    adapter.connector = MagicMock()
    adapter.connector.receive_message = MagicMock(return_value=value)
    adapter.serializer = MagicMock()
    adapter.serializer.deserialize = MagicMock(return_value=value)

    result = adapter.receive()
    assert result == value
    adapter.serializer.deserialize.assert_called_once_with(value)
    adapter.connector.receive_message.assert_called_once()


def test_sink_adapter():
    adapter = SinkAdapter()
    value = "result"

    adapter.serializer = MagicMock()
    adapter.serializer.serialize = MagicMock(return_value=value)
    adapter.connector = MagicMock()
    adapter.connector.send = MagicMock(return_value=value)

    adapter.send(value)
    adapter.serializer.serialize.assert_called_once_with(value)
    adapter.connector.send.assert_called_once_with(value, None)
