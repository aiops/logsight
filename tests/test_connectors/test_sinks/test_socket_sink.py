import builtins
import os
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest

from logsight.connectors.connectors.socket import SocketConfigProperties
from logsight.connectors.sinks import SocketSink


@pytest.fixture
def socket_sink():
    yield SocketSink(SocketConfigProperties())


def test_connect(socket_sink):
    socket_sink.socket = MagicMock()
    socket_sink.socket.connect = MagicMock()
    socket_sink._connect()

    assert socket_sink.connected is True
    socket_sink.socket.connect.assert_called_once()


def test_already_connect(socket_sink):
    socket_sink.socket = MagicMock()
    socket_sink.socket.connect = MagicMock()
    socket_sink.connected = True
    socket_sink._connect()

    assert socket_sink.connected is True
    assert socket_sink.socket.connect.call_count == 0


def test_exc_connect(socket_sink):
    socket_sink.socket = MagicMock()
    socket_sink.socket.connect = MagicMock(side_effect=Exception("test"))
    with pytest.raises(Exception):
        socket_sink._connect()


def test_send(socket_sink):
    test_input = "data"
    socket_sink.socket = MagicMock()
    socket_sink.socket.connect = MagicMock()
    socket_sink._connect()
    socket_sink.socket.sendall = MagicMock()
    socket_sink.send(test_input)
    socket_sink.socket.sendall.assert_called_once_with(bytes(test_input, 'utf-8'))
