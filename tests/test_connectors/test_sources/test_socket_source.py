from unittest.mock import MagicMock

import pytest

from logsight.connectors.connectors.socket import SocketConfigProperties
from logsight.connectors.sources import SocketSource


@pytest.fixture
def socket_source():
    yield SocketSource(SocketConfigProperties())


def test_connect(socket_source):
    socket_source.socket = MagicMock()
    socket_source.socket.bind = MagicMock()
    socket_source.socket.listen = MagicMock()
    socket_source._connect()

    assert socket_source.connected is True
    socket_source.socket.bind.assert_called_once()


def test_already_connect(socket_source):
    socket_source.socket = MagicMock()
    socket_source.connected = True
    socket_source._connect()

    assert socket_source.connected is True
    assert socket_source.socket.connect.call_count == 0


def test_receive_message(socket_source):
    test_input = "data"
    socket_source.socket = MagicMock()
    x = MagicMock()
    x.recv = MagicMock(return_value=bytes(test_input, "utf-8"))
    socket_source.socket.accept = MagicMock(return_value=(x, "address"))

    result = socket_source.receive_message()

    assert result == test_input
