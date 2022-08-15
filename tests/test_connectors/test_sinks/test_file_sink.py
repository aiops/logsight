import builtins
import os
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest

from logsight.connectors.connectors.file import FileConfigProperties
from logsight.connectors.sinks import FileSink


@pytest.fixture
def file_sink():
    path = Path(os.path.dirname(os.path.realpath(__file__)))
    yield FileSink(FileConfigProperties(path=str(path / "test_file.txt")))


def test_connect(file_sink):
    file_sink._connect()

    assert os.path.exists(file_sink.path)

    os.remove(file_sink.path)


def test_connect_create_folder(file_sink):
    with mock.patch("os.makedirs") as mocked:
        file_sink.path = Path("./test_path/test_file.txt")
        with mock.patch("builtins.open"):
            file_sink._connect()
            assert mocked.call_count == 1


def test_close(file_sink):
    file_sink.file = MagicMock()
    file_sink.file.close = MagicMock()

    file_sink.close()
    file_sink.file.close.assert_called_once()


def test_send(file_sink):
    test_input = "data"
    file_sink._connect()
    file_sink.file.write = MagicMock()
    file_sink.send(test_input)
    file_sink.file.write.assert_called_once_with(test_input + "\n")
    if os.path.exists(file_sink.path):
        os.remove(file_sink.path)
