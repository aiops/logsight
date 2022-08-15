from unittest import mock
from unittest.mock import MagicMock

import pytest

from logsight.connectors.connectors.file import FileConfigProperties
from logsight.connectors.sources import FileSource


@pytest.fixture
def path():
    yield "test_data/jboss_v10.json"


@pytest.fixture
def file_source(path):
    cfg = FileConfigProperties(path=path)
    file_source = FileSource(cfg)
    return file_source


def test_connect(file_source):
    with mock.patch("builtins.open"):
        file_source._connect()

        assert file_source.file is not None


def test_receive_message(file_source):
    file_source._read_line = MagicMock(return_value="line")

    file_source.receive_message()

    assert file_source._read_line.call_count == 1


def test_receive_message_batch(path):
    cfg = FileConfigProperties(path=path, batch_size=5)
    file_source = FileSource(cfg)
    file_source._read_line = MagicMock(return_value="line")

    file_source.receive_message()

    assert file_source._read_line.call_count == 5


def test_receive_message_batch_eof(path):
    cfg = FileConfigProperties(path=path, batch_size=5)
    file_source = FileSource(cfg)
    file_source._read_line = MagicMock(side_effect=["line", None])

    file_source.receive_message()

    assert file_source._read_line.call_count == 2


def test_read_line(file_source):
    file_source.file = MagicMock()
    file_source.file.readline = MagicMock(return_value="line")

    file_source._read_line()


def test_read_line_reopen(file_source):
    value = "test"
    file_source.file = MagicMock()
    file_source.file.readline = MagicMock(return_value="")
    file_source._reopen_file = MagicMock(return_value=value)
    result = file_source._read_line()
    assert result == value
    assert file_source.cnt == 1
    file_source._reopen_file.assert_called_once()


def test_reopen_file_eof(file_source):
    result = file_source._reopen_file()
    assert result is None
    assert file_source.eof is True


def test_has_next(file_source):
    assert file_source.has_next() != file_source.eof


def test_reopen_file(file_source):
    value = "test"
    file_source.files_list.append("new_file.txt")
    with mock.patch("builtins.open"):
        file_source.file = MagicMock()
        file_source.receive_message = MagicMock(return_value=value)

        result = file_source._reopen_file()

        file_source.receive_message.assert_called_once()

    assert result == value
