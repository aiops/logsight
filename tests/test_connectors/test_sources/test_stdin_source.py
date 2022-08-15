from unittest import mock

from logsight.connectors.sources import StdinSource


def test_stdin():
    stdin_source = StdinSource()
    value = "test"
    with mock.patch("builtins.input", return_value=value) as src_in:
        result = stdin_source.receive_message()
        assert result == value
        src_in.assert_called_once()
