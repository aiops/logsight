import sys
from io import StringIO

from logsight.connectors.sinks import PrintSink


def test_send_stdout_sink():
    data = "test"
    sink = PrintSink()
    output = StringIO()  # Create StringIO object
    sys.stdout = output  # and redirect stdout.
    sink.send(data)  # Call unchanged function.
    sys.stdout = sys.__stdout__
    assert f"[SINK] Sending data: {data}\n" == output.getvalue()
