from typing import Any, Optional

from connectors.sinks.sink import ConnectableSink


class PrintSink(ConnectableSink):
    def connect(self):
        pass

    def close(self):
        pass

    def send(self, data: Any, target: Optional[str] = None):
        print(f"[SINK] Sending data: {data}")
