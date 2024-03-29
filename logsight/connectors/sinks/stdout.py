from typing import Any, Optional

from connectors.base.sink import Sink


class PrintSink(Sink):
    def send(self, data: Any, target: Optional[str] = None):
        print(f"[SINK] Sending data: {data}")
