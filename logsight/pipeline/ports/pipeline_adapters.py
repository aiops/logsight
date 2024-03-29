from analytics_core.logs import LogBatch
from connectors.base.adapter import AdapterError, SourceAdapter


class PipelineSourceAdapter(SourceAdapter):
    def receive(self) -> LogBatch:
        message = self.connector.receive_message()
        try:
            log_batch = self.serializer.deserialize(message)
        except KeyError:
            raise AdapterError("Cannot deserialize message $m")
        return log_batch
