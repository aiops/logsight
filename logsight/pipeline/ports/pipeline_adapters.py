from analytics_core.logs import LogBatch
from connectors.base.adapter import SourceAdapter


class PipelineSourceAdapter(SourceAdapter):
    def receive(self) -> LogBatch:
        return self.serializer.deserialize(self.connector.receive_message())
