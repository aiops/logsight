from analytics_core.logs import LogBatch
from connectors.base.mixins import ConnectableSinkAdapter, ConnectableSourceAdapter, \
    SinkAdapter, SourceAdapter


class PipelineSourceAdapter(SourceAdapter):
    def receive_message(self) -> LogBatch:
        return self.serializer.deserialize(self.connector.receive_message())


class PipelineSinkAdapter(SinkAdapter):
    pass


class PipelineConnectableSourceAdapter(ConnectableSourceAdapter):
    def receive_message(self) -> LogBatch:
        return self.serializer.deserialize(self.connector.receive_message())


class PipelineConnectableSinkAdapter(ConnectableSinkAdapter):
    pass
