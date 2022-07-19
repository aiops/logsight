import pytest

from common.logsight_classes.configs import AdapterConfigProperties, ConnectorConfigProperties, ModuleConfig, \
    PipelineConfig, PipelineConnectors
from common.patterns.builder import BuilderException
from connectors.sources import StdinSource
from pipeline import PipelineBuilder
from pipeline.modules import AnomalyDetectionModule, LogStoreModule, LogParserModule


@pytest.fixture
def valid_pipeline_cfg():
    ad_config = ModuleConfig("AnomalyDetectionModule", next_module="sink")
    parse_config = ModuleConfig("LogParserModule", next_module="ad")
    sink_config = ModuleConfig("LogStoreModule", args={"connector": {"connection": "stdout", "connector_type": "sink"}})
    yield PipelineConfig(
        connectors=PipelineConnectors(
            AdapterConfigProperties(ConnectorConfigProperties(connector_type="source", connection="stdin"))),
        modules={"ad": ad_config, "sink": sink_config, "parse": parse_config})


@pytest.fixture
def module_not_connected_cfg():
    ad_config = ModuleConfig("AnomalyDetectionModule", next_module="sink")
    parse_config = ModuleConfig("LogParserModule")
    sink_config = ModuleConfig(classname="LogStoreModule",
                               args={
                                   "connector": {
                                       "connection": "stdout",
                                       "connector_type": "sink",
                                       "params": {}
                                   }})
    yield PipelineConfig(
        connectors=PipelineConnectors(
            data_source=AdapterConfigProperties(
                ConnectorConfigProperties(
                    connector_type="source",
                    connection="file"))),
        modules={"ad": ad_config, "sink": sink_config, "parse": parse_config})


def test_build(valid_pipeline_cfg):
    builder = PipelineBuilder()
    pipeline = builder.build(valid_pipeline_cfg)
    assert isinstance(pipeline.data_source.connector, StdinSource)
    assert len(pipeline.modules) == 3
    assert isinstance(pipeline.modules['ad'], AnomalyDetectionModule)
    assert isinstance(pipeline.modules['parse'], LogParserModule)
    assert isinstance(pipeline.modules['sink'], LogStoreModule)


def test_build_fail(module_not_connected_cfg):
    builder = PipelineBuilder()
    pytest.raises(BuilderException, builder.build, module_not_connected_cfg)
