import pytest

from common.logsight_classes.configs import ConnectionConfigProperties, ModuleConfig, PipelineConfig, PipelineConnectors
from common.patterns.builder import BuilderException
from connectors import FileSource, StdinSource
from pipeline import PipelineBuilder
from pipeline.modules import AnomalyDetectionModule, DataStoreModule, LogParserModule


@pytest.fixture
def valid_pipeline_cfg():
    ad_config = ModuleConfig("AnomalyDetectionModule", next_module="sink")
    parse_config = ModuleConfig("LogParserModule", next_module="ad")
    sink_config = ModuleConfig("DataStoreModule", args={"connector": {"connection": "print", "classname": "PrintSink"}})
    yield PipelineConfig(
        connectors=PipelineConnectors(ConnectionConfigProperties(classname="StdinSource", connection="d")),
        modules={"ad": ad_config, "sink": sink_config, "parse": parse_config})


@pytest.fixture
def module_not_connected_cfg():
    ad_config = ModuleConfig("AnomalyDetectionModule")
    parse_config = ModuleConfig("LogParserModule", next_module="ad")
    sink_config = ModuleConfig("DataStoreModule", args={"connector": {"connection": "print", "classname": "PrintSink"}})
    yield PipelineConfig(
        connectors=PipelineConnectors(ConnectionConfigProperties(classname="FileSource", connection="file")),
        modules={"ad": ad_config, "sink": sink_config, "parse": parse_config})


def test_build(valid_pipeline_cfg):
    builder = PipelineBuilder()
    pipeline = builder.build(valid_pipeline_cfg)
    assert isinstance(pipeline.data_source, StdinSource)
    assert len(pipeline.modules) == 3
    assert isinstance(pipeline.modules['ad'], AnomalyDetectionModule)
    assert isinstance(pipeline.modules['parse'], LogParserModule)
    assert isinstance(pipeline.modules['sink'], DataStoreModule)


def test_build_fail(module_not_connected_cfg):
    builder = PipelineBuilder()
    pytest.raises(BuilderException, builder.build, module_not_connected_cfg)
