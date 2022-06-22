import pytest

from common.logsight_classes.configs import ModuleConfig
from connectors import ConnectableConnector
from pipeline.builders.module_builder import ModuleBuilder
from pipeline.modules.core import ConnectableModule


@pytest.fixture(ids=["anomaly_detection", "fork", "log_parsing"],
                params=["AnomalyDetectionModule",
                        "ForkModule",
                        "LogParserModule",
                        ])
def valid_module_cfg(request):
    yield ModuleConfig(classname=request.param, args={})


def test_build_module(valid_module_cfg):
    module_builder = ModuleBuilder()
    module = module_builder.build(valid_module_cfg)
    assert module.__class__.__name__ == valid_module_cfg.classname


def test_build_module_with_connector():
    module_builder = ModuleBuilder()
    module = module_builder.build(
        ModuleConfig(classname="LogStoreModule",
                     args={
                         "connector": {
                             "connector": {
                                 "connection": "elasticsearch",
                                 "connector_type": "sink",
                                 "params": {}
                             },
                             "serializer": "LogBatchSerializer"
                         }
                     }))
    assert module.__class__.__name__ == "LogStoreModule"
    assert isinstance(module, ConnectableModule) is True
    assert isinstance(module.connector, ConnectableConnector) is True
