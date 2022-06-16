import threading
from unittest.mock import MagicMock, Mock

import pytest

from common.logsight_classes.configs import ConnectionConfigProperties
from pipeline import PipelineBuilder
from pipeline.modules.core import ConnectableModule
from services import ModulePipelineConfig
from elasticsearch import helpers

from services.database.postgres.db import PostgresDBService
from services.elasticsearch_service.elasticsearch_service import ElasticsearchService
from services.service_provider import ServiceProvider


@pytest.fixture
def pipeline():
    pipeline_cfg = ModulePipelineConfig().pipeline_config
    builder = PipelineBuilder()
    pipeline = builder.build(pipeline_cfg)
    yield pipeline


@pytest.fixture
def pipeline_with_control():
    pipeline_cfg = ModulePipelineConfig().pipeline_config
    pipeline_cfg.connectors.control_source = ConnectionConfigProperties(classname="StdinSource", connection="stdin")
    # Add control source

    builder = PipelineBuilder()
    pipeline = builder.build(pipeline_cfg)
    yield pipeline


def test_run(pipeline):
    pipeline.control_source = MagicMock()
    threading.Thread = MagicMock()
    threading.Thread.start = MagicMock()
    pipeline.data_source._receive_message = MagicMock(
        return_value="""{"logs": [{"timestamp": "2020-01-01", "message": "Hello World", "level": "INFO"}],
                      "index": "test_index"}""".encode('utf-8')
    )
    pipeline.data_source.has_next = MagicMock(side_effect=[True, False])
    if 'log_ad' in pipeline.modules:
        pipeline.modules['log_ad'].ad.model.predict = MagicMock(return_value=[0], side_effect=[[0]])
    for module_name in pipeline.modules:
        if isinstance(pipeline.modules[module_name], ConnectableModule):
            pipeline.modules[module_name].connector = MagicMock(sepc=pipeline.modules[module_name].connector)
            pipeline.modules[module_name].connector.connect = MagicMock()
            pipeline.modules[module_name].connector.send = MagicMock()

    es = ElasticsearchService("scheme", "host", 9201, "user", "password")
    es._connect = MagicMock()
    es.get_all_templates_for_index = MagicMock(return_value=[])
    ServiceProvider.provide_elasticsearch = MagicMock(return_value=es)

    pipeline.data_source.has_next = Mock(side_effect=[True, False])
    helpers.bulk = MagicMock()
    pipeline.data_source.connect = MagicMock()
    pipeline.storage = MagicMock()

    pipeline.run()


def test_start_control_listener(pipeline):
    pipeline.control_source = MagicMock()
    pipeline.control_source.has_next = MagicMock(side_effect=[True, False])
    pipeline.control_source.receive_message = MagicMock(return_value="Control message")

    pipeline._start_control_listener()

    pipeline.control_source.has_next.assert_called()
    assert pipeline.control_source.has_next.call_count == 2
    assert pipeline.control_source.receive_message.call_count == 1


def test__process_control_message(pipeline):
    message = "Test message"
    result = pipeline._process_control_message(message)
    assert result == message


def test_id(pipeline):
    assert pipeline.id == pipeline._id
