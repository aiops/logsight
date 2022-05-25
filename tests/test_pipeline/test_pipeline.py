from unittest.mock import MagicMock, Mock

import pytest

from pipeline import PipelineBuilder
from services import ModulePipelineConfig


@pytest.fixture
def pipeline():
    pipeline_cfg = ModulePipelineConfig().pipeline_config
    builder = PipelineBuilder()
    pipeline = builder.build(pipeline_cfg)
    yield pipeline


def test_run(pipeline):
    pipeline.data_source._receive_message = MagicMock(
        return_value="""{"logs": [{"event": {"timestamp": "2020-01-01", "message": "Hello World", "level": "INFO"}}],
                      "index": "test_index"}""".encode('utf-8')
    )
    pipeline.modules['log_ad'].ad.model.predict = MagicMock(return_value=[0], side_effect=[0])
    pipeline.data_source.has_next = Mock(side_effect=[True, False])
    pipeline.run()


def test_start_control_listener(pipeline):
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
