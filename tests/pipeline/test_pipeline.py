import unittest
from unittest.mock import MagicMock, Mock

import pytest

from connectors import ZeroMQSubSource
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
    pipeline.data_source.has_next = Mock(side_effect=[True, False])
    pipeline.run()
