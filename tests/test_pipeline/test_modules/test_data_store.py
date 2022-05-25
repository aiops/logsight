from unittest.mock import MagicMock

import pytest
from dacite import from_dict

from analytics_core.logs import LogBatch
from common.utils.helpers import to_flat_dict
from configs.global_vars import PIPELINE_INDEX_EXT
from connectors import PrintSink
from pipeline.modules import DataStoreModule


@pytest.fixture(scope="module")
def log_batch():
    return from_dict(data={"logs": [{"event": {"timestamp": "2020-01-01", "message": "Hello World", "level": "INFO"}}],
                           "index": "test_index"}, data_class=LogBatch)


def test_process(log_batch):
    store = DataStoreModule(PrintSink())
    store.connector.send = MagicMock()
    processed = [to_flat_dict(log) for log in log_batch.logs]

    store.process(log_batch)

    store.connector.send.assert_called_once_with(processed, target="_".join([log_batch.index, PIPELINE_INDEX_EXT]))


