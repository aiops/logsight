import pytest
from dacite import from_dict

from analytics_core.logs import LogBatch
from pipeline.modules.filter import FilterModule


@pytest.fixture(scope="module")
def log_batch():
    return from_dict(data={"logs": [{"timestamp": "2020-01-01", "message": "Hello World", "level": "INFO"},
                                    {"timestamp": "2020-01-01", "message": "Hello World", "level": "ERROR"}],
                           "index": "test_index"}, data_class=LogBatch)


def test_filter_transform(log_batch):
    filter_module = FilterModule("level", "contains", "INFO")
    result = filter_module.transform(log_batch)
    assert len(result.logs) == 1
    assert result.logs[0].level == "INFO"
