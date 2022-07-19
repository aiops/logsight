import json
from dataclasses import asdict

import pytest
import ujson

from analytics_core.logs import LogBatch, LogsightLog
from connectors.serializers import LogBatchSerializer


@pytest.fixture
def log_batch_s():
    yield LogBatchSerializer()


@pytest.fixture
def log_batch():
    yield LogBatch([LogsightLog("test", "test", "test")], "test")


def test_deserialize_log_batch(log_batch_s, log_batch):
    test_in = json.dumps(asdict(log_batch))
    result = log_batch_s.deserialize(test_in)
    assert result == log_batch


def test_serialize_log_batch(log_batch_s, log_batch):
    expected = ujson.dumps(asdict(log_batch))
    result = log_batch_s.serialize(log_batch)
    assert expected == result
