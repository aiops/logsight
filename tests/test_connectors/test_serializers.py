import json
from dataclasses import asdict
from unittest.mock import MagicMock

import pytest

from analytics_core.logs import LogBatch, LogsightLog
from connectors.serializers import DictSerializer, LogBatchSerializer


@pytest.fixture
def dict_s():
    yield DictSerializer()


@pytest.fixture
def log_batch_s():
    yield LogBatchSerializer()


@pytest.fixture
def log_batch():
    yield LogBatch([LogsightLog("test", "test", "test")], "test")


def test_deserialize_dict(dict_s):
    test_input = """{"key":"value"}"""
    result = dict_s.deserialize(test_input.encode("utf-8"))
    assert result == json.loads(test_input)


def test_serialize_dict(dict_s):
    test_input = {"key": "value"}
    result = dict_s.serialize(test_input)
    assert result == json.dumps(test_input).encode('utf-8')


def test_deserialize_log_batch(log_batch_s, log_batch):
    test_in = json.dumps(asdict(log_batch)).encode('utf-8')
    result = log_batch_s.deserialize(test_in)
    assert result == log_batch


def test_serialize_log_batch(log_batch_s, log_batch):
    expected = json.dumps(asdict(log_batch)).encode('utf-8')
    result = log_batch_s.serialize(log_batch)
    assert expected == result
