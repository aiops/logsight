import json
from dataclasses import asdict

import pytest
import ujson

from logsight.analytics_core.logs import LogBatch, LogsightLog
from logsight.connectors.serializers import DefaultSerializer, JSONSerializer, LogBatchSerializer


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


@pytest.fixture
def default_s():
    yield DefaultSerializer()


@pytest.fixture
def json_s():
    yield JSONSerializer()


def test_deserialize_default(default_s):
    test_in = "test"
    result = default_s.deserialize(test_in)
    assert test_in == result


def test_serialize_default(default_s):
    test_in = 3
    result = default_s.serialize(test_in)
    assert str(test_in) == result


def test_deserialize_json(json_s):
    expected = {"a": 1, "b": "test"}
    input_s = ujson.dumps(expected)
    result = json_s.deserialize(input_s)
    assert expected == result


def test_serialize_json(json_s):
    test_dict = {"a": 1, "b": "test"}
    expected = ujson.dumps(test_dict)
    result = json_s.serialize(test_dict)
    assert expected == result
