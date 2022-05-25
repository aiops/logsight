from unittest.mock import MagicMock

import pytest
from dacite import from_dict

from analytics_core.logs import LogBatch
from analytics_core.modules.anomaly_detection.core.config import AnomalyDetectionConfig
from pipeline.modules import AnomalyDetectionModule


@pytest.fixture(scope="module")
def log_batch():
    return from_dict(data={"logs": [{"event": {"timestamp": "2020-01-01", "message": "Hello World", "level": "INFO"}}],
                           "index": "test_index"}, data_class=LogBatch)


def test_transform(log_batch):
    ad = AnomalyDetectionModule()
    ad.ad.predict = MagicMock()
    ad.transform(log_batch)
    ad.ad.predict.assert_called_once_with(log_batch)


def test_init_no_config():
    ad = AnomalyDetectionModule()
    default_config = AnomalyDetectionConfig()
    assert ad.ad.config == default_config


def test_init_with_config():
    cfg = {"pad_len": 1, "max_len": 1, "prediction_threshold": 1}
    ad = AnomalyDetectionModule(ad_config=cfg)
    assert from_dict(AnomalyDetectionConfig, cfg) == ad.ad.config
    assert ad.ad.config.pad_len == cfg['pad_len']
    assert ad.ad.config.prediction_threshold == cfg["prediction_threshold"]
    assert ad.ad.config.max_len == cfg["max_len"]
