from unittest import mock
import pytest


@pytest.fixture
def log_config():
    yield


@mock.patch("configs.global_vars.DEBUG", True)
def test_log_config_debug():
    from services.configurator.config_manager import LogConfig
    log_config = LogConfig(debug=True)
    config = log_config.config
    assert config['loggers']['logsight']['handlers'] == ["debug", "warning"]
    assert config['loggers']['logsight']['level'] == 'DEBUG'


def test_log_config():
    from services.configurator.config_manager import LogConfig
    log_config = LogConfig()
    config = log_config.config
    assert config['loggers']['logsight']['handlers'] == ['info', 'warning']
    assert config['loggers']['logsight']['level'] == 'INFO'
