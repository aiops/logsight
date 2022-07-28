import pytest

from logsight.configs.properties import LogsightProperties
from logsight.logger.configuration import LogConfig
from logsight.logger.properties import LoggerConfigProperties


@pytest.mark.parametrize("debug,expected", [(True, 'DEBUG'),
                                            (False, 'INFO')])
def test_log_config(debug, expected):
    log_config = LoggerConfigProperties()
    base_config = LogsightProperties(debug=debug)
    config = LogConfig(log_config, base_config)
    assert config.config['loggers']['logsight']['level'] == expected
