from logsight.logger.properties import LoggerConfigProperties


def test_default_logger_config_properties():
    properties = LoggerConfigProperties(config_path="path")
    assert properties.config_path == "path"

