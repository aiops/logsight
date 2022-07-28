import pytest

from logsight.connectors.builders.properties import AdapterConfigProperties, ConnectorConfigProperties


def test_connector_config_properties():
    pytest.raises(ValueError, ConnectorConfigProperties, connection="fail", connector_type="test")
    cfg = ConnectorConfigProperties(connection="kafka", connector_type="source")
    assert cfg.connector_type == "source"


def test_adapter_config_properties():
    cfg = ConnectorConfigProperties(connection="kafka", connector_type="source")
    adapter = AdapterConfigProperties(connector=cfg)
    assert adapter.connector == cfg
    assert adapter.serializer is None
