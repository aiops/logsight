import pytest

from logsight.connectors.connectors.file import FileConfigProperties
from logsight.connectors.sources import FileSource


@pytest.fixture
def path():
    yield "test_data/jboss_v10.json"


@pytest.fixture
def file_source(path):
    cfg = FileConfigProperties(path=path)
    file_source = FileSource(cfg)
    return file_source
