from logsight.common.helpers import to_flat_dict
from tests.utils import TestInputConfig


def test_to_flat_dict():
    logsight_log = TestInputConfig.logsight_log
    result = to_flat_dict(logsight_log)
    assert isinstance(result, dict)
