from common.logsight_classes.mixins import DictMixin


def test_to_dict():
    mixin = DictMixin()

    assert isinstance(mixin.to_dict(), dict)
