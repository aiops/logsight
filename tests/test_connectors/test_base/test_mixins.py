from logsight.connectors.base.source import HasNextMixin


def test_has_next():
    mixin = HasNextMixin()
    
    assert "has_next" in HasNextMixin.__dict__
    assert mixin.has_next() is True
