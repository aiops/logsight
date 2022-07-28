from typing import Dict

from logsight.analytics_core.logs import LogsightLog


def unpack_singleton(x):
    """Gets the first element if the iterable has only one value.

    Otherwise, return the iterable.

    Parameters
    ----------
        x: iterable
    Returns
    --------
        The same iterable or the first element.
    """
    if hasattr(x, "__iter__") and len(x) == 1:
        return unpack_singleton(x[0])
    return x


def to_flat_dict(obj: LogsightLog) -> Dict:
    return dict(id=obj.id, tags=obj.tags, message=obj.message, timestamp=obj.timestamp, level=obj.level, **obj.metadata,
                tag_keys=list(obj.tags.keys()))
