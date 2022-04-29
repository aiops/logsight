import dataclasses
import json


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


class DataClassJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        return super().default(o)
