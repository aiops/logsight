import json


def load_json(path, mode="r"):
    with open(path, mode) as file:
        _obj = json.load(file)
    return _obj


def save_json(_object, path, mode='wb'):
    with open(path, mode) as file:
        json.dump(_object, file)
