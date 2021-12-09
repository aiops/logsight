import json


def load_json(path, mode="r"):
    with open(path, mode) as file:
        _obj = json.load(file)
    return _obj


def save_json(_object, path, mode='wb'):
    with open(path, mode) as file:
        json.dump(_object, file)


def verify_file_ext(filename: str, ext: str):
    filename += ext if not filename.endswith(ext) else ""
    return filename
