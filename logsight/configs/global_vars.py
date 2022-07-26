import os
from pathlib import Path

_default_path = Path(os.path.split(os.path.realpath(__file__))[0]) / "logsight_config.cfg"
CONFIG_PATH = os.environ.get('CONFIG_PATH', _default_path)
