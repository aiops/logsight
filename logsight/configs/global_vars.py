import os

DEBUG = False
CONFIG_PATH = os.path.split(os.path.realpath(__file__))[0]
FILE_SINK_PATH = os.path.join(os.path.split(CONFIG_PATH)[0], "datastore")
PIPELINE_PATH = os.path.join(CONFIG_PATH, "pipeline.cfg")
LOGS_CONFIG_PATH = os.path.join(CONFIG_PATH, "log.json")
CONNECTIONS_PATH = os.path.join(CONFIG_PATH, "connections.cfg")
PIPELINE_INDEX_EXT = "pipeline"
RETRY_ATTEMPTS = int(os.environ.get('RETRY_ATTEMPTS', 6))
RETRY_TIMEOUT = int(os.environ.get('RETRY_TIMEOUT', 10))
