import os

DEBUG = os.environ.get('DEBUG', "false").lower() in ('true', '1', 't')
CONFIG_PATH = os.path.split(os.path.realpath(__file__))[0]
FILE_SINK_PATH = os.path.join(os.path.split(CONFIG_PATH)[0], "datastore")
PIPELINE_PATH = os.path.join(CONFIG_PATH, "pipeline.cfg")
LOGS_CONFIG_PATH = os.path.join(CONFIG_PATH, "log.json")
CONNECTIONS_PATH = os.path.join(CONFIG_PATH, "connections.cfg")
PIPELINE_INDEX_EXT = "pipeline"
RETRY_ATTEMPTS = int(os.environ.get('RETRY_ATTEMPTS', 6))
RETRY_TIMEOUT = int(os.environ.get('RETRY_TIMEOUT', 10))
INCIDENT_JOBS = os.environ.get('INCIDENT_JOBS', "false").lower() in ('true', '1', 't')
DELETE_ES_JOB = os.environ.get('DELETE_ES_JOB', "false").lower() in ('true', '1', 't')
PARALLEL_JOBS = int(os.environ.get('PARALLEL_JOBS', 2))
JOB_INTERVAL = int(os.environ.get('JOB_INTERVAL', 60))

ES_PIPELINE_ID_INGEST_TIMESTAMP = "ingest_timestamp"
ES_CLEANUP_AGE = str(os.environ.get('ES_CLEANUP_AGE', "now-1y"))
ES_CLEANUP_JOB_INTERVAL = int(os.environ.get('ES_CLEANUP_JOB_INTERVAL', 60 * 60))

FILTER_NORMAL = os.environ.get('FILTER_NORMAL', "true").lower() in ('true', '1', 't')
PIPELINE_CONNECTION = str(os.environ.get('PIPELINE_CONNECTION', "kafka"))

RUN_PIPELINE = os.environ.get('RUN_PIPELINE', "true").lower() in ('true', '1', 't')
