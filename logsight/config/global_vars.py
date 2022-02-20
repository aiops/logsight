import os

DEBUG = False
USES_KAFKA = False
USES_ES = True
CONFIG_PATH = os.path.split(os.path.realpath(__file__))[0]
FILE_SINK_PATH = os.path.join(os.path.split(CONFIG_PATH)[0], "datastore")
PIPELINE_PATH = os.path.join(CONFIG_PATH, "pipeline.json")
