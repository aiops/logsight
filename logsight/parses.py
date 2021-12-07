from logsight_classes.application import Pipeline
from builders.application_builder import ApplicationBuilder
import json

obj = json.load(open("config/pipeline.json", 'rb'))

builder = ApplicationBuilder()

pipeline = builder.build_object(app_config=obj)
print(pipeline)
