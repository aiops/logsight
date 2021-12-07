from logsight_classes.pipeline import Pipeline
from builders.pipeline_builder import PipelineBuilder
import json

obj = json.load(open("config/new_pipeine_config.json", 'rb'))

builder = PipelineBuilder()

pipeline = builder.build_object(pipeline_config=obj)
print(pipeline)
