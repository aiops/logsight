import json
import logging.config
import os

from configs import global_vars
from pipeline.builders.pipeline_builder import PipelineBuilder
from services import ModulePipelineConfig

pipeline_path = "./test_pipeline.cfg"
logging.config.dictConfig(json.load(open(os.path.join(global_vars.CONFIG_PATH, "log.json"), 'r')))
logger = logging.getLogger('logsight')
pipeline_cfg_module = ModulePipelineConfig(pipeline_path)
pipeline_cfg = pipeline_cfg_module.pipeline_config

builder = PipelineBuilder()

pipeline = builder.build(pipeline_cfg)

pipeline.run()
