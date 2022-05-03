from pipeline.builders.pipeline_builder import PipelineBuilder
from services import ModulePipelineConfig

pipeline_path = "./test_pipeline.cfg"

pipeline_cfg_module = ModulePipelineConfig(pipeline_path)
pipeline_cfg = pipeline_cfg_module.pipeline_config

builder = PipelineBuilder()

pipeline = builder.build(pipeline_cfg)

print(pipeline)
pipeline.run()
