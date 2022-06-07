import json
import logging.config
import os
import platform
from multiprocessing import set_start_method

# hello world
from pipeline import PipelineBuilder
from results.common.factory import JobDispatcherFactory
from services.configurator.config_manager import LogConfig, ModulePipelineConfig

logging.config.dictConfig(LogConfig().config)
logger = logging.getLogger('logsight')

# needed for running on Windows or macOS
if platform.system() != 'Linux':
    logger.info(f"Start method fork for system {platform.system()}.")
    set_start_method("fork", force=True)


def run():
    pipeline_cfg = ModulePipelineConfig().pipeline_config
    builder = PipelineBuilder()
    pipeline = builder.build(pipeline_cfg)

    # Run incidents
    incidents = JobDispatcherFactory.get_incident_dispatcher(2, 60)
    incidents.start()
    # Run log agg
    log_agg = JobDispatcherFactory.get_log_agg_dispatcher(2, 60)
    log_agg.start()

    pipeline.run()


if __name__ == '__main__':
    run()
