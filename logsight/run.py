import json
import logging.config
import os
import platform
from multiprocessing import set_start_method

from configs import global_vars
# hello world
from configs.global_vars import JOB_INTERVAL, PARALLEL_JOBS
from pipeline import PipelineBuilder
from results.common.factory import JobDispatcherFactory
from services.configurator.config_manager import ModulePipelineConfig

logging.config.dictConfig(json.load(open(os.path.join(global_vars.CONFIG_PATH, "log.json"), 'r')))
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
    incidents = JobDispatcherFactory.get_incident_dispatcher(PARALLEL_JOBS, JOB_INTERVAL)
    incidents.start()
    # Run log agg
    log_agg = JobDispatcherFactory.get_log_agg_dispatcher(PARALLEL_JOBS, JOB_INTERVAL)
    log_agg.start()

    pipeline.run()


if __name__ == '__main__':
    run()
