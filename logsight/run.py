import logging.config
import platform
from multiprocessing import set_start_method

# hello world
from configs.global_vars import JOB_INTERVAL, PARALLEL_JOBS
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

    # # Run incidents
    incidents = JobDispatcherFactory.get_incident_dispatcher(PARALLEL_JOBS, JOB_INTERVAL)
    incidents.start()

    pipeline.run()


if __name__ == '__main__':
    run()
