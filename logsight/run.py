import logging.config
import platform
from multiprocessing import set_start_method

# hello world
from pipeline import PipelineBuilder
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
    pipeline.run()


if __name__ == '__main__':
    run()
