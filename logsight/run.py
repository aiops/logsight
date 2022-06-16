import logging.config
import platform
from multiprocessing import set_start_method

# hello world
from configs.global_vars import DELETE_ES_JOB, ES_CLEANUP_JOB_INTERVAL, INCIDENT_JOBS, JOB_INTERVAL, PARALLEL_JOBS
from pipeline import PipelineBuilder
from jobs.common.factory import JobDispatcherFactory
from jobs.persistence.timestamp_storage import TimestampStorageProvider
from services.configurator.config_manager import LogConfig, ModulePipelineConfig
from services.service_provider import ServiceProvider

logging.config.dictConfig(LogConfig().config)
logger = logging.getLogger('logsight')

# needed for running on Windows or macOS
if platform.system() != 'Linux':
    logger.debug(f"Start method fork for system {platform.system()}.")
    set_start_method("fork", force=True)


def verify_services():
    # Verify elasticsearch connection
    es = ServiceProvider.provide_elasticsearch()
    es.connect()

    # Verify db connection
    db = ServiceProvider.provide_postgres()
    db.connect()

    # Verify db connection for incidents
    if INCIDENT_JOBS:
        ts = TimestampStorageProvider.provide_timestamp_storage("incidents")
        ts.connect()


def run_pipeline():
    pipeline_cfg = ModulePipelineConfig().pipeline_config
    builder = PipelineBuilder()
    pipeline = builder.build(pipeline_cfg)
    pipeline.run()


def run_scheduled_jobs():
    # Run incidents
    if INCIDENT_JOBS:
        incidents = JobDispatcherFactory.get_incident_dispatcher(PARALLEL_JOBS, JOB_INTERVAL)
        logger.info("Starting Incident Job Dispatcher.")
        incidents.run()

    if DELETE_ES_JOB:
        logger.info("Starting Delete ES Index Job Dispatcher.")
        delete_es = JobDispatcherFactory.get_es_delete_idx_dispatcher(ES_CLEANUP_JOB_INTERVAL)
        delete_es.run()


def run():
    verify_services()
    # run_scheduled_jobs()
    run_pipeline()


if __name__ == '__main__':
    run()
