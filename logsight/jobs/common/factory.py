from common.patterns.concurrent_job_manager import NewJobManager
from common.patterns.job_manager import JobManager
from configs.global_vars import ES_CLEANUP_AGE
from jobs.common.job_dispatcher import PeriodicJobDispatcher, TimedJobDispatcher
from jobs.jobs.delete_es_data_job import DeleteESDataJob
from jobs.jobs.incidents import CalculateIncidentJob
from jobs.jobs.log_aggregation import CalculateLogAggregationJob
from jobs.persistence.timestamp_storage import TimestampStorageProvider


class JobDispatcherFactory:
    @staticmethod
    def get_incident_dispatcher(n_jobs: int, timeout_period: int) -> PeriodicJobDispatcher:
        manager = NewJobManager(n_jobs)
        storage = TimestampStorageProvider.provide_timestamp_storage('incidents')
        return PeriodicJobDispatcher(job=CalculateIncidentJob,
                                     job_manager=manager,
                                     storage=storage,
                                     timeout_period=timeout_period,
                                     timer_name="CalculateIncidentJob")

    @staticmethod
    def get_log_agg_dispatcher(n_jobs: int, timeout_period: int) -> PeriodicJobDispatcher:
        manager = NewJobManager(n_jobs)
        storage = TimestampStorageProvider.provide_timestamp_storage('log_agg')
        return PeriodicJobDispatcher(job=CalculateLogAggregationJob,
                                     job_manager=manager,
                                     storage=storage,
                                     timeout_period=timeout_period,
                                     timer_name="CalculateLogAgg")

    @staticmethod
    def get_es_delete_idx_dispatcher(timeout_period: int) -> TimedJobDispatcher:
        job = DeleteESDataJob(cleanup_age=ES_CLEANUP_AGE)
        return TimedJobDispatcher(job=job,
                                  timeout_period=timeout_period,
                                  timer_name="CalculateLogAgg")
