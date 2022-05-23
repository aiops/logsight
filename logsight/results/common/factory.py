from common.patterns.job_manager import JobManager
from results.common.job_dispatcher import PeriodicJobDispatcher
from results.incidents.incidents import CalculateIncidentJob
from results.persistence.timestamp_storage import TimestampStorageProvider


class JobDispatcherFactory:
    @staticmethod
    def get_incident_dispatcher(n_jobs: int, timeout_period: int) -> PeriodicJobDispatcher:
        manager = JobManager(n_jobs)
        storage = TimestampStorageProvider.provide_timestamp_storage('incidents')
        return PeriodicJobDispatcher(job=CalculateIncidentJob,
                                     job_manager=manager,
                                     storage=storage,
                                     timeout_period=timeout_period,
                                     timer_name="CalculateIncidentJob")

    @staticmethod
    def get_log_agg_dispatcher(n_jobs: int, timeout_period: int) -> PeriodicJobDispatcher:
        manager = JobManager(n_jobs)
        storage = TimestampStorageProvider.provide_timestamp_storage('log_agg')
        return PeriodicJobDispatcher(job=CalculateIncidentJob,
                                     job_manager=manager,
                                     storage=storage,
                                     timeout_period=timeout_period,
                                     timer_name="CalculateLogAgg")
