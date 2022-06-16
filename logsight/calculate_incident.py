import logging

from jobs.jobs.incidents import CalculateIncidentJob
from jobs.persistence.timestamp_storage import TimestampStorageProvider
from services.configurator.config_manager import LogConfig

logging.config.dictConfig(LogConfig().config)
logger = logging.getLogger('logsight')

storage = TimestampStorageProvider.provide_timestamp_storage("incidents")
index_intervals = storage.get_all()
# for it in index_intervals:
#     job = self.job(index_interval=it, error_callback=logger.error, done_callback=logger.info,
#                    table_name=self.storage.__table__)
job = CalculateIncidentJob(index_intervals[0], error_callback=print, done_callback=print, table_name="incidents")
job.execute()
