from common.patterns.job import Job
from services.service_provider import ServiceProvider


class DeleteESDataJob(Job):

    def __init__(self, notification_callback=None, done_callback=None, error_callback=None, name=None,
                 cleanup_age="now-1y",
                 **kwargs):
        super().__init__(notification_callback, done_callback, error_callback, name, **kwargs)
        self.cleanup_age = cleanup_age

    def _execute(self):
        with ServiceProvider.provide_elasticsearch() as es:
            es.delete_by_ingest_timestamp("*_pipeline", start_time="now-15y", end_time=self.cleanup_age)
            es.delete_by_ingest_timestamp("*_incidents", start_time="now-15y", end_time=self.cleanup_age)
