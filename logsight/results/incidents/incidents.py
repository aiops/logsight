import datetime
from time import sleep

from common.patterns.job import Job
from common.patterns.job_manager import JobManager
from pipeline.modules.core.timer import NamedTimer
from services import ConnectionConfigParser
from services.elasticsearch.elasticsearch_service import ElasticsearchService


class Incidents:
    def __init__(self):
        self.manager = JobManager(2)
        self.es = ElasticsearchService(**ConnectionConfigParser().get_elasticsearch_params())
        self.timer = NamedTimer(name="CalculateIncidents", callback=self.callback, timeout_period=10)
        self.start_date = datetime.datetime.min.strftime("%Y-%m-%dT%H:%M:%S")
        self.end_date = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        self.jobs = []

    # TODO: Separate start and end date for every index

    def callback(self):
        indices = self.es.get_all_indices()
        for index in indices:
            print("submitting job for ", index)
            job = self.manager.submit_job(
                CalculateIncidentJob(index, self.start_date, self.end_date, error_callback=print, done_callback=print))
            self.jobs.append(job)
        self.start_date = self.end_date
        self.end_date = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        self.timer.reset_timer()

    def start(self):
        self.timer.start()


class CalculateIncidentJob(Job):
    def __init__(self, index, start_date, end_date, **kwargs):
        super().__init__(**kwargs)
        self.index = index
        self.start_date = start_date
        self.end_date = end_date

        self.incident_detector = None

    def _execute(self):
        print("job executing", file=open("out.txt", "a+"))
        self.es = ElasticsearchService(**ConnectionConfigParser().get_elasticsearch_params())
        logs = self.es.get_all_logs_for_index(self.index, self.start_date, self.end_date)
        print(logs, file=open("out.txt", "a+"))
        return logs


if __name__ == "__main__":
    # print(datetime.datetime.min)
    incidents = Incidents()
    print("Starting")
    incidents.start()
    while True:
        sleep(3)
