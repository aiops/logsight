import logging
import logging.config
from multiprocessing import freeze_support
from time import sleep

from common.patterns.job import Job
from common.patterns.job_manager import JobManager
from services.configurator.config_manager import LogConfig


class TestJob(Job):
    def _execute(self):
        logger.info("Sleeping... " + self.name + self.id)
        sleep(2)
        logger.info("done_execute " + self.name + self.id)
        return "Done" + self.name


# Setting up the logging configuration.
logging.config.dictConfig(LogConfig().config)
logger = logging.getLogger('logsight')
if __name__ == '__main__':
    freeze_support()
    job = TestJob(name="test")
    job2 = TestJob(name="test2")
    job3 = TestJob(name="test3")

    manager = JobManager(3)

    manager.submit_job(job)
    manager.submit_job(job2)
    manager.submit_job(job3)

    manager.submit_job(job)
    manager.submit_job(job2)
    manager.submit_job(job3)
    manager.submit_job(job)
    manager.submit_job(job2)
    manager.submit_job(job3)

    manager.submit_job(job)
    manager.submit_job(job2)
    manager.submit_job(job3)
    flag = True
    while flag:
        sleep(10)
        flag = False
