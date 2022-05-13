import json
import logging.config
import os

from configs import global_vars
from results.common.factory import JobDispatcherFactory
from results.log_aggregation.log_aggregation import CalculateLogAggregationJob

if __name__ == "__main__":
    logging.config.dictConfig(json.load(open(os.path.join(global_vars.CONFIG_PATH, "log.json"), 'r')))
    logger = logging.getLogger('logsight')
    log_agg = JobDispatcherFactory.get_log_agg_dispatcher(n_jobs=3, timeout_period=10)
    intervals = log_agg.storage.get_all()
    job = CalculateLogAggregationJob(intervals[0], error_callback=logger.error, done_callback=logger.info)
    job.execute()
    # log_agg.start()
    # while True:
    #     # A function that is used to pause the execution of the program for a given number of seconds.
    #     sleep(3)

# if __name__ == "__main__":
#     incidents = JobDispatcherFactory.get_incident_dispatcher(n_jobs=3, timeout_period=10)
#     incidents.start()
#     while True:
#         sleep(3)
