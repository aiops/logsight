# from time import sleep
#
# from jobs.common.factory import JobDispatcherFactory
# import logging
# import logging.config
#
# from services.configurator.config_manager import LogConfig
#
# logging.config.dictConfig(LogConfig().config)
# logger = logging.getLogger('logsight')
# job_d = JobDispatcherFactory.get_es_delete_idx_dispatcher(timeout_period=15)
#
# job_d.execute_job()
#
# while True:
#     sleep(10)
# import datetime
#
# time = datetime.datetime.now()
# print(time)
from services.service_provider import ServiceProvider

db = ServiceProvider.provide_postgres()
db.update_log_receipt("11416e8b-a49b-4bb0-ad7d-3f64f73bb91a", 400)
