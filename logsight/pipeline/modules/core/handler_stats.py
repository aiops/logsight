import collections
import copy
import logging
import statistics
from time import perf_counter

from pipeline.modules.core.timer import NamedTimer
from pipeline.modules.core.wrappers import Synchronized

logger = logging.getLogger("logsight." + __name__)


class HandlerStats(Synchronized):
    def __init__(self, ctx: dict, log_stats_interval_sec: int = 60):
        self.ctx = ctx
        self.num_request = 0
        self.num_result = 0
        self.request_result_times = collections.deque(maxlen=10000)
        self.perf_counter = 0

        self.log_stats_interval_sec = log_stats_interval_sec

        self.timer = NamedTimer(self.log_stats_interval_sec, self.log_stats, self.__class__.__name__)
        self.timer.start()

    def receive_request(self):
        self.num_request += 1
        self.perf_counter = perf_counter()

    def handled_request(self):
        self.num_result += 1
        perf_counter_request = perf_counter()
        self.request_result_times.append(perf_counter_request - self.perf_counter)

    def log_stats(self):
        mean_processing_time = 0
        if len(self.request_result_times) > 0:
            copy_times = copy.deepcopy(self.request_result_times)
            mean_processing_time = statistics.mean(copy_times)
        logger.debug(
            f"Handler {self.ctx} handled {self.num_request} requests and calculated {self.num_result} results in total. " +
            f"Handling of 1000 requests took on average {mean_processing_time * 1000} seconds.")
        self.timer.reset_timer()
