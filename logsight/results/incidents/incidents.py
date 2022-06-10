import datetime
import logging.config
from typing import List

from analytics_core.modules.incidents import IncidentDetector
from configs.global_vars import PIPELINE_INDEX_EXT
from results.common.index_job import IndexJob
from results.persistence.dto import IndexInterval
from services.service_provider import ServiceProvider

logger = logging.getLogger("logsight." + __name__)


class CalculateIncidentJob(IndexJob):
    def __init__(self, index_interval: IndexInterval, **kwargs):
        super().__init__(index_interval, index_ext="incidents", **kwargs)
        self.incident_detector = IncidentDetector()

    @staticmethod
    def load_templates(index):
        with ServiceProvider.provide_elasticsearch() as es:
            return es.get_all_templates_for_index("_".join([index, PIPELINE_INDEX_EXT]))

    def _calculate(self, logs) -> List:
        return self.incident_detector.calculate_incidents(logs)
