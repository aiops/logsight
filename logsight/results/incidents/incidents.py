import logging.config
from datetime import timedelta
from typing import List

import dateutil

from analytics_core.modules.incidents import IncidentDetector
from configs.global_vars import PIPELINE_INDEX_EXT
from results.common.index_job import IndexJob, IndexJobResult
from results.persistence.dto import IndexInterval
from services.service_provider import ServiceProvider

logger = logging.getLogger("logsight." + __name__)


class CalculateIncidentJob(IndexJob):
    def __init__(self, index_interval: IndexInterval, **kwargs):
        super().__init__(index_interval, index_ext="incidents", **kwargs)
        self.incident_detector = IncidentDetector()

    def load_templates(self, index, end_date):
        with ServiceProvider.provide_elasticsearch() as es:
            return es.get_all_templates_for_index("_".join([index, PIPELINE_INDEX_EXT]),
                                                  str(end_date.isoformat()))

    def _calculate(self, logs) -> List:
        pass

    def _perform_aggregation(self):
        # Load data
        logs = self._load_data(self.index_interval.index, self.index_interval.start_date, self.index_interval.end_date)
        if not len(logs):
            return False
        templates = self.load_templates(self.index_interval.index, self.index_interval.end_date)
        # calculate results
        results = self.incident_detector.calculate_incidents(logs, templates)
        # store results
        self._store_results(results, "_".join([self.index_interval.index, self.index_ext]))
        logger.debug(f"Stored {len(results)} results")
        self._update_index_interval(dateutil.parser.isoparse(logs[-1]['timestamp']) + timedelta(milliseconds=1))
        return True
