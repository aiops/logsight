import logging
from datetime import datetime

from elasticsearch import NotFoundError

from analytics_core.logs import LogBatch
from analytics_core.modules.risk_analysis.risk_analysis import RiskAnalysis
from configs.global_vars import PIPELINE_INDEX_EXT
from pipeline.modules.core import TransformModule
from services.service_provider import ServiceProvider

logger = logging.getLogger("logsight." + __name__)


class RiskFactorModule(TransformModule):

    def __init__(self):
        super().__init__()
        self.risk_analysis = RiskAnalysis()

    def transform(self, data: LogBatch) -> LogBatch:
        templates = self._get_templates(data.index)
        data.logs = [self.risk_analysis.calculate_risk(log, templates) for log in data.logs]
        return data

    @staticmethod
    def _get_templates(index: str):
        try:
            with ServiceProvider.provide_elasticsearch() as es:
                return es.get_all_templates_for_index("_".join([index, PIPELINE_INDEX_EXT]),
                                                      str(datetime.now().isoformat()))
        except NotFoundError:
            return []
