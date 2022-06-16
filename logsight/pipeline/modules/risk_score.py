import logging
from datetime import datetime

from elasticsearch import NotFoundError

from analytics_core.logs import LogBatch
from analytics_core.modules.risk_analysis.risk_analysis import RiskAnalysis
from configs.global_vars import PIPELINE_INDEX_EXT
from pipeline.modules.core import TransformModule
from services.service_provider import ServiceProvider

logger = logging.getLogger("logsight." + __name__)


class RiskScoreModule(TransformModule):

    def __init__(self):
        super().__init__()
        self.risk_analysis = RiskAnalysis()

    def transform(self, data: LogBatch) -> LogBatch:
        data.logs = [self.risk_analysis.calculate_risk(log) for log in data.logs]
        return data
