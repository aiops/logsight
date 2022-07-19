from analytics_core.logs import LogsightLog
from analytics_core.modules.risk_analysis.vars import RISK_SCORE_ADDED_STATE_LEVEL_ERROR_SEMANTICS_FAULT, \
    RISK_SCORE_ADDED_STATE_LEVEL_ERROR_SEMANTICS_REPORT, \
    RISK_SCORE_ADDED_STATE_LEVEL_INFO_SEMANTICS_FAULT, RISK_SCORE_ADDED_STATE_LEVEL_INFO_SEMANTICS_REPORT, \
    RISK_SCORE_RECURRING_STATE_LEVEL_ERROR_SEMANTICS_FAULT, \
    RISK_SCORE_RECURRING_STATE_LEVEL_ERROR_SEMANTICS_REPORT, \
    RISK_SCORE_RECURRING_STATE_LEVEL_INFO_SEMANTICS_FAULT, RISK_SCORE_RECURRING_STATE_LEVEL_INFO_SEMANTICS_REPORT


class RiskAnalysis:
    # tuple (added_state,risk_score,level)
    states = {(0, 0, 0): RISK_SCORE_RECURRING_STATE_LEVEL_INFO_SEMANTICS_REPORT,
              (0, 0, 1): RISK_SCORE_RECURRING_STATE_LEVEL_ERROR_SEMANTICS_REPORT,
              (0, 1, 0): RISK_SCORE_RECURRING_STATE_LEVEL_INFO_SEMANTICS_FAULT,
              (0, 1, 1): RISK_SCORE_RECURRING_STATE_LEVEL_ERROR_SEMANTICS_FAULT,
              (1, 0, 0): RISK_SCORE_ADDED_STATE_LEVEL_INFO_SEMANTICS_REPORT,
              (1, 0, 1): RISK_SCORE_ADDED_STATE_LEVEL_ERROR_SEMANTICS_REPORT,
              (1, 1, 0): RISK_SCORE_ADDED_STATE_LEVEL_INFO_SEMANTICS_FAULT,
              (1, 1, 1): RISK_SCORE_ADDED_STATE_LEVEL_ERROR_SEMANTICS_FAULT}

    @staticmethod
    def level_as_binary(level):
        return int(str(level).upper() in ["ERROR", "ERR", "CRITICAL", "FAULT"])

    def calculate_risk(self, log: LogsightLog):
        log.metadata['added_state'] = 0
        log.metadata['risk_score'] = self.states[
            (log.metadata['added_state'], int(log.metadata['prediction']), (self.level_as_binary(log.level)))]
        return log
