from logsight.analytics_core.logs import LogsightLog
from logsight.analytics_core.modules.risk_analysis import vars


class RiskAnalysis:
    # tuple (added_state,prediction,level)
    states = {(0, 0, 0): vars.RISK_SCORE_RECURRING_STATE_LEVEL_INFO_SEMANTICS_REPORT,
              (0, 0, 1): vars.RISK_SCORE_RECURRING_STATE_LEVEL_ERROR_SEMANTICS_REPORT,
              (0, 1, 0): vars.RISK_SCORE_RECURRING_STATE_LEVEL_INFO_SEMANTICS_FAULT,
              (0, 1, 1): vars.RISK_SCORE_RECURRING_STATE_LEVEL_ERROR_SEMANTICS_FAULT,
              (1, 0, 0): vars.RISK_SCORE_ADDED_STATE_LEVEL_INFO_SEMANTICS_REPORT,
              (1, 0, 1): vars.RISK_SCORE_ADDED_STATE_LEVEL_ERROR_SEMANTICS_REPORT,
              (1, 1, 0): vars.RISK_SCORE_ADDED_STATE_LEVEL_INFO_SEMANTICS_FAULT,
              (1, 1, 1): vars.RISK_SCORE_ADDED_STATE_LEVEL_ERROR_SEMANTICS_FAULT,
              (-1, 0, 0): vars.RISK_SCORE_DELETED_STATE_LEVEL_INFO_SEMANTICS_REPORT,
              (-1, 0, 1): vars.RISK_SCORE_DELETED_STATE_LEVEL_ERROR_SEMANTICS_REPORT,
              (-1, 1, 0): vars.RISK_SCORE_DELETED_STATE_LEVEL_INFO_SEMANTICS_FAULT,
              (-1, 1, 1): vars.RISK_SCORE_DELETED_STATE_LEVEL_ERROR_SEMANTICS_FAULT
              }

    @staticmethod
    def level_as_binary(level):
        return int(str(level).upper() in ["ERROR", "ERR", "CRITICAL", "FAULT"])

    @staticmethod
    def state_to_code(state):
        state_codes = {"added": 0, "recurring": 1, "deleted": -1}
        return state_codes.get(state, 1)

    def calculate_risk(self, log: LogsightLog):
        log.metadata['added_state'] = 0
        log.metadata['risk_score'] = self.states[
            (log.metadata['added_state'], int(log.metadata['prediction']), (self.level_as_binary(log.level)))]
        return log

    def calculate_verification_risk(self, state, prediction, level):
        state = self.state_to_code(state)
        prediction = int(prediction)
        level = self.level_as_binary(level)
        return self.states[(state, prediction, level)]
