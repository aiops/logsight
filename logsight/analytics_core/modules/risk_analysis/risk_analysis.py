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
        state_codes = {"added": 1, "recurring": 0, "deleted": -1}
        return state_codes.get(state, 1)

    def calculate_risk(self, log: LogsightLog):
        """
        The function takes a log object, adds a new metadata field called 'added_state' and sets it to
        0, then adds another metadata field called 'risk_score' and sets it to the value of the 'states'
        dictionary, which is a 3-tuple of the 'added_state' value, the 'prediction' value, and the
        'level_as_binary' value
        
        :param log: LogsightLog - the log object that is being processed
        :type log: LogsightLog
        :return: The log with the added metadata.
        """
        log.metadata['added_state'] = 0
        log.metadata['risk_score'] = self.states[
            (log.metadata['added_state'], int(log.metadata['prediction']), (self.level_as_binary(log.level)))]
        return log

    def calculate_verification_risk(self, state, prediction, level):
        """
        > Given a state, a prediction, and a level, return the verification risk associated with that
        state, prediction, and level
        
        :param state: the state
        :param prediction: 0 or 1
        :param level: the level of the prediction
        :return: The probability of the state, prediction, and level.
        """
        state = self.state_to_code(state)
        prediction = int(prediction)
        level = self.level_as_binary(level)
        return self.states[(state, prediction, level)]
