import os

RISK_SCORE_ADDED_STATE_LEVEL_INFO_SEMANTICS_REPORT = int(
    os.environ.get('RISK_SCORE_ADDED_STATE_LEVEL_INFO_SEMANTICS_REPORT', 1))
RISK_SCORE_ADDED_STATE_LEVEL_ERROR_SEMANTICS_REPORT = int(
    os.environ.get('RISK_SCORE_ADDED_STATE_LEVEL_ERROR_SEMANTICS_REPORT', 70))
RISK_SCORE_ADDED_STATE_LEVEL_INFO_SEMANTICS_FAULT = int(
    os.environ.get('RISK_SCORE_ADDED_STATE_LEVEL_INFO_SEMANTICS_FAULT', 60))
RISK_SCORE_ADDED_STATE_LEVEL_ERROR_SEMANTICS_FAULT = int(
    os.environ.get('RISK_SCORE_ADDED_STATE_LEVEL_ERROR_SEMANTICS_FAULT', 80))

RISK_SCORE_RECURRING_STATE_LEVEL_INFO_SEMANTICS_REPORT = int(
    os.environ.get('RISK_SCORE_RECURRING_STATE_LEVEL_INFO_SEMANTICS_REPORT', 0))
RISK_SCORE_RECURRING_STATE_LEVEL_ERROR_SEMANTICS_REPORT = int(
    os.environ.get('RISK_SCORE_RECURRING_STATE_LEVEL_ERROR_SEMANTICS_REPORT', 25))
RISK_SCORE_RECURRING_STATE_LEVEL_INFO_SEMANTICS_FAULT = int(
    os.environ.get('RISK_SCORE_RECURRING_STATE_LEVEL_INFO_SEMANTICS_FAULT', 25))
RISK_SCORE_RECURRING_STATE_LEVEL_ERROR_SEMANTICS_FAULT = int(
    os.environ.get('RISK_SCORE_RECURRING_STATE_LEVEL_ERROR_SEMANTICS_FAULT', 25))
