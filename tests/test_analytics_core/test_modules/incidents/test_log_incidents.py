import pytest

from analytics_core.modules.incidents import IncidentDetector

from tests.inputs import processed_logs, expected_incident_result


def test_calculate_incidents():
    detector = IncidentDetector()
    result = detector.calculate_incidents(processed_logs)
    assert expected_incident_result == result
