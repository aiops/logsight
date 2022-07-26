from analytics_core.modules.incidents import IncidentDetector
from ....inputs import expected_incident_result, processed_logs


def test_calculate_incidents():
    detector = IncidentDetector()
    result = detector.calculate_incidents(processed_logs)
    assert expected_incident_result == result
