from analytics_core.modules.incidents import IncidentDetector
from tests.inputs import processed_logs, incident_results


def test_calculate_incidents():
    detector = IncidentDetector()
    result = detector.calculate_incidents(processed_logs, [])
    print(result)
    assert result == incident_results
