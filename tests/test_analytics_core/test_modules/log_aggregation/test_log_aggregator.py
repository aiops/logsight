from analytics_core.logs import LogsightLog
from analytics_core.modules.log_aggregation import LogAggregator
from tests.inputs import processed_logs, agg_results


def test_aggregate_logs():
    log_agg = LogAggregator()
    result = log_agg.aggregate_logs(processed_logs)
    assert result == agg_results
