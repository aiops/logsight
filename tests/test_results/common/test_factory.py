from results.common.factory import JobDispatcherFactory
from results.common.job_dispatcher import PeriodicJobDispatcher
from results.persistence.timestamp_storage import PostgresTimestampStorage


def test_get_incident_dispatcher():
    max_workers = 2
    dispatcher = JobDispatcherFactory.get_incident_dispatcher(n_jobs=max_workers, timeout_period=10)
    assert dispatcher.manager.pool._max_workers == max_workers
    assert isinstance(dispatcher, PeriodicJobDispatcher)
    assert isinstance(dispatcher.storage, PostgresTimestampStorage)
    assert dispatcher.timer.name == "CalculateIncidentJob_timer"
    assert dispatcher.storage.__table__ == "incidents"


def test_get_log_agg_dispatcher():
    max_workers = 2
    dispatcher = JobDispatcherFactory.get_log_agg_dispatcher(n_jobs=max_workers, timeout_period=10)
    assert dispatcher.manager.pool._max_workers == max_workers
    assert isinstance(dispatcher, PeriodicJobDispatcher)
    assert isinstance(dispatcher.storage, PostgresTimestampStorage)
    assert dispatcher.timer.name == "CalculateLogAgg_timer"
    assert dispatcher.storage.__table__ == "log_agg"
