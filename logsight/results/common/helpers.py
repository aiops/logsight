from datetime import datetime

from results.common.index_job import IndexJobResult
from results.persistence.timestamp_storage import TimestampStorageProvider


def update_status(result: IndexJobResult):
    """
    The update_status function updates the timestamps of the calculated incidents.

    Args:
        result:IndexJobResult

    Returns:
        The updated incident_timestamps object
    """
    # update the processing window
    result.index_interval.start_date = result.index_interval.end_date
    result.index_interval.end_date = datetime.now()
    with TimestampStorageProvider.provide_timestamp_storage(result.table) as db:
        db.update_timestamps(result.index_interval)
    return result
