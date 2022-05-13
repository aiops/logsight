from datetime import datetime

from results.persistence.dto import IndexInterval
from results.persistence.timestamp_storage import TimestampStorageProvider


def update_status(incident_timestamp: IndexInterval, table: str):
    """
    The update_status function updates the timestamps of the calculated incidents.

    Args:
        incident_timestamp:IncidentTimestamps: Pass the incident_timestamps object to the function
        table: Table name for timestamp storage

    Returns:
        The updated incident_timestamps object
    """
    # update the processing window
    incident_timestamp.start_date = incident_timestamp.end_date
    incident_timestamp.end_date = datetime.now()
    with TimestampStorageProvider.provide_timestamp_storage(table) as db:
        db.update_timestamps(incident_timestamp)
