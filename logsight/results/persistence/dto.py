import datetime
from dataclasses import dataclass


@dataclass
class IndexInterval:
    index: str
    latest_ingest_time: datetime.datetime
    latest_processed_time: datetime.datetime

    def __repr__(self):
        return (f'{self.__class__.__name__}'
                f'(latest_ingest_time={str(self.latest_ingest_time)}, latest_processed_time={str(self.latest_processed_time)})')
