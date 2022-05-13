import datetime
from dataclasses import dataclass


@dataclass
class IndexInterval:
    index: str
    start_date: datetime.datetime
    end_date: datetime.datetime
