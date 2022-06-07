import datetime
from dataclasses import dataclass


@dataclass
class IndexInterval:
    index: str
    start_date: datetime.datetime
    end_date: datetime.datetime

    def __repr__(self):
        return (f'{self.__class__.__name__}'
                f'(start_date={str(self.start_date)}, end_date={str(self.end_date)})')
