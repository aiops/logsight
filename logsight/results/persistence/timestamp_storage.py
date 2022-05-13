from typing import List

from dacite import from_dict

from results.persistence import sql_statements as statements
from results.persistence.dto import IndexInterval
from services import ConnectionConfigParser
from services.database.base import Database


class TimestampStorageProvider:
    @staticmethod
    def provide_timestamp_storage(table):
        return PostgresTimestampStorage(**ConnectionConfigParser().get_postgres_params(), table=table)


class TimestampStorage:
    def get_timestamps_for_index(self, index: str) -> IndexInterval:
        raise NotImplementedError

    def get_all(self) -> List[IndexInterval]:
        raise NotImplementedError

    def update_timestamps(self, timestamps: IndexInterval) -> IndexInterval:
        raise NotImplementedError

    def select_all_application_index(self) -> List[str]:
        raise NotImplementedError

    def select_all_index(self) -> List[str]:
        raise NotImplementedError


class PostgresTimestampStorage(TimestampStorage, Database):
    def __init__(self, table, host, port, username, password, db_name, driver=""):
        super().__init__(host, port, username, password, db_name, driver)
        self.__table__ = table or "timestamps"

    def select_all_application_index(self) -> List[str]:
        sql = statements.SELECT_ALL_APP_INDEX
        rows = [row['index'] for row in self._read_many(sql)]
        return rows

    def select_all_index(self) -> List[str]:
        sql = statements.SELECT_ALL_INDEX
        rows = [row['index'] for row in self._read_many(sql % self.__table__)]
        return rows

    def get_timestamps_for_index(self, index: str) -> IndexInterval:
        sql = statements.SELECT_FOR_INDEX
        row = self._read_one(sql % (self.__table__, index))
        return from_dict(data_class=IndexInterval, data=row)

    def get_all(self) -> List[IndexInterval]:
        sql = statements.SELECT_ALL
        rows = self._read_many(sql % self.__table__)
        return [from_dict(data_class=IndexInterval, data=row) for row in rows]

    def update_timestamps(self, timestamps: IndexInterval):
        sql = statements.UPDATE_TIMESTAMPS
        row = self._execute_sql(sql % (self.__table__, timestamps.index, timestamps.start_date, timestamps.end_date))
        return from_dict(data_class=IndexInterval, data=row)

    def _verify_database_exists(self, conn):
        super()._verify_database_exists(conn)
        self._auto_create_table(conn)

    def _auto_create_table(self, conn):
        table = conn.execute(statements.SELECT_TABLE, self.__table__).fetchall()
        if not table:
            conn.execute(statements.CREATE_TABLE % self.__table__)
