from logsight.common.enums import LogBatchStatus
from logsight.connectors.connectors.sql_db import DatabaseConfigProperties, DatabaseConnector
from logsight.connectors.connectors.sql_db.exceptions import DatabaseException
from .sql_statements import SELECT_TABLES, UPDATE_LOG_RECEIPT


class PostgresDBService(DatabaseConnector):

    def __init__(self, config: DatabaseConfigProperties):
        self.__doc__ = super().__init__.__doc__
        super().__init__(config)

    def _verify_database_exists(self, conn):
        super()._verify_database_exists(conn)
        tables = conn.execute(SELECT_TABLES).fetchall()
        if len(tables) == 0:
            raise DatabaseException(f"Tables not yet created for database {self.db_name}.")

    def update_log_receipt(self, batch_id, processed_log_count, satus=LogBatchStatus.DONE.value):
        query = UPDATE_LOG_RECEIPT
        return self._execute_sql(query, (processed_log_count, satus, batch_id))
