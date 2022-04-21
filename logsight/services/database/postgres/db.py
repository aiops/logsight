from .sql_statements import *
from ..exceptions import DatabaseException
from ..wrappers import AppDatabase


class PostgresDBConnection(AppDatabase):

    def __init__(self, host, port, username, password, db_name, driver=""):
        self.__doc__ = super().__init__.__doc__
        super().__init__(host, port, username, password, db_name, driver)

    def _verify_database_exists(self, conn):

        databases = conn.execute(SELECT_DATABASES, ()).fetchall()
        if (self.db_name,) not in databases:
            raise ConnectionError("Database does not exist.")
        tables = conn.execute(SELECT_TABLES).fetchall()
        if ("applications",) not in tables:
            raise DatabaseException(f"Tables not yet created for database {self.db_name}.")

    def read_apps(self):
        sql = LIST_APPS
        rows = self._execute_sql(sql)
        apps = []
        if not isinstance(rows, list):
            rows = [rows]
        for row in rows:
            row = dict(zip(row.keys(), row))
            apps.append(row)
            for k in row.keys():
                row[k] = str(row[k])
        return apps

    def read_app(self, app_id):
        sql = READ_APPLICATION
        return self._execute_sql(sql)
