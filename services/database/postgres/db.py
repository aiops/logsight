from ..wrappers import AppDatabase
from .sql_statements import *


class PostgresDBConnection(AppDatabase):

    def __init__(self, host, port, username, password, db_name, driver=""):
        __doc__ = super().__init__.__doc__
        super().__init__(host, port, username, password, db_name, driver)
        self.connect()

    def read_apps(self):
        sql = LIST_APPS
        return self._execute_sql(sql)

    def read_app(self, app_id):
        sql = READ_APPLICATION
        return self._execute_sql(sql)
