from .base import Database


class AppDatabase(Database):
    def _verify_database_exists(self, conn):
        raise NotImplementedError

    def read_apps(self):
        raise NotImplementedError

    def read_app(self, app_id):
        raise NotImplementedError
