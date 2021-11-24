from .base import Database


class AppDatabase(Database):
    def read_apps(self):
        raise NotImplementedError

    def read_app(self, app_id):
        raise NotImplementedError

