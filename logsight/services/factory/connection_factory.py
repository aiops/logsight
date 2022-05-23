from services import ConnectionConfig


class ConnectionFactory:
    def __init__(self):
        self.conn_parser = ConnectionConfig()

    def create_postgres_connection(self):
        return
