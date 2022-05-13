from services import ConnectionConfigParser


class ConnectionFactory:
    def __init__(self):
        self.conn_parser = ConnectionConfigParser()

    def create_postgres_connection(self):
        return
