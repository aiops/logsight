from unittest.mock import MagicMock

import pytest

from services.database.exceptions import DatabaseException
from services.database.postgres.db import PostgresDBConnection
from services.service_provider import ServiceProvider


@pytest.fixture
def database():
    db = ServiceProvider.provide_postgres()
    db.conn = MagicMock()
    db.conn.execute = MagicMock()
    yield db


def proxy_result(name):
    return [(name,)]


def test__verify_database_exists(database):
    database.conn.execute.return_value.fetchall = MagicMock(
        side_effect=[proxy_result(database.db_name), proxy_result("applications")])

    database._verify_database_exists(database.conn)

    database.conn.execute.assert_called()


def test__verify_database_exists_exception(database):
    database.conn.execute.return_value.fetchall = MagicMock(
        side_effect=[proxy_result(database.db_name), proxy_result("users")])

    pytest.raises(DatabaseException, database._verify_database_exists, database.conn)

    database.conn.execute.assert_called()
