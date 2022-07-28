from unittest.mock import MagicMock

import pytest

from logsight.connectors.connectors.sql_db import DatabaseConfigProperties, DatabaseConnector


@pytest.fixture
def database():
    database_config = DatabaseConfigProperties(host="localhost",
                                               port=5432, username="username", password="password", db_name="db_name")
    return DatabaseConnector(database_config)


def test__create_engine(database):
    database._create_engine()
    assert database.engine is not None


def test_connect(database):
    database._create_engine()
    database.engine.connect = MagicMock()
    database._verify_database_exists = MagicMock(return_value=None)
    database.connect()
    assert database.conn is not None


def test_connect_no_engine(database):
    database.engine = MagicMock(return_value=None, side_effect=None)
    database._verify_database_exists = MagicMock(return_value=None)
    database.connect()
    assert database.conn is not None


def test_connect_unreachable(database):
    database._create_engine()
    database._verify_database_exists = MagicMock(return_value=None)
    pytest.raises(ConnectionError, database.connect)


def test__verify_database_exists(database):
    database._create_engine()
    database.conn = MagicMock()
    database.conn.execute = MagicMock()
    database.conn.execute.return_value.fetchall.return_value = [(database.db_name,)]

    database._verify_database_exists(database.conn)
    database.conn.execute.assert_called_once()


def test__verify_database_not_exists(database):
    database.conn = MagicMock()
    database.conn.execute = MagicMock()
    database.conn.execute.return_value.fetchall.return_value = []

    pytest.raises(ConnectionError, database._verify_database_exists, database.conn)
    database.conn.execute.assert_called_once()


def test_close(database):
    database._create_engine()
    database.conn = MagicMock()
    database.close()
    assert database.conn is None


def test__execute_sql(database):
    database.conn = MagicMock()
    database.conn.execute.return_value.fetchall.return_value = ["test"]
    assert "test" in database._execute_sql("TEST_SQL")


def test__read_one(database):
    database.conn = MagicMock()
    database.conn.execute.return_value.fetchall.return_value = ["test"]
    database._read_one("""sql string""")
    assert "test" in database._execute_sql("TEST_SQL")


def test__read_many(database):
    row = [{"test": "test"}]
    database.conn = MagicMock()
    database.conn.execute = MagicMock()
    database.conn.execute.return_value.fetchall.return_value = row
    result = database._read_many("""sql string""")
    assert row == result
    database.conn.execute.assert_called_once()


def test_enter_exit(database):
    database.engine.connect = MagicMock()
    database._verify_database_exists = MagicMock(return_value=None)
    with database as d:
        assert d.conn is not None  # when entered
    # connection is closed when exited
    assert d.conn is None
