from unittest.mock import MagicMock

import pytest

from common.enums import LogBatchStatus
from services.database.sql_statements import UPDATE_LOG_RECEIPT
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
        side_effect=[proxy_result("not_exists"), proxy_result("users")])

    pytest.raises(ConnectionError, database._verify_database_exists, database.conn)

    database.conn.execute.assert_called()


def test_update_log_receipt(database):
    database._execute_sql = MagicMock()
    query = UPDATE_LOG_RECEIPT
    database.update_log_receipt("random_uuid", 500)

    database._execute_sql.assert_called_once_with(query, (500, LogBatchStatus.DONE.value, "random_uuid",))
