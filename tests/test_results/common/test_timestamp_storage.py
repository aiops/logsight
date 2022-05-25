from dataclasses import asdict
from unittest.mock import MagicMock, Mock

import pytest

from results.persistence.dto import IndexInterval
from results.persistence.sql_statements import CREATE_TABLE, SELECT_ALL, SELECT_ALL_APP_INDEX, SELECT_ALL_INDEX, \
    SELECT_FOR_INDEX, \
    SELECT_TABLE, UPDATE_TIMESTAMPS
from results.persistence.timestamp_storage import PostgresTimestampStorage
from tests.utils import random_times


@pytest.fixture(scope="module")
def db_timestamps() -> PostgresTimestampStorage:
    return PostgresTimestampStorage("test", "host", "9200", "username", "password", "db_name", "postgresql")


def get_index_intervals(n_intervals):
    return [asdict(IndexInterval("index", *random_times("2020-01-01 00:00:00", "2022-01-01 00:00:00", 2))) for _ in
            range(n_intervals)]


def test_select_all_application_index(db_timestamps):
    n_rows = 4
    entries = get_index_intervals(n_rows)
    db_timestamps._read_many = MagicMock(side_effect=[entries])
    result = db_timestamps.select_all_application_index()
    db_timestamps._read_many.assert_called_once_with(SELECT_ALL_APP_INDEX)
    assert [e['index'] for e in entries] == result
    assert len(result) == n_rows


def test_select_all_index(db_timestamps):
    n_rows = 10
    db_timestamps._read_many = MagicMock(side_effect=[get_index_intervals(n_rows)])
    result = db_timestamps.select_all_index()
    db_timestamps._read_many.assert_called_once_with(SELECT_ALL_INDEX % db_timestamps.__table__)
    assert len(result) == n_rows


def test_get_timestamps_for_index(db_timestamps):
    n_rows = 1
    intervals = get_index_intervals(n_rows)
    db_timestamps._read_one = MagicMock(side_effect=intervals)
    result = db_timestamps.get_timestamps_for_index("index")

    db_timestamps._read_one.assert_called_once_with(SELECT_FOR_INDEX % (db_timestamps.__table__, "index"))
    assert isinstance(result, IndexInterval)
    assert intervals[0] == asdict(result)


def test_get_all(db_timestamps):
    n_rows = 5
    intervals = get_index_intervals(n_rows)
    db_timestamps._read_many = MagicMock(side_effect=[intervals])

    result = db_timestamps.get_all()

    db_timestamps._read_many.assert_called_once_with(SELECT_ALL % db_timestamps.__table__)
    assert isinstance(result, list)
    assert isinstance(result[0], IndexInterval)
    assert len(result) == n_rows


def test_update_timestamps(db_timestamps):
    interval_dict = get_index_intervals(1)[0]
    interval = IndexInterval(**interval_dict)
    db_timestamps._execute_sql = MagicMock(side_effect=[interval_dict])

    result = db_timestamps.update_timestamps(interval)
    db_timestamps._execute_sql.assert_called_once_with(
        UPDATE_TIMESTAMPS % (db_timestamps.__table__, interval.index, interval.start_date, interval.end_date))
    assert isinstance(result, IndexInterval)


def test__verify_database_exists(db_timestamps):
    db_timestamps._verify_database_exists = MagicMock()
    db_timestamps._verify_database_exists()
    db_timestamps._verify_database_exists.assert_called_once()


def test__auto_create_table(db_timestamps):
    db_timestamps.conn = MagicMock()
    db_timestamps.conn.execute = MagicMock()
    db_timestamps.conn.execute.return_value.fetch_all.return_value = "table_name"
    db_timestamps._auto_create_table(db_timestamps.conn)

    db_timestamps.conn.execute.assert_called_once_with(SELECT_TABLE, db_timestamps.__table__)
