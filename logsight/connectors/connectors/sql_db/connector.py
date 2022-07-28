import logging
from functools import wraps

from sqlalchemy.engine import create_engine
from sqlalchemy.exc import DatabaseError, OperationalError
from sqlalchemy.pool import NullPool

from logsight.common.helpers import unpack_singleton
from .configuration import DatabaseConfigProperties
from .exceptions import DatabaseException
from ... import ConnectableConnector

logger = logging.getLogger("logsight." + __name__)


def ensure_connection(func):
    @wraps(func)
    def decorated(cls, sql, *args):
        try:
            if cls.conn is None:
                cls.connect()
            result = func(cls, sql, *args)
            return result
        except OperationalError:
            cls.reconnect()
        except (Exception, DatabaseError) as error:
            logger.error(f"Failed database operation. Reason: {error}.")
            raise error

    return decorated


class DatabaseConnector(ConnectableConnector):
    """Base database class which uses sqlalchemy library.   """

    def __init__(self, config: DatabaseConfigProperties):
        self.db_name = config.db_name
        self.driver = config.driver
        self.username = config.username
        self.password = config.password
        self.host = config.host
        self.port = config.port
        self.engine = self._create_engine()
        self.conn = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _create_engine(self):
        """Create a new Engine instance."""
        return create_engine(f"{self.driver}://{self.username}:{self.password}@"
                             f"{self.host}:{self.port}/{self.db_name}", pool_pre_ping=True,
                             echo=False, poolclass=NullPool)

    def _connect(self):
        """Connect to the postgres database"""
        reason = ""
        try:
            self.conn = self.engine.connect()  # will return a valid object if connection success
        except OperationalError as e:
            logger.error(e)
            reason = f"Database {self.db_name} unreachable on {self.host}:{self.port}"
        if self.conn:
            try:
                self._verify_database_exists(self.conn)
                return self
            except DatabaseException as e:
                reason = e
        logger.warning(reason)
        raise ConnectionError(reason)

    def _verify_database_exists(self, conn):
        databases = conn.execute("""SELECT datname FROM pg_database;""", ()).fetchall()
        if (self.db_name,) not in databases:
            raise ConnectionError("Database does not exist.")

    def close(self):
        """Close the postgres connection"""
        if self.conn and not self.conn.closed:
            self.conn.close()
        assert self.conn.closed
        self.conn = None

    @ensure_connection
    def _execute_sql(self, sql, *args):
        """Executes the given sql statement using the provided arguments
        Parameters
        ----------
        sql : str
            sql statement
        *args - tuple
            provided arguments for sql statement
        result : any
            result specified in statement"""

        execute = self.conn.execute(sql, args)
        row = execute.fetchall()
        return unpack_singleton(row) if row is not None else row

    @ensure_connection
    def _read_one(self, sql, *args):
        """Executes the given sql statement using the provided arguments.
            This is used for reading statements

        Parameters
        ----------
        sql : str
            sql statement
        *args - tuple
            provided arguments for sql statement

        Returns
        -------
        dict
            Dictionary containing the column names and row values
        """
        execute = self.conn.execute(sql, args)
        row = execute.fetchone()
        row = dict(zip(execute.keys(), row)) if row is not None else row
        return row

    @ensure_connection
    def _read_many(self, sql, *args):
        """Executes the given sql statement using the provided arguments.
            This is used for reading statements

        Parameters
        ----------
        sql : str
            sql statement
        *args - tuple
            provided arguments for sql statement

        Returns
        -------
        list :
            list of all the rows
        """

        execute = self.conn.execute(sql, args)
        row = list(map(dict, execute.fetchall()))
        return row
