import logging
from functools import wraps
from time import sleep

from sqlalchemy.engine import create_engine
from sqlalchemy.exc import OperationalError, DatabaseError

from utils.helpers import unpack_singleton

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
            logger.error(error)
            raise error

    return decorated


# noinspection PyNoneFunctionAssignment
class Database:
    """Base database class which uses sqlalchemy library.
        Attributes
        ----------
        host : str
            Name of host
        port : int, str
            Connection port

        username : str
            Username for database

        password : str
            Password for database

        db_name : str
            Name of database

        driver : str, default='postgresql+psycopg2'
            sqlalchemy driver for connecting to the database
    """

    def __init__(self, host, port, username, password, db_name, driver=""):

        self.db_name = db_name
        self.driver = driver
        self.username = username or ''
        self.password = password or ''
        self.host = host or ''
        self.port = port or ''
        self._reconnect_attempts = 5
        self.engine = None
        self.conn = self.connect()

    def _create_engine(self):
        """Create a new Engine instance."""
        self.engine = create_engine(f"{self.driver}://{self.username}:{self.password}@"
                                    f"{self.host}:{self.port}/{self.db_name}", pool_pre_ping=True)

    def connect(self):
        """Connect to the postgres database"""
        try:
            if self.engine is None:
                self._create_engine()
            self.conn = self.engine.connect()
            self._reconnect_attempts = 5
        except OperationalError:
            self.reconnect()
        logger.debug(f"Connected to database {self.db_name}")

    def reconnect(self):
        """Reconnect to database"""
        try:
            if self._reconnect_attempts > 0:
                self._reconnect_attempts -= 1
                sleep(1)
                logger.debug(f"Trying to reconnect. Attempts : {self._reconnect_attempts}.")
                self.connect()
            else:
                self.conn = None
                logger.debug(f"Failed connecting to database {self.db_name}")
                logger.error(f"Failed connecting to database {self.db_name}")
        except OperationalError:
            self.reconnect()

    def close(self):
        """Close the postgres connection"""
        logger.info(f"Closing connection to database {self.db_name}")
        if self.conn and not self.conn.closed:
            self.conn.close()
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
        list
            list of all of the rows
        """

        execute = self.conn.execute(sql, args)
        row = execute.fetchall()
        row = [x.values() for x in row]
        keys = execute.keys()
        row = [dict(zip(keys, x)) for x in row] if row else row
        return row

    def check_connection(self):
        return