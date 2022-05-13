import logging
from functools import wraps

from sqlalchemy.engine import create_engine
from sqlalchemy.exc import DatabaseError, OperationalError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from common.utils import unpack_singleton
from services.database.exceptions import DatabaseException

logger = logging.getLogger("logsight." + __name__)

MAX_ATTEMPTS = 5


def ensure_connection(func):
    @wraps(func)
    @retry(reraise=True, stop=stop_after_attempt(MAX_ATTEMPTS), wait=wait_fixed(20))
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
        self.engine = None
        self.conn = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _create_engine(self):
        """Create a new Engine instance."""
        self.engine = create_engine(f"{self.driver}://{self.username}:{self.password}@"
                                    f"{self.host}:{self.port}/{self.db_name}", pool_pre_ping=True,
                                    echo=False)

    @retry(reraise=True, retry=retry_if_exception_type(ConnectionError), stop=stop_after_attempt(MAX_ATTEMPTS),
           wait=wait_fixed(5))
    def connect(self):
        """Connect to the postgres database"""
        n_attempt = self.connect.retry.statistics['attempt_number']
        attempt_msg = f"Attempt: {n_attempt}/{MAX_ATTEMPTS}" if n_attempt > 1 else ""
        logger.debug(
            f"Connecting to database {self.db_name} on {self.host}:{self.port}.{attempt_msg}")
        reason = ""
        try:
            if not self.engine:
                self._create_engine()
            self.conn = self.engine.connect()  # will return a valid object if connection success
        except OperationalError as e:
            logger.debug(e)
            reason = f"Database {self.db_name} unreachable on {self.host}:{self.port}"
        if self.conn:
            try:
                self._verify_database_exists(self.conn)
                logger.info(f"Connected to database {self.db_name}")
                return self
            except DatabaseException as e:
                reason = e
        logger.error(reason)
        raise ConnectionError(reason)

    def _verify_database_exists(self, conn):
        databases = conn.execute("""SELECT datname FROM pg_database;""", ()).fetchall()
        if (self.db_name,) not in databases:
            raise ConnectionError("Database does not exist.")

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
        list :
            list of all the rows
        """

        execute = self.conn.execute(sql, args)
        row = execute.fetchall()
        row = [x.values() for x in row]
        keys = execute.keys()
        row = [dict(zip(keys, x)) for x in row] if row else row
        return row

    def check_connection(self):
        return
