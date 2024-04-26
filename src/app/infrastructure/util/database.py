
# core python
from dataclasses import dataclass
import logging
import pyodbc
import os
import socket
import weakref

# pypi
import pandas as pd
from sqlalchemy import MetaData, create_engine, sql

# native
from infrastructure.util.config import AppConfig


# TODO: Make this a static class variable?
# Cache for created databases. We use two because the weak reference doesn't work with tuples or
# dicts. Need to revisit this at some point
_DB_ENGINE_CACHE = weakref.WeakValueDictionary()
_DB_META_CACHE = weakref.WeakValueDictionary()

MSSQL_CONN_STR = 'mssql+pyodbc://{host}:1433/{db}?driver={driver}&TrustServerCertificate=yes&trusted_connection=yes'
MSSQL_CONN_STR_WITH_USER = 'mssql+pyodbc://{username}:{password}@{host}:1433/{db}?driver={driver}&TrustServerCertificate=yes&Encrypt=no&App={app_name}'

# Driver options in order of preference
DRIVERS = [
    'SQL Server Native Client 11.0',
    'SQL Server Native Client 10.0',
    'SQL Server',
    'ODBC Driver 18 for SQL Server'
]


def get_app_name():
    base_app_name = os.environ.get('APP_NAME')
    return f'{base_app_name}_PID{os.getpid()}@{socket.gethostname()}'


def get_engine(config_section: str):
    """
    Get or create engine. Since each engine object will create a connection to the database server
    we shouldn't create unecessary copies for each table.
    See: http://docs.sqlalchemy.org/en/rel_1_1/core/connections.html#engine-disposal
    """

    # Get host & DB. Check if already in the cache.
    host = AppConfig().get(config_section, 'host', fallback=None)
    db = AppConfig().get(config_section, 'database', fallback=None)
    key = (host, db)

    if key not in _DB_ENGINE_CACHE:
        # Get user & pass from config
        username = AppConfig().get(config_section, 'username', fallback=None)
        password = AppConfig().get(config_section, 'password', fallback=None)

        # Get app name
        app_name = get_app_name()
        logging.debug(f'Setting DB connection app name to {app_name}')

        # Prepare connection string
        driver = select_driver()
        if not driver:
            raise RuntimeError('No SQL drivers found')

        connection_str = MSSQL_CONN_STR.format(
            host=host,
            db=db,
            driver=driver
        )

        # If username/password were in config, replace above conn str
        if username is not None and password is not None:
            connection_str = MSSQL_CONN_STR_WITH_USER.format(
                host=host,
                db=db,
                driver=driver,
                username=username,
                password=password,
                app_name=app_name
            )

        # Add sqlalchemy configs, if provided
        sqlalchemy_pool_size = AppConfig().get(config_section, 'sqlalchemy_pool_size', fallback=None)
        sqlalchemy_max_overflow = AppConfig().get(config_section, 'sqlalchemy_max_overflow', fallback=None)
        sqlalchemy_pool_timeout = AppConfig().get(config_section, 'sqlalchemy_pool_timeout', fallback=None)

        # http://docs.sqlalchemy.org/en/latest/dialects/mssql.html#legacy-schema-mode
        engine_args = {'url': connection_str, 'legacy_schema_aliasing': False}
        # Add optional default overrides
        if sqlalchemy_pool_size is not None:
            engine_args['pool_size'] = int(sqlalchemy_pool_size)
            logging.debug('SQLAlchemy engine creation: adding pool size {}'.format(engine_args['pool_size']))
        if sqlalchemy_max_overflow is not None:
            engine_args['max_overflow'] = int(sqlalchemy_max_overflow)
            logging.debug('SQLAlchemy engine creation: adding max overflow {}'.format(engine_args['max_overflow']))
        if sqlalchemy_pool_timeout is not None:
            engine_args['pool_timeout'] = int(sqlalchemy_pool_timeout)
            logging.debug('SQLAlchemy engine creation: adding pool timeout {}'.format(engine_args['pool_timeout']))
        engine = create_engine(**engine_args)
        _DB_ENGINE_CACHE[key] = engine

    return _DB_ENGINE_CACHE[key]


def get_metadata(config_section: str):
    """
    Get or create metadata
    """
    
    # Get host & DB. Check if already in the cache.
    host = AppConfig().get(config_section, 'host', fallback=None)
    db = AppConfig().get(config_section, 'database', fallback=None)
    key = (host, db)

    if key not in _DB_META_CACHE:
        # MetaData is a container object for table, column, and index definitions.
        # Good description is here
        # http://stackoverflow.com/questions/6983515/why-is-it-useful-to-have-a-metadata-object-which-is-not-bind-to-an-engine-in-sql
        meta = MetaData()
        _DB_META_CACHE[key] = meta

    return _DB_META_CACHE[key]


def select_driver():
    """
    Select best available driver

    :returns: A string representing the driver
    """
    installed_drivers = pyodbc.drivers()
    selected_driver = None
    for driver in DRIVERS:
        if driver in installed_drivers:
            selected_driver = driver.replace(' ', '+')
            break
    return selected_driver


@dataclass
class BaseDB(object):
    """A light wrapper around SQLAlchemy Engine and MetaData."""

    config_section: str

    def __post_init__(self):
        """
        Initialize Database object and create engine

        :param conn_key: Used to lookup db connection info
        :param environment: Optional instance specific override
        :return: None
        """
        # Get or create engine and metadata
        self.engine = get_engine(self.config_section)
        self.meta = get_metadata(self.config_section)

    def execute_read(self, sql_stmt, log_query=False):
        """
        Safely execute a SELECT statement. Execution is done in a transaction that is not
        committed to handle the case when an insert statement is passed by mistake

        :param sql_stmt: SqlAlchemy statement
        :param log_query: Set to log compiled query
        :return: Pandas DataFrame with results
        """
        if log_query:
            logging.info('=== SQL START ===')
            print(sql_stmt.compile())
            print(sql_stmt.compile().params)
            logging.info('=== SQL END ===')

        # Add NOLOCK hint in hopes of improving performance
        # TODO: revisit this ... is this ok?
        # sql_stmt = sql_stmt.with_hint()

        # Create transaction to run statement in and don't commit for failsafe
        with self.engine.begin() as connection:
            data = pd.read_sql_query(sql_stmt, connection, coerce_float=False)

        return data

    def execute_write(self, sql_stmt, log_query=False, commit=None):
        """
        Execute an INSERT, UPDATE, or DELETE statement. Execution is done in a transaction and
        COMMIT must be set in order to commit the transaction.

        :param sql_stmt: SqlAlchemy statement
        :param log_query: Set to log compiled query
        :param commit: Whether to commit. If not provided, defer to AppConfig
        :return: A sqlalchemy.engine.ResultProxy
        """
        if log_query:
            logging.info('=== SQL START ===')
            print(sql_stmt.compile())
            print(sql_stmt.compile().params)
            logging.info('=== SQL END ===')

        # Get commit from AppConfig if not provided
        if commit is None:
            commit = AppConfig().get(self.config_section, 'commit', fallback=False)

        # Create transaction to run statement in. Rollback if commit not set
        with self.engine.begin() as connection:
            result = connection.execute(sql_stmt)
            data = result
            if commit:
                connection.commit()
            else:
                logging.warning('Commit not set. Rolling back %s', sql_stmt)
                connection.rollback()

        return data


def _convert_to_df(rows, description):
    """
    Convert pyodbc result to dataframe

    :param rows: Raw pyodbc rows from cursor.fetchall()
    :param description: Description of columns from cursor.description
    :returns: DataFrame of results
    """
    # Description is a list of tuples. Each tuple is of the form (column name, type code,
    # display size, internal size, precision, scale, nullable). Extract just column names
    columns = [col_description[0] for col_description in description]

    # Create dict that we can turn into a dataframe
    df_dict = {}
    for column in columns:
        df_dict[column] = []

    # Go through each row and add the values to the dict
    for row in rows:
        assert len(row) == len(columns)
        for i, val in enumerate(row):
            df_dict[columns[i]].append(val)

    return pd.DataFrame(df_dict)


def execute_multi_query(conn, query_str):
    """
    Executes a query that may return multiple result sets. Each result set is returned as a separate
    DataFrame

    :param conn: A pyodbc connection
    :param query_str: The query str to execute
    :returns: A list of DataFrames
    """
    cursor = conn.cursor()
    cursor.execute(query_str)

    results = []
    while True:
        try:
            rows = cursor.fetchall()
            df = _convert_to_df(rows, cursor.description)
            results.append(df)
        except pyodbc.ProgrammingError:
            # fetchall will fail if the result set was not a query
            pass

        if not cursor.nextset():
            break

    return results
    

