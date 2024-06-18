
# core python
import logging
import os
import re
import uuid
from typing import Dict, Union, List

# pypi
import pandas as pd
import sqlalchemy
from sqlalchemy import sql, text, Table, Integer, Boolean
from sqlalchemy.dialects.mssql import BIGINT, BIT, INTEGER, SMALLINT, TINYINT
from sqlalchemy.exc import IntegrityError

# native
from infrastructure.util.database import BaseDB
from infrastructure.util.file import prepare_file_path, get_unc_path
from infrastructure.util.config import AppConfig



BULK_INSERT_STMT = r"""
BULK INSERT {}
FROM '{}'
WITH (
    FIELDTERMINATOR = '|',
    ROWTERMINATOR = '|\n',
    DATAFILETYPE = 'widechar',
    TABLOCK
)
"""


class BaseTable(object):
    """
    Base class for representations of database tables. Given a database and table name, this class
    will reflect the table and then provide accessors to the columns and a generic query function.
    Custom or complex sql queries can use table_def to build the query.
    """
    config_section = None
    schema = 'dbo'
    table_name = None
    is_rotatable = False
    _database = None

    def __init__(self):
        """
        Initialize BaseTable object

        :returns: None
        """

        if not self.config_section:
            raise RuntimeError('Instances of BaseTable must provide a config_section which contains DB connection details')

        if not self.table_name:
            raise RuntimeError('Instances of BaseTable must set table_name')

        # If we're overriding the environment we need to recreate the database
        self._database = BaseDB(self.config_section)

        # Only create table definition if it doesn't exist yet
        if self.table_name in self._database.meta.tables:
            self.table_def = self._database.meta.tables[self.table_name]
        else:
            self.table_def = self.create_table_def()

    def create_table_def(self):
        """
        Function to create table def if table is not in metadata. Override this function to provide
        an explicit table definition rather than auto loading

        :returns: Table definition
        """
        return Table(self.table_name, self._database.meta, schema=self.schema,
                    autoload_with=self._database.engine)

    @property
    def c(self): # pylint: disable=C0103
        """
        Syntactic sugar to avoid table.table_def.c.column
        """
        return self.table_def.c

    def execute_write(self, sql_stmt, commit=None):
        """
        Syntactic sugar to aviod table.database.execute...

        :param sql_stmt: Statement to execute
        :param commit: Whether to commit. If not provided, see database.py::execute_write
        :returns: A sqlalchemy.engine.ResultProxy
        """
        return self._database.execute_write(sql_stmt, commit=commit)

    def execute_insert(self, data: Dict, commit=None):
        """
        Syntactic sugar to aviod table.database.execute...

        :param data: dict of single row to insert. Keys should match table columns.
        :param commit: Whether to commit. If not provided, see database.py::execute_write
        :returns: A sqlalchemy.engine.ResultProxy
        """
        stmt = sql.insert(self.table_def).values(**data)
        return self.execute_write(stmt, commit=commit)

    def execute_read(self, sql_stmt, nolock=True):
        """
        Syntactic sugar to aviod table.database.execute...

        :param sql_stmt: Statement to execute
        :param nolock: Whether to use nolock (performance gain)
        :returns: Dataframe of results
        """

        if nolock:
            sql_stmt = sql_stmt.with_hint(self.table_def, text='WITH (NOLOCK)')
        return self._database.execute_read(sql_stmt)

    def read(self):
        """
        Default read command that returns all data. Subclasses should override if they want to
        restrict columns

        :returns: Dataframe of all rows and columns
        """
        stmt = sql.select([self.table_def])
        return self.execute_read(stmt)

    def bulk_insert(self, df):
        """
        Used to insert a large number of rows into a table. Passed in dataframe must match the table
        exactly.

        :param df: A data frame of rows to insert
        :returns: Pyodbc result object
        """

        # Filter df columns to columns which exist in the table, to avoid SQL error from inserting a column which DNE
        df = df[df.columns.intersection(self.c.keys())]

        # num_rows = df.shape[0]
        # res_rows = df.to_sql(self.table_name, self._database.engine, self.schema, if_exists='append', index=False)
        # if res_rows == num_rows:
        #     logging.info('Insert done.')
        #     return res_rows

        file_name = '{}.txt'.format(uuid.uuid4())

        data_dir = AppConfig().get('files', 'data_dir', fallback='\\\\dev-data\\lws$\\Cameron\\lws\\var\\data')
        
        file_path = os.path.join(data_dir, 'temp', file_name)
        file_path = get_unc_path(file_path)
        prepare_file_path(file_path, rotate=False)

        # TODO: Consider using pandas df.to_csv()
        # UTF-16 encoding is required in order for bulk insert to be able to handle unicode data
        # https://stackoverflow.com/questions/5182164/sql-server-default-character-encoding
        with open(file_path, 'w', encoding='utf-16') as data_file:
            num_rows = df.shape[0]
            cur_row = 0
            db_cols = [(c.name, c.type) for c in self.table_def.columns]

            for row in df.itertuples():
                row_dict = row._asdict()
                cur_row += 1
                row_values = []

                # We need a value for each column in the order those columns are in the database
                for col_name, col_type in db_cols:
                    if col_name in row_dict:
                        value = row_dict[col_name]
                        if pd.isnull(value):
                            row_values.append('')
                        elif isinstance(col_type, (Boolean, Integer)):
                            row_values.append(str(int(value)))
                        elif isinstance(col_type, (BIGINT, BIT, INTEGER, SMALLINT, TINYINT)):
                            row_values.append(str(int(value)))
                        else:
                            # MSSQL doesn't do escaping well until 2017 version so we need to drop
                            # delimiter chars
                            value = str(value).replace('|', '')
                            row_values.append(value)
                    else:
                        row_values.append('')

                # After getting a value for each column, create a string to add to our pipe-delimited
                # file for this row.
                row_str = '|'.join(row_values) + '|\n'

                data_file.write(row_str)

        # Prepare statement
        table_fullname = '{}.{}.{}'.format(
            self._database.engine.url.database,
            self.schema,
            self.table_name
        )
        file_path = os.path.join(data_dir, 'temp', file_name)
        insert_stmt = BULK_INSERT_STMT.format(table_fullname, file_path)
        # insert_stmt = insert_stmt.replace('/', '\\')
        logging.debug(insert_stmt)

        # Execute
        result = self._database.execute_write(sql.text(insert_stmt))
        if result.rowcount != num_rows:
            logging.warning('Row count does not match expected: %d != %d', result.rowcount,
                            num_rows)

        # os.system(f'copy {file_path} L:\\temp\\CJ20230419.txt')
        os.remove(file_path)
        return result

    def upsert(self, pk_column_name: Union[List[str],str], data: dict):
        """
        Update if row matching pk_column_name exists, else insert

        :param pk_column_name: Name(s) of column(s) to check whether row(s) already exist.
                Assumption: this key exists in data dict, and is a column in the table
        :returns: A sqlalchemy.engine.ResultProxy
        """
        # try:
        table = self.table_def
        # logging.info(f'Table columns: {table.c}')
        # print(f'Table columns: {table.c}')
        # logging.info(f'Table columns iter: {[col for col in table.c]}')
        # print(f'Table columns iter: {[col for col in table.c]}')

        # Create an insert statement
        insert_stmt = table.insert().values(**data)

        if isinstance(pk_column_name, str):
            # Create an update statement
            update_stmt = table.update().where(table.c[pk_column_name] == data[pk_column_name]).values(**data)
        elif isinstance(pk_column_name, list):
            # Create an update statement with multiple columns
            conditions = [(table.c[col] == data[col]) for col in pk_column_name]
            update_stmt = table.update().where(*conditions).values(**data)

        # Execute the update statement
        result = self.execute_write(update_stmt)

        # If no rows were updated, perform an insert
        if result.rowcount == 0:
            result = self.execute_write(insert_stmt)

        return result

            # Commit the transaction
            # self.commit()

        # except IntegrityError as e:
        #     # Handle any integrity constraint violations or errors here
        #     self.rollback()
        #     # Log or raise an exception if needed

        # except Exception as e:
        #     # Handle other exceptions here
        #     self.rollback()
        #     # Log or raise an exception if needed

    @property
    def cn(self):  # Class name. Avoids having to print/log type(self).__name__.
        return type(self).__name__    

    @classmethod
    def readable_name(self):
        return f'{self._database.engine.host}.{self._database.engine.db}.{self.schema}.{self.table_name} table'
        

class ScenarioTable(BaseTable):
    """
    Table that can be rotated. Requires table to have data_dt and scenario columns
    """
    is_rotatable = True
    base_scenario = 'BASE'

    def _get_next_rotation(self, data_date=None, extra_where=None):
        """
        Get the next rotation number for a given data_date

        :param data_date: The data data
        :param extra_where: Optional extra where statement
        :return: Next rotation number or None if no data present
        """
        # Get max rotation
        if sqlalchemy.__version__ >= '2':
            stmt = sql.select(self.table_def.c.scenario.distinct())
        else:
            stmt = sql.select([self.table_def.c.scenario.distinct()])

        if data_date is not None:
            stmt = stmt.where(self.table_def.c.data_dt == data_date)

        if extra_where is not None:
            stmt = stmt.where(extra_where)

        data = self._database.execute_read(stmt)
        if not data.empty:
            # Default to 0 if no rotations present
            next_rotation = 0

            for scenario in data[self.table_def.c.scenario.name].tolist():
                # Check if matches BASE.X
                pattern = r'{}\.(\d+)'.format(self.base_scenario)
                match = re.match(pattern, scenario)
                if match:
                    rotation = match.groups()[0]
                    rotation = int(rotation)
                    next_rotation = max(next_rotation, rotation + 1)
        else:
            # Nothing to rotate
            next_rotation = None

        return next_rotation


    def rotate(self, data_date=None, extra_where=None, commit=False):
        """
        Rotate data with data_dt matching data_date. Updates scenario BASE to BASE.X.

        Optionally include additional fields through extra_where where required

        :param data_date: The data date
        :param extra_where: Optional extra where statement
        :param commit: Whether to commit (rollback if not set)
        :returns: The number of the newly created rotation or None if no data found
        """
        next_rotation = self._get_next_rotation(data_date, extra_where)
        data_date_str = '(no date provided)' if data_date is None else data_date.strftime('%Y-%m-%d')
        if next_rotation is not None:
            next_scenario = '{}.{}'.format(self.base_scenario, next_rotation)

            # Update base to be base.<max_rotation+1> if it exists
            stmt = sql.update(self.table_def).\
                        where(self.table_def.c.scenario == self.base_scenario).\
                        values(scenario=next_scenario)

            if data_date is not None:
                stmt = stmt.where(self.table_def.c.data_dt == data_date)

            if extra_where is not None:
                stmt = stmt.where(extra_where)

            updated_rows = self._database.execute_write(stmt, commit=commit)
            logging.debug(
                '%s: Rotated %d rows to %s for %s',
                self.table_name,
                updated_rows.rowcount,
                next_scenario,
                data_date_str
            )
        else:
            logging.debug('%s: No rows for date %s. Skipping rotate',
                            self.table_name, data_date_str)
        return next_rotation


    def read_base_scenario(self):
        """
        Read all entries with the latest scenario

        :returns: DataFrame
        """
        stmt = (
            sql.select([self.table_def])
            .where(self.c.scenario == self.base_scenario)
        )
        data = self.execute_read(stmt)
        return data


    def read_for_date(self, data_date):
        """
        Read all entries for a specific date and with the latest scenario

        :param data_date: The data date
        :returns: DataFrame
        """
        stmt = (
            sql.select([self.table_def])
            .where(self.c.scenario == self.base_scenario)
            .where(self.c.data_dt == data_date)
        )
        data = self.execute_read(stmt)
        return data


    def get_max_data_date(self, reference_date=None):
        """
        Find the max data date for the table, optionally from a reference date

        :param reference_date: Optional reference date
        :returns: Date
        """
        stmt = sql.select(sql.expression.func.max(self.c.data_dt))
        if reference_date:
            stmt = stmt.where(self.c.data_dt <= reference_date)
        result = self.execute_read(stmt)
        return None if result.empty else result.iloc[0, 0]


    def read_latest(self):
        """
        Read all data from latest snap

        :returns: Datafram of latest data
        """
        data_date = self.get_max_data_date()
        return self.read_for_date(data_date) if data_date else pd.DataFrame({})
