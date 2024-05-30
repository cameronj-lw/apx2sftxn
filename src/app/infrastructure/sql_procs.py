

# pypi
from sqlalchemy import sql
import pandas as pd

# native
from infrastructure.util.apxdb import wrap_in_session_procs
from infrastructure.util.database import execute_multi_query, BaseDB, get_pyodbc_conn
from infrastructure.util.stored_proc import BaseStoredProc


class APXRepDBpAPXReadSecurityHashProc(BaseStoredProc):
	config_section = 'apxrepdb'
	proc_name = 'pAPXReadSecurityHash'
	pk_column_name = 'SecurityID'

	def read(self, SecurityID=None):
		"""
		Execute stored proc, optionally with criteria

		:return: DataFrame
		"""
		base_db = BaseDB(self.config_section)

		with base_db.engine.connect() as conn:
			# Base query
			query = f"""exec dbo.{self.proc_name}"""

			# Optionally append SecurityID param
			if SecurityID:
				query = f"{query} @SecurityID = {SecurityID}"

			# Build statement
			statement = sql.text(query)

			# Execute
			result_set = conn.execute(statement)

			# Fetch all rows into a DataFrame
			df = pd.DataFrame(result_set.fetchall(), columns=result_set.keys())

		# Return df
		return df


class APXRepDBpReadvPortfolioBaseSettingExProc(BaseStoredProc):
	config_section = 'apxrepdb'
	proc_name = 'pReadvPortfolioBaseSettingEx'
	pk_column_name = 'PortfolioBaseID'

	def read(self, PortfolioBaseID=None):
		"""
		Execute stored proc, optionally with criteria

		:return: DataFrame
		"""
		base_db = BaseDB(self.config_section)

		with base_db.engine.connect() as conn:
			# Base query
			query = f"""exec dbo.{self.proc_name}"""

			# Optionally append PortfolioBaseID param
			if PortfolioBaseID:
				query = f"{query} @PortfolioBaseID = {PortfolioBaseID}"

			# Wrap in APX session procs
			query = wrap_in_session_procs(query)

			# Build statement
			statement = sql.text(query)

			# Execute
			result_set = conn.execute(statement)

			# Fetch all rows into a DataFrame
			df = pd.DataFrame(result_set.fetchall(), columns=result_set.keys())

		# Return df
		return df


class APXDBRealizedGainLossProcAndFunc(BaseStoredProc):
	config_section = 'apxdb'

	def read(self, Portfolios, FromDate, ToDate, ReportingCurrencyCode='PC'):
		"""
		Execute stored proc & function with specified criteria

		:return: DataFrame
		"""

		with get_pyodbc_conn(self.config_section) as conn:
			conn.autocommit = True

			# Base query
			query = f"""
			declare @ReportData varbinary(max)

			exec APXUser.pRealizedGainLoss
				@Portfolios = '{Portfolios}',
				@FromDate = '{FromDate}',
				@ToDate = '{ToDate}',
				@ReportingCurrencyCode = '{ReportingCurrencyCode}',
				@ReportData = @ReportData out

			select * from ApxUser.fRealizedGainLoss(@ReportData) rd
			"""

			# Wrap in APX session procs
			query = wrap_in_session_procs(query)

			results = execute_multi_query(conn, query)
			
			conn.close()

			return results[0]

	def __str__(self):
		return f'{self.config_section}-fRealizedGainLoss'


class APXDBTransactionActivityProcAndFunc(BaseStoredProc):
	config_section = 'apxdb'
	
	def read(self, Portfolios, FromDate, ToDate, ReportingCurrencyCode='PC'):
		"""
		Execute stored proc & function with specified criteria

		:return: DataFrame
		"""

		with get_pyodbc_conn(self.config_section) as conn:
			conn.autocommit = True

			# Base query
			query = f"""

			declare @ReportData varbinary(max)

			exec APXUser.pTransactionActivity
				@Portfolios = '{Portfolios}',
				@FromDate = '{FromDate}',
				@ToDate = '{ToDate}',
				@ReportingCurrencyCode = '{ReportingCurrencyCode}',
				@ReportData = @ReportData out

			select * from ApxUser.fTransactionActivity(@ReportData) rd

			"""

			# Wrap in APX session procs
			query = wrap_in_session_procs(query)

			results = execute_multi_query(conn, query)
			
			conn.close()

			return results[0]

	def __str__(self):
		return f'{self.config_section}-fTransactionActivity'