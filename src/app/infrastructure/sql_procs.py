

# pypi
from sqlalchemy import sql
import pandas as pd

# native
from infrastructure.util.database import execute_multi_query, BaseDB
from infrastructure.util.stored_proc import BaseStoredProc


class APXRepDBpAPXReadSecurityHashProc(BaseStoredProc):
	config_section = 'apxrepdb'
	proc_name = 'pAPXReadSecurityHash'

	def read(self, SecurityID=None):
		"""
		Execute stored proc, optionally with criteria

		:return: DataFrame
		"""
		base_db = BaseDB(self.config_section)

		with base_db.engine.connect() as conn:
			# Base query
			query = """exec dbo.pAPXReadSecurityHash"""

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