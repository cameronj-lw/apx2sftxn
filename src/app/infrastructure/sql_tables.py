
# core python
import logging

# pypi
import sqlalchemy
from sqlalchemy import sql

# native
from infrastructure.util.database import execute_multi_query, BaseDB
from infrastructure.util.table import BaseTable, ScenarioTable



""" APXDB """

class APXDBvPortfolioView(BaseTable):
	config_section = 'apxdb'
	schema = 'AdvApp'
	table_name = 'vPortfolio'

	def read(self, PortfolioID=None):
		"""
		Read all entries, optionally with criteria

		:return: DataFrame
		"""
		stmt = sql.select(self.table_def)
		if PortfolioID is not None:
			stmt = stmt.where(self.c.PortfolioID == PortfolioID)
		return self.execute_read(stmt)


class APXDBvPortfolioBaseView(BaseTable):
	config_section = 'apxdb'
	schema = 'AdvApp'
	table_name = 'vPortfolioBase'

	def read(self, PortfolioBaseID=None):
		"""
		Read all entries, optionally with criteria

		:return: DataFrame
		"""
		stmt = sql.select(self.table_def)
		if PortfolioBaseID is not None:
			stmt = stmt.where(self.c.PortfolioBaseID == PortfolioBaseID)
		return self.execute_read(stmt)


class APXDBvPortfolioBaseCustomView(BaseTable):
	config_section = 'apxdb'
	schema = 'AdvApp'
	table_name = 'vPortfolioBaseCustom'

	def read(self, PortfolioBaseID=None):
		"""
		Read all entries, optionally with criteria

		:return: DataFrame
		"""
		stmt = sql.select(self.table_def)
		if PortfolioBaseID is not None:
			stmt = stmt.where(self.c.PortfolioBaseID == PortfolioBaseID)
		return self.execute_read(stmt)


class APXDBvPortfolioBaseSettingExView(BaseTable):
	config_section = 'apxdb'
	schema = 'AdvApp'
	table_name = 'vPortfolioBaseSettingEx'

	def read(self, PortfolioBaseID=None):
		"""
		Read all entries, optionally with criteria

		:return: DataFrame
		"""
		stmt = sql.select(self.table_def)
		if PortfolioBaseID is not None:
			stmt = stmt.where(self.c.PortfolioBaseID == PortfolioBaseID)
		return self.execute_read(stmt)


class APXDBvCurrencyView(BaseTable):
	config_section = 'apxdb'
	schema = 'AdvApp'
	table_name = 'vCurrency'

	def read(self, CurrencyCode=None):
		"""
		Read all entries, optionally with criteria

		:return: DataFrame
		"""
		stmt = sql.select(self.table_def)
		if CurrencyCode is not None:
			stmt = stmt.where(self.c.CurrencyCode == CurrencyCode)
		return self.execute_read(stmt)


class APXDBvSecurityView(BaseTable):
	config_section = 'apxdb'
	schema = 'AdvApp'
	table_name = 'vSecurity'

	def read(self, SecurityID=None, SecUserDef1ID=None
				, SecUserDef2ID=None, SecUserDef3ID=None, IndustryGroupID=None, SectorID=None):
		"""
		Read all entries, optionally with criteria

		:return: DataFrame
		"""
		stmt = sql.select(self.table_def)
		if SecurityID is not None:
			stmt = stmt.where(self.c.SecurityID == SecurityID)
		if SecUserDef1ID is not None:
			stmt = stmt.where(self.c.SecUserDef1ID == SecUserDef1ID)
		if SecUserDef2ID is not None:
			stmt = stmt.where(self.c.SecUserDef2ID == SecUserDef2ID)
		if SecUserDef3ID is not None:
			stmt = stmt.where(self.c.SecUserDef3ID == SecUserDef3ID)
		if IndustryGroupID is not None:
			stmt = stmt.where(self.c.IndustryGroupID == IndustryGroupID)
		if SectorID is not None:
			stmt = stmt.where(self.c.SectorID == SectorID)
		return self.execute_read(stmt)


class APXDBvFXRateView(BaseTable):
	config_section = 'apxdb'
	schema = 'AdvApp'
	table_name = 'vFXRate'

	def read(self, NumeratorCurrCode=None, DenominatorCurrCode=None, AsOfDate=None):
		"""
		Read all entries, optionally with criteria

		:return: DataFrame
		"""
		stmt = sql.select(self.table_def)
		if NumeratorCurrCode is not None:
			stmt = stmt.where(self.c.NumeratorCurrCode == NumeratorCurrCode)
		if DenominatorCurrCode is not None:
			stmt = stmt.where(self.c.DenominatorCurrCode == DenominatorCurrCode)
		if AsOfDate is not None:
			stmt = stmt.where(self.c.AsOfDate == AsOfDate)
		return self.execute_read(stmt)


""" COREDB """

class COREDBSFTransactionTable(ScenarioTable):
	config_section = 'coredb'
	table_name = 'sf_transaction'


""" MGMTDB """

class MGMTDBMonitorTable(ScenarioTable):
	config_section = 'mgmtdb'
	table_name = 'monitor'

	def read(self, scenario=None, data_date=None, run_group=None, run_name=None, run_type=None, run_host=None, run_status_text=None):
		"""
		Read all entries, optionally with criteria

		:return: DataFrame
		"""
		stmt = sql.select(self.table_def)
		if scenario is not None:
			stmt = stmt.where(self.c.scenario == scenario)
		if data_date is not None:
			stmt = stmt.where(self.c.data_dt == data_date)
		if run_group is not None:
			stmt = stmt.where(self.c.run_group == run_group)
		if run_name is not None:
			stmt = stmt.where(self.c.run_name == run_name)
		if run_type is not None:
			stmt = stmt.where(self.c.run_type == run_type)
		if run_host is not None:
			stmt = stmt.where(self.c.run_host == run_host)
		if run_status_text is not None:
			stmt = stmt.where(self.c.run_status_text == run_status_text)
		return self.execute_read(stmt)

	def read_for_date(self, data_date):
		"""
		Read all entries for a specific date

		:param data_date: The data date
		:returns: DataFrame
		"""
		stmt = (
			sql.select(self.table_def)
			.where(self.c.data_dt == data_date)
		)
		data = self.execute_read(stmt)
		return data



