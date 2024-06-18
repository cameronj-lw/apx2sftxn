
# core python
import logging

# pypi
import pandas as pd
import sqlalchemy
from sqlalchemy import sql

# native
from infrastructure.util.apxdb import wrap_in_session_procs
from infrastructure.util.database import execute_multi_query, BaseDB, get_pyodbc_conn
from infrastructure.util.table import BaseTable, ScenarioTable



""" APXDB """

class APXDBvPortfolioView(BaseTable):
	config_section = 'apxdb'
	schema = 'AdvApp'
	table_name = 'vPortfolio'
	pk_column_name = 'PortfolioID'

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
	pk_column_name = 'PortfolioBaseID'

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
	pk_column_name = 'PortfolioBaseID'

	def read(self, PortfolioBaseID=None):
		"""
		Read all entries, optionally with criteria

		:return: DataFrame
		"""
		stmt = sql.select(self.table_def)
		if PortfolioBaseID is not None:
			stmt = stmt.where(self.c.PortfolioBaseID == PortfolioBaseID)
		return self.execute_read(stmt)


class APXDBvPortfolioSettingExView(BaseTable):
	config_section = 'apxdb'
	schema = 'APXUser'
	table_name = 'vPortfolioSettingEx'
	pk_column_name = 'PortfolioID'

	def read(self, PortfolioID=None):
	
		with get_pyodbc_conn(self.config_section) as conn:
			conn.autocommit = True

			# Base query
			query = f"select * from {self.schema}.{self.table_name}"
			if PortfolioID:
				query += f" where PortfolioID={PortfolioID}"

			# Wrap in APX session procs
			query = wrap_in_session_procs(query)

			results = execute_multi_query(conn, query)
			
			conn.close()

			return results[0]


class APXDBvPortfolioBaseSettingExView(BaseTable):
	config_section = 'apxdb'
	schema = 'APXUser'
	table_name = 'vPortfolioBaseSettingEx'
	pk_column_name = 'PortfolioBaseID'

	def read(self, PortfolioBaseID=None):
	
		with get_pyodbc_conn(self.config_section) as conn:
			conn.autocommit = True

			# Base query
			query = f"select * from {self.schema}.{self.table_name}"
			if PortfolioBaseID:
				query += f" where PortfolioBaseID={PortfolioBaseID}"

			# Wrap in APX session procs
			query = wrap_in_session_procs(query)

			results = execute_multi_query(conn, query)
			
			conn.close()

			return results[0]


class APXDBvCurrencyView(BaseTable):
	config_section = 'apxdb'
	schema = 'AdvApp'
	table_name = 'vCurrency'
	pk_column_name = 'CurrencyCode'

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
	pk_column_name = 'SecurityID'

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

	def read(self, PriceDate=None, NumeratorCurrencyCode=None, DenominatorCurrencyCode=None, AsOfDate=None):
		"""
		Read all entries, optionally with criteria

		:return: DataFrame
		"""
		stmt = sql.select(self.table_def)
		if PriceDate is not None:
			stmt = stmt.where(self.c.PriceDate == PriceDate)
		if NumeratorCurrencyCode is not None:
			stmt = stmt.where(self.c.NumeratorCurrencyCode == NumeratorCurrencyCode)
		if DenominatorCurrencyCode is not None:
			stmt = stmt.where(self.c.DenominatorCurrencyCode == DenominatorCurrencyCode)
		if AsOfDate is not None:
			stmt = stmt.where(self.c.AsOfDate == AsOfDate)
		return self.execute_read(stmt)


class APXDBvCustodianView(BaseTable):
	config_section = 'apxdb'
	schema = 'AdvApp'
	table_name = 'vCustodian'

	def read(self, CustodianID=None):
		"""
		Read all entries, optionally with criteria

		:return: DataFrame
		"""
		stmt = sql.select(self.table_def)
		if CustodianID is not None:
			stmt = stmt.where(self.c.CustodianID == CustodianID)
		return self.execute_read(stmt)


""" APXREPDB """


class APXRepDBvStmtGroupByPortfolioView(BaseTable):
	# TODO_CLEANUP: remove this once not used
	config_section = 'apxrepdb'
	schema = 'dbo'
	table_name = 'vStmtGroupByPortfolio'

	def read(self, PortfolioID=None, PortfolioCode=None, PortfolioGroupID=None, PortfolioGroupCode=None):
		"""
		Read all entries, optionally with criteria

		:return: DataFrame
		"""
		stmt = sql.select(self.table_def)
		if PortfolioID is not None:
			stmt = stmt.where(self.c.PortfolioID == PortfolioID)
		if PortfolioCode is not None:
			stmt = stmt.where(self.c.PortfolioCode == PortfolioCode)
		if PortfolioGroupID is not None:
			stmt = stmt.where(self.c.PortfolioGroupID == PortfolioGroupID)
		if PortfolioGroupCode is not None:
			stmt = stmt.where(self.c.PortfolioGroupCode == PortfolioGroupCode)
		return self.execute_read(stmt)


class APXRepDBvPortfolioAndStmtGroupCurrencyView(BaseTable):
	config_section = 'apxrepdb'
	schema = 'dbo'
	table_name = 'vPortfolioAndStmtGroupCurrency'

	def read(self, PortfolioID=None, PortfolioCode=None, PortfolioGroupID=None, PortfolioGroupCode=None):
		"""
		Read all entries, optionally with criteria

		:return: DataFrame
		"""
		stmt = sql.select(self.table_def)
		if PortfolioID is not None:
			stmt = stmt.where(self.c.PortfolioID == PortfolioID)
		if PortfolioCode is not None:
			stmt = stmt.where(self.c.PortfolioCode == PortfolioCode)
		if PortfolioGroupID is not None:
			stmt = stmt.where(self.c.PortfolioGroupID == PortfolioGroupID)
		if PortfolioGroupCode is not None:
			stmt = stmt.where(self.c.PortfolioGroupCode == PortfolioGroupCode)
		return self.execute_read(stmt)


class APXRepDBLWTxnSummaryTable(ScenarioTable):
	config_section = 'apxrepdb'
	schema = 'dbo'
	table_name = 'LW_txn_summary'

	def read(self, scenario=None, data_handle=None, portfolio_code=None, from_date=None, to_date=None):
		"""
		Read all entries, optionally with criteria

		:return: DataFrame
		"""
		stmt = sql.select(self.table_def)
		if scenario is not None:
			stmt = stmt.where(self.c.scenario == scenario)
		if data_handle is not None:
			stmt = stmt.where(self.c.data_handle == data_handle)
		if portfolio_code is not None:
			stmt = stmt.where(self.c.portfolio_code == portfolio_code)
		if from_date is not None:
			stmt = stmt.where(self.c.trade_date >= from_date) if to_date else stmt.where(self.c.trade_date == from_date)
		if to_date is not None:
			stmt = stmt.where(self.c.trade_date <= to_date) if from_date else stmt.where(self.c.trade_date == to_date)
		return self.execute_read(stmt)


""" LWDB """

class LWDBCalendarTable(ScenarioTable):
	config_section = 'lwdb'
	table_name = 'calendar'

	def read_for_date(self, data_date):
		"""
		Read all entries for a specific date and with the latest scenario

		:param data_date: The data date
		:return: DataFrame
		"""
		if sqlalchemy.__version__ >= '2':
			stmt = sql.select(self.table_def)
		else:
			stmt = sql.select([self.table_def])
		stmt = (
			stmt
			.where(self.c.scenario == self.base_scenario)
			.where(self.c.data_dt == data_date)
		)
		data = self.execute_read(stmt)
		return data


class LWDBAPXAppraisalTable(ScenarioTable):
	config_section = 'lwdb'
	table_name = 'apx_appraisal'

	def read(self, scenario=None, data_dt=None, PortfolioCode=None, SecurityID=None, SecuritySymbol=None, ProprietarySymbol=None):
		"""
		Read all entries, optionally with criteria

		:return: DataFrame
		"""
		stmt = sql.select(self.table_def)
		if scenario is not None:
			stmt = stmt.where(self.c.scenario == scenario)
		else:
			stmt = stmt.where(self.c.scenario == self.base_scenario)
		if data_dt is not None:
			stmt = stmt.where(self.c.data_dt == data_dt)
		if PortfolioCode is not None:
			stmt = stmt.where(self.c.PortfolioCode == PortfolioCode)
		if SecurityID is not None:
			stmt = stmt.where(self.c.SecurityID == SecurityID)
		if SecuritySymbol is not None:
			stmt = stmt.where(self.c.SecuritySymbol == SecuritySymbol)
		if ProprietarySymbol is not None:
			stmt = stmt.where(self.c.ProprietarySymbol == ProprietarySymbol)
		return self.execute_read(stmt)


class LWDBAPXAppraisalTable_prodlwdb(ScenarioTable):
	# TODO_CLEANUP: remove once not used
	config_section = 'lwdb_prod'
	table_name = 'apx_appraisal'

	def read(self, scenario=None, data_dt=None, PortfolioCode=None, SecurityID=None, SecuritySymbol=None, ProprietarySymbol=None):
		"""
		Read all entries, optionally with criteria

		:return: DataFrame
		"""
		stmt = sql.select(self.table_def)
		if scenario is not None:
			stmt = stmt.where(self.c.scenario == scenario)
		else:
			stmt = stmt.where(self.c.scenario == self.base_scenario)
		if data_dt is not None:
			stmt = stmt.where(self.c.data_dt == data_dt)
		if PortfolioCode is not None:
			stmt = stmt.where(self.c.PortfolioCode == PortfolioCode)
		if SecurityID is not None:
			stmt = stmt.where(self.c.SecurityID == SecurityID)
		if SecuritySymbol is not None:
			stmt = stmt.where(self.c.SecuritySymbol == SecuritySymbol)
		if ProprietarySymbol is not None:
			stmt = stmt.where(self.c.ProprietarySymbol == ProprietarySymbol)
		return self.execute_read(stmt)


class LWDBAPXAppraisalTable_uatlwdb(ScenarioTable):
	# TODO_CLEANUP: remove once not used
	config_section = 'lwdb_uat'
	table_name = 'apx_appraisal'

	def read(self, scenario=None, data_dt=None, PortfolioCode=None, SecurityID=None, SecuritySymbol=None, ProprietarySymbol=None):
		"""
		Read all entries, optionally with criteria

		:return: DataFrame
		"""
		stmt = sql.select(self.table_def)
		if scenario is not None:
			stmt = stmt.where(self.c.scenario == scenario)
		else:
			stmt = stmt.where(self.c.scenario == self.base_scenario)
		if data_dt is not None:
			stmt = stmt.where(self.c.data_dt == data_dt)
		if PortfolioCode is not None:
			stmt = stmt.where(self.c.PortfolioCode == PortfolioCode)
		if SecurityID is not None:
			stmt = stmt.where(self.c.SecurityID == SecurityID)
		if SecuritySymbol is not None:
			stmt = stmt.where(self.c.SecuritySymbol == SecuritySymbol)
		if ProprietarySymbol is not None:
			stmt = stmt.where(self.c.ProprietarySymbol == ProprietarySymbol)
		return self.execute_read(stmt)


class LWDBSFTransactionTable(BaseTable):
	config_section = 'lwdb'
	table_name = 'sf_transaction'

	def read(self, portfolio_code=None, from_date=None, to_date=None):
		"""
		Read all entries, optionally with criteria

		:return: DataFrame
		"""
		stmt = sql.select(self.table_def)
		if portfolio_code is not None:
			stmt = stmt.where(self.c.portfolio_code == portfolio_code)
		if from_date is not None:
			stmt = stmt.where(self.c.trade_date__c >= from_date) if to_date else stmt.where(self.c.trade_date__c == from_date)
		if to_date is not None:
			stmt = stmt.where(self.c.trade_date__c <= to_date) if from_date else stmt.where(self.c.trade_date__c == to_date)
		return self.execute_read(stmt)


""" COREDB """

	
class COREDBAPXfTransactionActivityQueueTable(BaseTable):
	config_section = 'coredb'
	table_name = 'apx_fTransactionActivity_queue'

	def read(self, portfolio_code=None, trade_date=None, queue_status=None):
		"""
		Read all entries, optionally with criteria

		:return: DataFrame
		"""
		stmt = sql.select(self.table_def)
		if portfolio_code is not None:
			stmt = stmt.where(self.c.portfolio_code == portfolio_code)
		if trade_date is not None:
			stmt = stmt.where(self.c.trade_date == trade_date)
		if queue_status is not None:
			stmt = stmt.where(self.c.queue_status == queue_status)
		stmt = stmt.order_by(self.c.trade_date).order_by(self.c.portfolio_code)
		return self.execute_read(stmt)

	
class COREDBAPXfRealizedGainLossQueueTable(BaseTable):
	config_section = 'coredb'
	table_name = 'apx_fRealizedGainLoss_queue'

	def read(self, portfolio_code=None, trade_date=None, queue_status=None):
		"""
		Read all entries, optionally with criteria

		:return: DataFrame
		"""
		stmt = sql.select(self.table_def)
		if portfolio_code is not None:
			stmt = stmt.where(self.c.portfolio_code == portfolio_code)
		if trade_date is not None:
			stmt = stmt.where(self.c.trade_date == trade_date)
		if queue_status is not None:
			stmt = stmt.where(self.c.queue_status == queue_status)
		stmt = stmt.order_by(self.c.trade_date).order_by(self.c.portfolio_code)
		return self.execute_read(stmt)

	
class COREDBLWTransactionSummaryQueueTable(BaseTable):
	config_section = 'coredb'
	table_name = 'lw_transaction_summary_queue'

	def read(self, portfolio_code=None, trade_date=None, queue_status=None):
		"""
		Read all entries, optionally with criteria

		:return: DataFrame
		"""
		stmt = sql.select(self.table_def)
		if portfolio_code is not None:
			stmt = stmt.where(self.c.portfolio_code == portfolio_code)
		if trade_date is not None:
			stmt = stmt.where(self.c.trade_date == trade_date)
		if queue_status is not None:
			stmt = stmt.where(self.c.queue_status == queue_status)
		stmt = stmt.order_by(self.c.trade_date).order_by(self.c.portfolio_code)
		return self.execute_read(stmt)

	
class COREDBAPX2SFTxnQueueTable(BaseTable):
	config_section = 'coredb'
	table_name = 'lw_apx2sf_txn_queue'

	def read(self, portfolio_code=None, trade_date=None, queue_status=None):
		"""
		Read all entries, optionally with criteria

		:return: DataFrame
		"""
		stmt = sql.select(self.table_def)
		if portfolio_code is not None:
			stmt = stmt.where(self.c.portfolio_code == portfolio_code)
		if trade_date is not None:
			stmt = stmt.where(self.c.trade_date == trade_date)
		if queue_status is not None:
			stmt = stmt.where(self.c.queue_status == queue_status)
		stmt = stmt.order_by(self.c.trade_date).order_by(self.c.portfolio_code)
		return self.execute_read(stmt)

	
class COREDBAPXfRealizedGainLossTable(BaseTable):
	config_section = 'coredb'
	table_name = 'apx_fRealizedGainLoss'

	def read(self, portfolio_id=None, portfolio_code=None, from_date=None, to_date=None
				, PortfolioTransactionID=None, TranID=None, LotNumber=None):
		"""
		Read all entries, optionally with criteria

		:return: DataFrame
		"""
		stmt = sql.select(self.table_def)
		if portfolio_id is not None:
			stmt = stmt.where(self.c.portfolio_id == portfolio_id)
		if portfolio_code is not None:
			stmt = stmt.where(self.c.portfolio_code == portfolio_code)
		if from_date is not None:
			stmt = stmt.where(self.c.CloseDate >= from_date) if to_date else stmt.where(self.c.CloseDate == from_date)
		if to_date is not None:
			stmt = stmt.where(self.c.CloseDate <= to_date) if from_date else stmt.where(self.c.CloseDate == to_date)
		if PortfolioTransactionID is not None:
			stmt = stmt.where(self.c.PortfolioTransactionID == PortfolioTransactionID)
		if TranID is not None:
			stmt = stmt.where(self.c.TranID == TranID)
		if LotNumber is not None:
			stmt = stmt.where(self.c.LotNumber == LotNumber)
		return self.execute_read(stmt)

	
class COREDBAPXfTransactionActivityTable(BaseTable):
	config_section = 'coredb'
	table_name = 'apx_fTransactionActivity'

	def read(self, portfolio_id=None, portfolio_code=None, from_date=None, to_date=None):
		"""
		Read all entries, optionally with criteria

		:return: DataFrame
		"""
		stmt = sql.select(self.table_def)
		if portfolio_id is not None:
			stmt = stmt.where(self.c.portfolio_id == portfolio_id)
		if portfolio_code is not None:
			stmt = stmt.where(self.c.portfolio_code == portfolio_code)
		if from_date is not None:
			stmt = stmt.where(self.c.TradeDate >= from_date) if to_date else stmt.where(self.c.TradeDate == from_date)
		if to_date is not None:
			stmt = stmt.where(self.c.TradeDate <= to_date) if from_date else stmt.where(self.c.TradeDate == to_date)
		return self.execute_read(stmt)


class COREDBLWTxnSummaryTable(BaseTable):
	config_section = 'coredb'
	table_name = 'LW_txn_summary'

	def read(self, portfolio_code=None, from_date=None, to_date=None):
		"""
		Read all entries, optionally with criteria

		:return: DataFrame
		"""
		stmt = sql.select(self.table_def)
		if portfolio_code is not None:
			stmt = stmt.where(self.c.portfolio_code == portfolio_code)
		if from_date is not None:
			stmt = stmt.where(self.c.trade_date >= from_date) if to_date else stmt.where(self.c.trade_date == from_date)
		if to_date is not None:
			stmt = stmt.where(self.c.trade_date <= to_date) if from_date else stmt.where(self.c.trade_date == to_date)
		return self.execute_read(stmt)

	
class COREDBLWTransactionSummaryTable(BaseTable):
	config_section = 'coredb'
	table_name = 'lw_transaction_summary'

	def read(self, portfolio_id=None, portfolio_code=None, trade_date=None):
		"""
		Read all entries, optionally with criteria

		:return: DataFrame
		"""
		stmt = sql.select(self.table_def)
		if portfolio_id is not None:
			stmt = stmt.where(self.c.portfolio_id == portfolio_id)
		if portfolio_code is not None:
			stmt = stmt.where(self.c.portfolio_code == portfolio_code)
		if trade_date is not None:
			stmt = stmt.where(self.c.trade_date == trade_date)
		return self.execute_read(stmt)


class COREDBSFTransactionTable(BaseTable):
	config_section = 'coredb'
	table_name = 'sf_transaction'

	def read(self, portfolio_code=None, from_date=None, to_date=None):
		"""
		Read all entries, optionally with criteria

		:return: DataFrame
		"""
		stmt = sql.select(self.table_def)
		if portfolio_code is not None:
			stmt = stmt.where(self.c.portfolio_code == portfolio_code)
		if from_date is not None:
			stmt = stmt.where(self.c.trade_date__c >= from_date) if to_date else stmt.where(self.c.trade_date__c == from_date)
		if to_date is not None:
			stmt = stmt.where(self.c.trade_date__c <= to_date) if from_date else stmt.where(self.c.trade_date__c == to_date)
		return self.execute_read(stmt)


class CoreDBSFPortfolioLatestView(BaseTable):
	config_section = 'coredb'
	table_name = 'sf_portfolio_latest'

	def read(self, LW_Portfolio_ID__c=None):
		"""
		Read all entries, optionally with criteria

		:return: DataFrame
		"""
		stmt = sql.select(self.table_def)
		if LW_Portfolio_ID__c is not None:
			stmt = stmt.where(self.c.LW_Portfolio_ID__c == LW_Portfolio_ID__c)
		return self.execute_read(stmt)


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



