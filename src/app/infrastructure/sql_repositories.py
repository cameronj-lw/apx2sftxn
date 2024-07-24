
# core python
from abc import abstractmethod
import datetime
import logging
import os
import socket
from typing import Any, Dict, List, Tuple, Union

# pypi
import pandas as pd
from sqlalchemy import sql

# native
from domain.models import Heartbeat, Transaction, TransactionProcessingQueueItem, QueueStatus, PKColumnMapping
from domain.python_tools import get_current_callable
from domain.repositories import HeartbeatRepository, TransactionRepository, TransactionProcessingQueueRepository, SupplementaryRepository
# from infrastructure.in_memory_repositories import CoreDBRealizedGainLossInMemoryRepository
from infrastructure.models import MGMTDBHeartbeat, Txn2TableColMap
from infrastructure.sql_procs import APXDBRealizedGainLossProcAndFunc, APXDBTransactionActivityProcAndFunc
from infrastructure.sql_tables import (
    APXRepDBLWTxnSummaryTable,
    MGMTDBMonitorTable,      
    COREDBAPXfRealizedGainLossQueueTable, COREDBAPXfRealizedGainLossTable, 
    COREDBAPXfTransactionActivityQueueTable, COREDBAPXfTransactionActivityTable, 
    COREDBLWTransactionSummaryQueueTable, LWDBAPXAppraisalTable, COREDBLWTransactionSummaryTable, COREDBLWTxnSummaryTable,
    COREDBAPX2SFTxnQueueTable, COREDBSFTransactionTable,
    # LWDBSFPortfolioTable,
)
from infrastructure.util.date import get_previous_bday
from infrastructure.util.math import normal_round


""" MGMTDB """

class MGMTDBHeartbeatRepository(HeartbeatRepository):
    # Specify which class callers are recommended to use when instantiating heartbeat instances
    heartbeat_class = MGMTDBHeartbeat
    table = MGMTDBMonitorTable()

    def create(self, heartbeat: Heartbeat) -> int:

        # Create heartbeat instance
        hb_dict = heartbeat.to_dict()

        # Populate with table base scenario
        hb_dict['scenario'] = self.table.base_scenario

        # Truncate asofuser if needed
        hb_dict['asofuser'] = (hb_dict['asofuser'] if len(hb_dict['asofuser']) <= 32 else hb_dict['asofuser'][:32])

        # Columns used as basis for upsert
        pk_columns = ['data_dt', 'scenario', 'run_group', 'run_name', 'run_type', 'run_host', 'run_status_text']

        # Remove columns not in the table def
        hb_dict = {k: hb_dict[k] for k in hb_dict if k in self.table.c.keys()}

        # Bulk insert new rows:
        logging.debug(f"{self.cn}: About to upsert {hb_dict}")
        res = self.table.upsert(pk_column_name=pk_columns, data=hb_dict)  # TODO: error handling?
        if isinstance(res, int):
            row_cnt = res
        else:
            row_cnt = res.rowcount
        if abs(row_cnt) != 1:
            raise UnexpectedRowCountException(f"Expected 1 row to be saved, but there were {row_cnt}!")
        logging.debug(f'End of {self.cn} create: {heartbeat}')
        return row_cnt

    def get(self, data_date: Union[datetime.date,None]=None, group: Union[str,None]=None, name: Union[str,None]=None) -> List[Heartbeat]:
        # Query table - returns result into df:
        query_result = self.table.read(scenario=self.table.base_scenario, data_date=data_date, run_group=group, run_name=name, run_type='INFO', run_status_text='HEARTBEAT')
        
        # Convert to dict:
        query_result_dicts = query_result.to_dict('records')

        # Create list of heartbeats
        heartbeats = [self.heartbeat_class.from_dict(qrd) for qrd in query_result_dicts]

        # Return result heartbeats list
        return heartbeats

    @classmethod
    def readable_name(self):
        return 'MGMTDB Monitor table'


""" LWDB """

class LWDBAPXAppraisalPrevBdayRepository(SupplementaryRepository):
    table = LWDBAPXAppraisalTable()
    relevant_columns = ['data_dt', 'portfolio_code', 'SecurityID', 'LocalCostPerUnit', 'RptCostPerUnit']

    def __init__(self):
        super().__init__(pk_columns=[
                PKColumnMapping('TradeDate'), 
                PKColumnMapping('portfolio_code', 'PortfolioCode'),
                PKColumnMapping('SecurityID1', 'SecurityID'),
            ])
    
    def create(self, data: Dict[str, Any]) -> int:
        raise NotImplementedError(f'Cannot save to {self.cn}!') 

    def get(self, pk_column_values: Dict[str, Any]) -> dict:
        # We want to get appraisal data from prev bday before TradeDate:
        trade_date = pk_column_values.get('TradeDate')
        prev_bday = get_previous_bday(trade_date)

        # Query table
        res_df = self.table.read(data_dt=prev_bday, PortfolioCode=pk_column_values.get('PortfolioCode'), SecurityID=pk_column_values.get('SecurityID'))
        res_df['portfolio_code'] = res_df['PortfolioCode']
        
        # Add calculated columns
        res_df['LocalCostPerUnit'] = res_df['LocalUnadjustedCostBasis'] / res_df['Quantity']
        res_df['RptCostPerUnit'] = res_df['UnadjustedCostBasis'] / res_df['Quantity']
        res_dicts = res_df[self.relevant_columns].to_dict('records')

        # Should be 1 row max... sanity check... # TODO_EH: what if this has more than one row?
        if len(res_dicts) > 1: 
            logging.info(f"Found multiple rows in {self.table.cn} for {prev_bday} {pk_column_values.get('PortfolioCode')} {pk_column_values.get('SecurityID')}!")
            return res_dicts[0]
        elif len(res_dicts):
            return res_dicts[0]
        else:
            logging.debug(f"Found 0 rows in {self.table.cn} for {prev_bday} {pk_column_values.get('PortfolioCode')} {pk_column_values.get('SecurityID')}!")
            return {}

    def supplement(self, transaction: Transaction):
        super().supplement(transaction)

        # Additionally, update the transaction's per-unit values:
        transaction.LocalCostBasis = transaction.LocalCostPerUnit * transaction.Quantity
        transaction.RptCostBasis = transaction.RptCostPerUnit * transaction.Quantity


# TODO: implement below class, or remove
# class LWDBSFPortfolioRepository(SupplementaryRepository):
#     table = LWDBSFPortfolioTable()
#     readable_name = self.table.readable_name
#     relevant_columns = ['data_dt', 'portfolio_code', 'SecurityID', 'LocalCostPerUnit', 'RptCostPerUnit']

#     def __init__(self):
#         super().__init__(pk_columns=[
#                 PKColumnMapping('TradeDate'), 
#                 PKColumnMapping('portfolio_code', 'PortfolioCode'),
#                 PKColumnMapping('SecurityID1', 'SecurityID'),
#             ])
    
#     def create(self, data: Dict[str, Any]) -> int:
#         raise NotImplementedError(f'Cannot save to {self.cn}!') 

#     def get(self, pk_column_values: Dict[str, Any]) -> dict:
#         # We want to get appraisal data from prev bday before TradeDate:
#         trade_date = pk_column_values.get('TradeDate')
#         prev_bday = get_previous_bday(trade_date)

#         # Query table
#         res_df = self.table.read(data_dt=prev_bday, PortfolioCode=pk_column_values.get('PortfolioCode'), SecurityID=pk_column_values.get('SecurityID'))
#         res_df['portfolio_code'] = res_df['PortfolioCode']
        
#         # Add calculated columns
#         res_df['LocalCostPerUnit'] = res_df['LocalUnadjustedCostBasis'] / res_df['Quantity']
#         res_df['RptCostPerUnit'] = res_df['UnadjustedCostBasis'] / res_df['Quantity']
#         res_dicts = res_df[self.relevant_columns].to_dict('records')

#         # Should be 1 row max... sanity check... # TODO_EH: what if this has more than one row?
#         if len(res_dicts) > 1: 
#             logging.info(f"Found multiple rows in {self.table.cn} for {prev_bday} {pk_column_values.get('PortfolioCode')} {pk_column_values.get('SecurityID')}!")
#             return res_dicts[0]
#         elif len(res_dicts):
#             return res_dicts[0]
#         else:
#             logging.info(f"Found 0 rows in {self.table.cn} for {prev_bday} {pk_column_values.get('PortfolioCode')} {pk_column_values.get('SecurityID')}!")
#             return {}

#     def supplement(self, transaction: Transaction):
#         super().supplement(transaction)

#         # Additionally, update the transaction's per-unit values:
#         transaction.LocalCostBasis = transaction.LocalCostPerUnit * transaction.Quantity
#         transaction.RptCostBasis = transaction.RptCostPerUnit * transaction.Quantity

""" APXDB """

class APXDBRealizedGainLossRepository(TransactionRepository):
    proc = APXDBRealizedGainLossProcAndFunc()
    
    def create(self, transactions: Union[List[Transaction],Transaction]) -> int:
        raise NotImplementedError(f'Cannot create in {self.cn}!')

    def get(self, portfolio_code: Union[str,None]=None, trade_date: Union[datetime.date, Tuple[datetime.date, datetime.date], None]=None) -> List[Transaction]:
        # Infer from & to dates, based on trade_date type
        if isinstance(trade_date, tuple):
            from_date, to_date = trade_date
        elif isinstance(trade_date, datetime.date):
            from_date = to_date = trade_date
        else:
            from_date = to_date = None

        # read source proc, convert to Transactions, return them
        res_df = self.proc.read(Portfolios=portfolio_code, FromDate=from_date, ToDate=to_date)
        transactions = [Transaction(**d) for d in res_df.to_dict('records')]
        return transactions

    def __str__(self):
        return str(self.proc)

# TODO_CLEANUP: remove once not used
# class APXDBTransactionActivityRepository_OLD(TransactionRepository):
#     proc = APXDBTransactionActivityProcAndFunc()
    
#     def create(self, transactions: Union[List[Transaction],Transaction]) -> int:
#         raise NotImplementedError(f'Cannot create in {self.cn}!')

#     def get(self, portfolio_code: Union[str,None]=None, trade_date: Union[datetime.date, Tuple[datetime.date, datetime.date], None]=None) -> List[Transaction]:
#         # Infer from & to dates, based on trade_date type
#         if isinstance(trade_date, tuple):
#             from_date, to_date = trade_date
#         elif isinstance(trade_date, datetime.date):
#             from_date = to_date = trade_date
#         else:
#             from_date = to_date = None

#         # read source proc, convert to Transactions, return them
#         res_df = self.proc.read(Portfolios=portfolio_code, FromDate=from_date, ToDate=to_date)
#         transactions = [Transaction(**d) for d in res_df.to_dict('records')]
#         return transactions

#     def __str__(self):
#         return str(self.proc)


class APXDBTransactionActivityRepository(TransactionRepository):
    txn_source = APXDBTransactionActivityProcAndFunc()  # COREDBAPXfTransactionActivityTable()  # APXDBTransactionActivityProcAndFunc()
    realized_gains_source = COREDBAPXfRealizedGainLossTable()  # APXDBRealizedGainLossProcAndFunc()
    
    def create(self, transactions: Union[List[Transaction],Transaction]) -> int:
        raise NotImplementedError(f'Cannot create in {self.cn}!')

    def get_raw(self, portfolio_code: Union[str,None]=None, trade_date: Union[datetime.date, Tuple[datetime.date, datetime.date], None]=None) -> List[Transaction]:
        # Infer from & to dates, based on trade_date type
        if isinstance(trade_date, tuple):
            from_date, to_date = trade_date
            historical_from_date = from_date + datetime.timedelta(days=-70)
        elif isinstance(trade_date, datetime.date):
            from_date = to_date = trade_date
            historical_from_date = from_date + datetime.timedelta(days=-70)
        else:
            return []  # TODO_EH: exception?

        # Get source transactions, including historical
        res_df = self.txn_source.read(Portfolios=portfolio_code, FromDate=historical_from_date, ToDate=to_date)
        # res_df = self.txn_source.read(portfolio_code=portfolio_code, from_date=historical_from_date, to_date=to_date)
        transactions = [Transaction(**d) for d in res_df.to_dict('records')]
        return transactions
        
    def get(self, portfolio_code: Union[str,None]=None, trade_date: Union[datetime.date, Tuple[datetime.date, datetime.date], None]=None) -> List[Transaction]:
        # Infer from & to dates, based on trade_date type
        if isinstance(trade_date, tuple):
            from_date, to_date = trade_date
            historical_from_date = from_date + datetime.timedelta(days=-70)
        elif isinstance(trade_date, datetime.date):
            from_date = to_date = trade_date
            historical_from_date = from_date + datetime.timedelta(days=-70)
        else:
            return []  # TODO_EH: exception?

        # Get source transactions, including historical
        res_df = self.txn_source.read(Portfolios=portfolio_code, FromDate=historical_from_date, ToDate=to_date)
        # res_df = self.txn_source.read(portfolio_code=portfolio_code, from_date=historical_from_date, to_date=to_date)
        transactions = [Transaction(**d) for d in res_df.to_dict('records')]
        
        # Find dividends with SETTLE date within the specified trade_date range
        dividends = [t for t in transactions 
            if from_date <= t.SettleDate.date() <= to_date and t.TransactionCode == 'dv']

        # Read realized gains proc once (avoids reading it for every dividend separately)
        # realized_gains_df = self.realized_gains_source.read(Portfolios=portfolio_code, FromDate=historical_from_date, ToDate=to_date)
        realized_gains_df = self.realized_gains_source.read(portfolio_code=portfolio_code, from_date=historical_from_date, to_date=to_date)

        for dv in dividends:
            # APXTxns.pm line 353: jam on the LW blinders:  dv are all about SettleDate
            dv.TradeDate = dv.SettleDate

            # APXTxns.pm line 453-464: Find a wd matching the settle date, security (i.e. divacc), and having very similar amount
            wd_candidates = [t for t in transactions if t.TransactionCode in ('wd', 'dp')] 
            wd_candidates = [t for t in wd_candidates if t.SettleDate == dv.SettleDate]
            wd_candidates = [t for t in wd_candidates if t.SecurityID1 == dv.SecurityID2]
            wd_candidates = [t for t in wd_candidates if abs(t.TradeAmountLocal - dv.TradeAmountLocal) < 0.015]  # APXTxns.pm line 11

            if len(wd_candidates):
                wd = wd_candidates[0]

                # Remove this one from the transactions, since it has been "merged" into the dividend
                # transactions = [t for t in transactions if t != wd]
                dv.add_lineage(f'{dv.TransactionCode} -> Merged dp/wd {wd.PortfolioTransactionID}... but did not remove the wd... see APXTxns.pm line 462', source_callable=get_current_callable())

            # APXTxns.pm line 465-519: Find a sl from cash to cash, and having very similar amount
            sl_candidates = [t for t in transactions if t.TransactionCode == 'sl']
            sl_candidates = [t for t in sl_candidates if t.SettleDate == dv.SettleDate]
            sl_candidates = [t for t in sl_candidates if t.SecTypeCode1 == 'ca']
            sl_candidates = [t for t in sl_candidates if t.SecTypeCode2 == 'ca']
            sl_candidates = [t for t in sl_candidates if abs(t.TradeAmountLocal - dv.TradeAmountLocal) < 0.015]  # APXTxns.pm line 11

            if len(sl_candidates):
                sl = sl_candidates[0]

                # Remove this one from the transactions, since it has been "merged" into the dividend
                transactions = [t for t in transactions if t != sl]
                dv.add_lineage(f'{dv.TransactionCode} -> Merged sl {sl.PortfolioTransactionID}', source_callable=get_current_callable())

                # update the 'dv' txn row to consolidate in the FX (line 468-476)
                for attr in ['SecurityID2', 'TradeAmount', 'TradeDateFX', 'SettleDateFX', 'SpotRate', 'FXDenominatorCurrencyCode', 'FXNumeratorCurrencyCode', 'SecTypeCode2', 'FxRate']:
                    if hasattr(sl, attr):
                        sl_value = getattr(sl, attr)
                        setattr(dv, attr, sl_value)
                    # TODO_EH: possible that the attribute DNE?

                # line 484-518: combine the gains 
                # Need to find realized gains first:                
                realized_gains_transactions = [Transaction(**d) for d in realized_gains_df.to_dict('records') if d['PortfolioTransactionID'] == sl.PortfolioTransactionID]
                if len(realized_gains_transactions):
                    # If we reached here, there is a relevant "realized gain" to combine with:
                    realized_gains_transaction = realized_gains_transactions[0]
                    if hasattr(dv, 'RealizedGainLoss'):
                        if dv.RealizedGainLoss is not None:
                            dv.RealizedGainLoss += realized_gains_transaction.RealizedGainLoss
                        else:
                            dv.RealizedGainLoss = realized_gains_transaction.RealizedGainLoss
                    else:
                        dv.RealizedGainLoss = realized_gains_transaction.RealizedGainLoss

        # Finally, we have:
        # transactions: excludes any sl/wd which have been "merged" into dividends above.
        # Note this still includes historical, hence the need to filter based on TradeDate below
        # dividends: updated above based on any identified sl/wd to "merge" in
        # Combine these two, then return the combined result
        res_transactions = [t for t in transactions
            if from_date <= t.TradeDate.date() <= to_date and t.TransactionCode != 'dv']
        res_transactions.extend(dividends)
        return res_transactions

    def __str__(self):
        return str(self.txn_source)


class APXDBDividendRepository(TransactionRepository):
    # TODO_CLEANUP: remove once not used (APXDBTransactionActivityRepository shall provide dividends instead)
    txn_source = APXDBTransactionActivityProcAndFunc()
    realized_gains_source = APXDBRealizedGainLossProcAndFunc()
    
    def create(self, transactions: Union[List[Transaction],Transaction]) -> int:
        raise NotImplementedError(f'Cannot create in {self.cn}!')

    def get(self, portfolio_code: Union[str,None]=None, trade_date: Union[datetime.date, Tuple[datetime.date, datetime.date], None]=None) -> List[Transaction]:
        # Populate from date
        # APXTxns.pm line 59: We want to get dividends data from up to 70 days before TradeDate:
        if isinstance(trade_date, datetime.date):
            from_date = trade_date + datetime.timedelta(days=-70)
        else:
            logging.error(f'Unexpected type for trade_date: {type(trade_date)}')
            return  # TODO_EH: exception?
        
        res_df = self.txn_source.read(Portfolios=portfolio_code, FromDate=from_date, ToDate=trade_date)
        transactions = [Transaction(**d) for d in res_df.to_dict('records')]

        # Find dividends with SETTLE date with the specified trade_date
        dividends = [t for t in transactions if t.SettleDate.date() == trade_date and t.TransactionCode == 'dv']

        if not len(dividends):
            return []

        # Read realized gains proc once (avoids reading it for every dividend separately)
        realized_gains_df = self.realized_gains_source.read(Portfolios=portfolio_code, FromDate=trade_date, ToDate=trade_date)

        for dv in dividends:
            # Start with empty array for transactions "merged" into this dividend.
            # If we find sl and/or wd, we'll add them here.
            dv.transactions_merged_in = []

            # APXTxns.pm line 353: jam on the LW blinders:  dv are all about SettleDate
            dv.TradeDate = dv.SettleDate

            # APXTxns.pm line 453-464: Find a wd matching the settle date, security (i.e. divacc), and having very similar amount
            wd_candidates = [t for t in transactions if t.TransactionCode in ('wd', 'dp')] 
            wd_candidates = [t for t in wd_candidates if t.SettleDate == dv.SettleDate]
            wd_candidates = [t for t in wd_candidates if t.SecurityID1 == dv.SecurityID2]
            wd_candidates = [t for t in wd_candidates if abs(t.TradeAmountLocal - dv.TradeAmountLocal) < 0.015]  # APXTxns.pm line 11

            if len(wd_candidates):
                wd = wd_candidates[0]

                # Record this as having been "merged" into the dividend
                dv.transactions_merged_in.append(wd)

            # APXTxns.pm line 465-519: Find a sl from cash to cash, and having very similar amount
            sl_candidates = [t for t in transactions if t.TransactionCode == 'sl']
            sl_candidates = [t for t in sl_candidates if t.SettleDate == dv.SettleDate]
            sl_candidates = [t for t in sl_candidates if t.SecTypeCode1 == 'ca']
            sl_candidates = [t for t in sl_candidates if t.SecTypeCode2 == 'ca']
            sl_candidates = [t for t in sl_candidates if abs(t.TradeAmountLocal - dv.TradeAmountLocal) < 0.015]  # APXTxns.pm line 11

            if len(sl_candidates):
                sl = sl_candidates[0]

                # Record this as having been "merged" into the dividend
                dv.transactions_merged_in.append(sl)

                # update the 'dv' txn row to consolidate in the FX (line 468-476)
                for attr in ['SecurityID2', 'TradeAmount', 'TradeDateFX', 'SettleDateFX', 'SpotRate', 'FXDenominatorCurrencyCode', 'FXNumeratorCurrencyCode', 'SecTypeCode2', 'FxRate']:
                    if hasattr(sl, attr):
                        sl_value = getattr(sl, attr)
                        setattr(dv, attr, sl_value)
                    # TODO_EH: possible that the attribute DNE?
                    # TODO: need to delete the sl txn? - APXTxns.pm line 481

                # line 484-518: combine the gains 
                # Need to find realized gains first:                
                realized_gains_transactions = [Transaction(**d) for d in realized_gains_df.to_dict('records') if d['PortfolioTransactionID'] == sl.PortfolioTransactionID]
                if len(realized_gains_transactions):
                    # If we reached here, there is a relevant "realized gain" to combine with:
                    realized_gains_transaction = realized_gains_transactions[0]
                    if hasattr(dv, 'RealizedGainLoss'):
                        if dv.RealizedGainLoss is not None:
                            dv.RealizedGainLoss += realized_gains_transaction.RealizedGainLoss
                        else:
                            dv.RealizedGainLoss = realized_gains_transaction.RealizedGainLoss
                    else:
                        dv.RealizedGainLoss = realized_gains_transaction.RealizedGainLoss

        return dividends

    def __str__(self):
        return str(self.txn_source)


class APXDBPastDividendRepository_OLD(SupplementaryRepository):
    proc = APXDBTransactionActivityProcAndFunc()

    def __init__(self):
        super().__init__(pk_columns=[  # No pk_columns since it is irrelevant...
                # PKColumnMapping('portfolio_code'),
                # PKColumnMapping('TradeDate'), 
                # PKColumnMapping('SettleDate'), 
                # PKColumnMapping('SecTypeCode2'),
                # PKColumnMapping('SecurityID2'),
                # PKColumnMapping('TradeAmountLocal'),
            ]) 
    
    def create(self, data: Dict[str, Any]) -> int:
        raise NotImplementedError(f'Cannot save to {self.cn}!') 

    def get(self, pk_column_values: Dict[str, Any]) -> dict:
        raise NotImplementedError(f'Cannot GET to {self.cn}!') 

    # def get(self, pk_column_values: Dict[str, Any]) -> dict:
    #     # We want to get dividends data from up to 70 days before TradeDate:
    #     trade_date = pk_column_values.get('TradeDate')
    #     from_date = trade_date + datetime.timedelta(days=-70)

    #     # Query proc
    #     res_df = self.proc.read(data_dt=prev_bday, Portfolios=pk_column_values.get('portfolio_code'), FromDate=from_date, ToDate=trade_date)

    #     # Find a wd matching the settle date, security (i.e. divacc), and having very similar amount
    #     wd_candidates = res_df[res_df['TransactionCode'] == 'wd']
    #     wd_candidates = wd_candidates[wd_candidates['SettleDate'] == ]

        # Look for a FX txn

    def supplement(self, transaction: Transaction):
        if transaction.TransactionCode != 'dv':
            return

        # APXTxns.pm line 59: We want to get dividends data from up to 70 days before TradeDate:
        trade_date = transaction.TradeDate
        from_date = trade_date + datetime.timedelta(days=-70)

        # Query proc
        res_df = self.proc.read(Portfolios=transaction.portfolio_code, FromDate=from_date, ToDate=trade_date)

        # APXTxns.pm line 453-464: Find a wd matching the settle date, security (i.e. divacc), and having very similar amount
        wd_candidates = res_df[res_df['TransactionCode'].isin(['wd', 'dp'])]
        wd_candidates = wd_candidates[wd_candidates['SettleDate'] == transaction.SettleDate]
        wd_candidates = wd_candidates[wd_candidates['SecurityID1'] == transaction.SecurityID2]
        wd_candidates = wd_candidates[abs(wd_candidates['TradeAmountLocal'] - transaction.TradeAmountLocal) < 0.015]  # APXTxns.pm line 11
        # TODO: now what to do if we found one? Need to delete the wd txn somehow ... 
        
        # APXTxns.pm line 465-519: Find a sl from cash to cash, and having very similar amount
        sl_candidates = res_df[res_df['TransactionCode'] == 'sl']
        sl_candidates = sl_candidates[sl_candidates['SettleDate'] == transaction.SettleDate]
        sl_candidates = sl_candidates[sl_candidates['SecTypeCode1'] == 'ca']
        sl_candidates = sl_candidates[sl_candidates['SecTypeCode2'] == 'ca']
        sl_candidates = sl_candidates[abs(sl_candidates['TradeAmountLocal'] - transaction.TradeAmountLocal) < 0.015]  # APXTxns.pm line 11
        if len(sl_candidates):
            sl_dict = sl_candidates.to_dict('records')[0]

            # update the 'dv' txn row to consolidate in the FX
            for attr in ['SecurityID2', 'TradeAmount', 'TradeDateFX', 'SettleDateFX', 'SpotRate', 'FXDenominatorCurrencyCode', 'FXNumeratorCurrencyCode', 'SecTypeCode2', 'FxRate']:
                setattr(transaction, attr, sl_dict[attr])
                # TODO: need to delete the sl txn - APXTxns.pm line 481

            # TODO: combine the gains


class APXRepDBLWTxnSummaryRepository(TransactionRepository):
    table = APXRepDBLWTxnSummaryTable()
    txn2table_columns = [
        # ({transaction attribute}, {table column})
        ('lw_lineage'       , 'lw_lineage'),
        ('PortfolioCode'    , 'portfolio_code'),
        ('PortfolioName'    , 'portfolio_name'),
        ('TransactionCode'  , 'tran_code'),
        ('TradeDate'        , 'trade_date'),
        ('SettleDate'       , 'settle_date'),
        ('Symbol1'          , 'symbol'),
        # ('Cusip'    , 'cusip'),  # TODO_CLEANUP: seems cusip is always null in current table ... remove once confirmed not needed
        ('Name4Stmt'        , 'name4stmt'),
        ('Quantity'         , 'quantity'),
        ('TradeAmount'      , 'trade_amount'),
        ('CashFlow'         , 'cash_flow'),
        ('BrokerName'       , 'broker_name'),
        ('CustodianName'    , 'custodian_name'),
        ('CustAcctNotify'   , 'cust_acct_notify'),
        ('PricePerUnit'     , 'price_per_unit'),
        ('Commission'       , 'commission'),
        ('NetInterest'      , 'net_interest'),
        ('NetDividend'      , 'net_dividend'),
        ('NetFgnIncome'     , 'net_fgn_income'),
        ('CapGainsDistrib'  , 'cap_gains_distrib'),
        ('TotalIncome'      , 'tot_income'),
        ('RealizedGain'     , 'realized_gain'),
        ('TfsaContribAmt'   , 'tfsa_contrib_amt'),
        ('RspContribAmt'    , 'rsp_contrib_amt'),
        ('RetOfCapital'     , 'net_return_of_capital'),
        ('SecurityID1'      , 'security_id1'),
        ('SecurityID2'      , 'security_id2'),
        ('Symbol1'          , 'symbol1'),
        ('Symbol2'          , 'symbol2'),
        ('SecTypeCode1'     , 'sectype1'),
        ('SecTypeCode2'     , 'sectype2'),
        # ('TxnUserDef3Name'  , 'source'),  # TODO_CLEANUP: seems source is always null in current table ... remove once confirmed not needed
        # ('PortfolioCode'    , 'market_name'),  # TODO_CLEANUP: seems market_name is always null in current table ... remove once confirmed not needed
        ('CostPerUnit'      , 'cost_per_unit'),
        ('CostBasis'        , 'total_cost'),
        ('LocalTranKey'     , 'local_tran_key'),
        ('TransactionName'  , 'tran_desc'),
        ('Comment01'        , 'comment01'),
        ('SectionDesc'      , 'section_desc'),
        ('StmtTranDesc'     , 'stmt_tran_desc'),
        ('NetEligDividend'  , 'net_elig_dividend'),
        ('NetNonEligDividend', 'net_non_elig_dividend'),
        ('FxRate'           , 'fx_rate'),
        ('PrincipalCurrencyISOCode1', 'fx_denom_ccy'),
        ('ReportingCurrencyISOCode', 'fx_numer_ccy'),
        ('WhFedTaxAmt'      , 'whfedtax_amt'),
        ('WhNrTaxAmt'       , 'whnrtax_amt'),
        ('TradeAmountLocal' , 'trade_amount_local'),
        ('CostPerUnitLocal' , 'cost_per_unit_local'),
        ('CostBasisLocal'   , 'total_cost_local'),
    ]

    def create(self, transactions: Union[List[Transaction],Transaction]) -> int:
        raise NotImplementedError(f'Cannot create in {self.cn}!')

    def get(self, portfolio_code: Union[str,None]=None, trade_date: Union[datetime.date, Tuple[datetime.date, datetime.date], None]=None) -> List[Transaction]:
        pass


""" COREDB """

class CoreDBTransactionProcessingQueueRepository(TransactionProcessingQueueRepository):
    """ Base class for all CoreDB queues, since the subclasses would have similar/identical implementations """
    
    """ Subclasses must specify a table """
    @property
    @abstractmethod
    def table(self):
        pass

    def create(self, queue_item: TransactionProcessingQueueItem) -> int:
        current = self.table.read(queue_status=queue_item.queue_status.name, portfolio_code=queue_item.portfolio_code
                                    , trade_date=queue_item.trade_date)
        if len(current):
            # Nothing to do ... it already has the desired status.
            # We consider this a success and therefore return 1:
            return 1  
        else:
            # TODO_EH: try-catch for SQL error?
            insert_res = self.table.execute_insert(data={
                'portfolio_code': queue_item.portfolio_code,
                'trade_date': queue_item.trade_date,
                'queue_status': queue_item.queue_status.name,
                'modified_by': os.environ.get('APP_NAME'),
                'modified_at': datetime.datetime.now(),
            })
            return insert_res.rowcount

    def update_queue_status(self, queue_item: TransactionProcessingQueueItem, old_queue_status: Union[QueueStatus,None]=None) -> int:
        # Build update stmt
        stmt = sql.update(self.table.table_def)
        stmt = stmt.values(queue_status=queue_item.queue_status.name)
        stmt = stmt.where(self.table.c.portfolio_code == queue_item.portfolio_code)
        stmt = stmt.where(self.table.c.trade_date == queue_item.trade_date)
        stmt = stmt.where(self.table.c.queue_status == old_queue_status.name)

        # Execute write
        update_res = self.table.execute_write(stmt)

        # Return rowcount
        return update_res.rowcount

    def get(self, portfolio_code: Union[str,None]=None, trade_date: Union[datetime.date,None]=None
                , queue_status: Union[QueueStatus,None]=None) -> List[TransactionProcessingQueueItem]:
        res_df = self.table.read(portfolio_code=portfolio_code, trade_date=trade_date, queue_status=queue_status.name)
        res_queue_items = [TransactionProcessingQueueItem(portfolio_code=r['portfolio_code'], trade_date=r['trade_date']
                            , queue_status=QueueStatus.from_value(r['queue_status'])) for r in res_df.to_dict('records')]
        return res_queue_items


class CoreDBRealizedGainLossQueueRepository(CoreDBTransactionProcessingQueueRepository):
    table = COREDBAPXfRealizedGainLossQueueTable()

class CoreDBTransactionActivityQueueRepository(CoreDBTransactionProcessingQueueRepository):
    table = COREDBAPXfTransactionActivityQueueTable()


class CoreDBLWTransactionSummaryQueueRepository(CoreDBTransactionProcessingQueueRepository):
    table = COREDBLWTransactionSummaryQueueTable()


class COREDBAPX2SFTxnQueueRepository(CoreDBTransactionProcessingQueueRepository):
    table = COREDBAPX2SFTxnQueueTable()


class CoreDBRealizedGainLossSupplementaryRepository(SupplementaryRepository):
    table = COREDBAPXfRealizedGainLossTable()
    relevant_columns = ['RealizedGainLoss', 'RealizedGainLossLocal', 'CostBasis', 'CostBasisLocal', 'Quantity']

    def __init__(self):
        super().__init__(pk_columns=[
                                    PKColumnMapping('PortfolioTransactionID'), 
                                    PKColumnMapping('TranID'), 
                                    PKColumnMapping('LotNumber'), 
                                ])
    
    def create(self, data: Dict[str, Any]) -> int:
        raise NotImplementedError(f'Cannot save to {self.cn}! Should you use CoreDBRealizedGainLossTransactionRepository instead?')

    def get(self, pk_column_values: Dict[str, Any]) -> dict:
        # Query table
        res_df = self.table.read(**pk_column_values)
        
        # Convert to list of dicts
        res_dicts = res_df[self.relevant_columns].to_dict('records')

        # Should be 1 row max... sanity check... # TODO_EH: what if this has more than one row?
        if len(res_dicts) > 1: 
            logging.info(f"Found multiple rows in {self.table.cn} for {pk_column_values}!")
            return res_dicts[0]
        elif len(res_dicts):
            return res_dicts[0]
        else:
            logging.debug(f"Found 0 rows in {self.table.cn} for {pk_column_values}!")
            return {}

    def supplement(self, transaction: Transaction):
        # Save original quantity (we need to save it back after to avoid it getting overwritten)
        quantity_orig = transaction.Quantity

        # Supplement as normal
        # Also save the supplemental data for use below
        supplemental_data = super().supplement(transaction)

        # We need to check if there is a quantity in the supplemental data, and if so, then supplement further:
        if isinstance(supplemental_data, dict):
            if supplemental_quantity := supplemental_data.get('Quantity'):
                if hasattr(transaction, 'CostBasis'):
                    transaction.RptCostBasis = transaction.CostBasis
                    transaction.RptCostPerUnit = transaction.RptCostBasis / supplemental_quantity
                else:
                    logging.debug(f'{transaction.PortfolioTransactionID} has no CostBasis')
                if hasattr(transaction, 'CostBasisLocal'):
                    transaction.LocalCostBasis = transaction.CostBasisLocal
                    transaction.LocalCostPerUnit = transaction.LocalCostBasis / supplemental_quantity

        # Save back the original quantity 
        transaction.Quantity = quantity_orig


class CoreDBRealizedGainLossTransactionRepository(TransactionRepository):
    table = COREDBAPXfRealizedGainLossTable()
    repo_to_refresh = None  # CoreDBRealizedGainLossInMemoryRepository()

    def create(self, transactions: Union[List[Transaction],Transaction]) -> int:
        # May be a Transaction. If so, make it a list:
        if isinstance(transactions, Transaction):
            transactions = [transactions]

        now = datetime.datetime.now()
        delete_stmts = []
        refresh_criterias = []
        old_row_count = self.table.row_count()
        logging.debug(f'{self.cn} got row count {old_row_count}')
        for txn in transactions:
            # Supplement transactions
            txn.modified_by = os.environ.get('APP_NAME') if not hasattr(txn, 'modified_by') else txn.modified_by
            txn.modified_at = now

            # Also create & append delete stmt, if it's not already there:
            if old_row_count:
                delete_stmt = sql.delete(self.table.table_def)
                refresh_criteria = {}
                if hasattr(txn, 'portfolio_code'):
                    delete_stmt = delete_stmt.where(self.table.c.portfolio_code == txn.portfolio_code)
                    # refresh_criteria['portfolio_code'] = txn.portfolio_code
                if hasattr(txn, 'PortfolioCode'):
                    delete_stmt = delete_stmt.where(self.table.c.portfolio_code == txn.PortfolioCode)
                    # refresh_criteria['portfolio_code'] = txn.PortfolioCode
                if hasattr(txn, 'PortfolioBaseCode'):
                    delete_stmt = delete_stmt.where(self.table.c.portfolio_code == txn.PortfolioBaseCode)
                    # refresh_criteria['portfolio_code'] = txn.PortfolioBaseCode            
                if hasattr(txn, 'CloseDate'):
                    delete_stmt = delete_stmt.where(self.table.c.CloseDate == txn.CloseDate)
                    # refresh_criteria['from_date'] = txn.CloseDate
                elif hasattr(txn, 'trade_date'):
                    delete_stmt = delete_stmt.where(self.table.c.CloseDate == txn.trade_date)
                    # refresh_criteria['from_date'] = txn.trade_date
                else:
                    logging.error(f'Txn has no trade date!? {txn}')
                    # TODO_EH: raise exception?
            
                if delete_stmt not in delete_stmts:
                    delete_stmts.append(delete_stmt)
                if refresh_criteria not in refresh_criterias:
                    refresh_criterias.append(refresh_criteria)

        # Convert list of SimpleNamespace instances to list of dictionaries
        data = [{k: getattr(txn, k) for k in txn.__dict__} for txn in transactions]

        # Create DataFrame from list of dictionaries
        df = pd.DataFrame(data)

        # Delete old results
        if old_row_count:
            logging.info(f'Deleting old results from {self.table.cn}...')
            for stmt in delete_stmts:
                logging.debug(f'Deleting old results from {self.table.cn}... {str(stmt)}')
                delete_res = self.table.execute_write(stmt)
        else:
            logging.info(f'Skipping delete in {self.cn} because there are 0 existing rows!')

        # Bulk insert df
        logging.info(f'Inserting new results to {self.table.cn}...')
        res = self.table.bulk_insert(df)

        # Refresh repo (if applicable)
        if self.repo_to_refresh:
            for criteria in refresh_criterias:
                logging.debug(f'{self.cn} refreshing {self.repo_to_refresh.cn} for criteria: {criteria}')
                refresh_res = self.repo_to_refresh.refresh(params=criteria)

        # Reutrn row count
        return res.rowcount

    def get(self, portfolio_code: Union[str,None]=None, trade_date: Union[datetime.date, Tuple[datetime.date, datetime.date], None]=None) -> List[Transaction]:
        # Infer from date & to date from trade_date
        if isinstance(trade_date, tuple):
            from_date, to_date = trade_date
        elif isinstance(trade_date, datetime.date):
            from_date = to_date = trade_date
        elif trade_date:
            logging.error(f'{type(trade_date).__name__}: invalid arg for {self.cn} GET trade_date: {trade_date}')
        else:
            from_date = to_date = None
        
        res_df = self.table.read(portfolio_code=portfolio_code, from_date=from_date, to_date=to_date)
        res_transactions = [Transaction(**r) for r in res_df.to_dict('records')]
        return res_transactions


class CoreDBTransactionActivityRepository(TransactionRepository):
    txn_source = COREDBAPXfTransactionActivityTable()
    realized_gains_source = COREDBAPXfRealizedGainLossTable()
    
    def create(self, transactions: Union[List[Transaction],Transaction]) -> int:
        # May be a Transaction. If so, make it a list:
        if isinstance(transactions, Transaction):
            transactions = [transactions]

        now = datetime.datetime.now()
        delete_stmts = []
        old_row_count = self.txn_source.row_count()
        logging.debug(f'{self.cn} got row count {old_row_count}')
        for txn in transactions:
            # Supplement transactions
            txn.modified_by = os.environ.get('APP_NAME') if not hasattr(txn, 'modified_by') else txn.modified_by
            txn.modified_at = now

            # Also create & append delete stmt, if it's not already there:
            if old_row_count:
                delete_stmt = sql.delete(self.txn_source.table_def)
                if hasattr(txn, 'portfolio_code'):
                    delete_stmt = delete_stmt.where(self.txn_source.c.portfolio_code == txn.portfolio_code)
                if hasattr(txn, 'PortfolioCode'):
                    delete_stmt = delete_stmt.where(self.txn_source.c.portfolio_code == txn.PortfolioCode)
                if hasattr(txn, 'PortfolioBaseCode'):
                    delete_stmt = delete_stmt.where(self.txn_source.c.portfolio_code == txn.PortfolioBaseCode)
                if hasattr(txn, 'CloseDate'):
                    delete_stmt = delete_stmt.where(self.txn_source.c.CloseDate == txn.CloseDate)
                elif hasattr(txn, 'trade_date'):
                    delete_stmt = delete_stmt.where(self.txn_source.c.CloseDate == txn.trade_date)
                else:
                    logging.error(f'Txn has no trade date!? {txn}')
                    # TODO_EH: raise exception?
            
                if delete_stmt not in delete_stmts:
                    delete_stmts.append(delete_stmt)

        # Convert list of SimpleNamespace instances to list of dictionaries
        data = [{k: getattr(txn, k) for k in txn.__dict__} for txn in transactions]

        # Create DataFrame from list of dictionaries
        df = pd.DataFrame(data)

        # Delete old results
        if old_row_count:
            logging.info(f'Deleting old results from {self.txn_source.cn}...')
            for stmt in delete_stmts:
                logging.debug(f'Deleting old results from {self.txn_source.cn}... {str(stmt)}')
                delete_res = self.txn_source.execute_write(stmt)
        else:
            logging.info(f'Skipping delete in {self.cn} because there are 0 existing rows!')

        # Bulk insert df
        logging.info(f'Inserting new results to {self.txn_source.cn}...')
        res = self.txn_source.bulk_insert(df)

        # Reutrn row count
        return res.rowcount

    def get(self, portfolio_code: Union[str,None]=None, trade_date: Union[datetime.date, Tuple[datetime.date, datetime.date], None]=None) -> List[Transaction]:
        # Infer from & to dates, based on trade_date type
        if isinstance(trade_date, tuple):
            from_date, to_date = trade_date
            historical_from_date = from_date + datetime.timedelta(days=-70)
        elif isinstance(trade_date, datetime.date):
            from_date = to_date = trade_date
            historical_from_date = from_date + datetime.timedelta(days=-70)
        else:
            return []  # TODO_EH: exception?

        # Get source transactions, including historical
        res_df = self.txn_source.read(portfolio_code=portfolio_code, from_date=historical_from_date, to_date=to_date)
        transactions = [Transaction(**d) for d in res_df.to_dict('records')]
        
        # Find dividends with SETTLE date within the specified trade_date range
        dividends = [t for t in transactions 
            # if from_date <= t.SettleDate.date() <= to_date and t.TransactionCode == 'dv']
            if from_date <= t.SettleDate <= to_date and t.TransactionCode == 'dv']

        # Read realized gains proc once (avoids reading it for every dividend separately)
        realized_gains_df = self.realized_gains_source.read(portfolio_code=portfolio_code, from_date=historical_from_date, to_date=to_date)

        for dv in dividends:
            # APXTxns.pm line 353: jam on the LW blinders:  dv are all about SettleDate
            dv.TradeDate = dv.SettleDate

            # APXTxns.pm line 453-464: Find a wd matching the settle date, security (i.e. divacc), and having very similar amount
            wd_candidates = [t for t in transactions if t.TransactionCode in ('wd', 'dp')] 
            wd_candidates = [t for t in wd_candidates if t.SettleDate == dv.SettleDate]
            wd_candidates = [t for t in wd_candidates if t.SecurityID1 == dv.SecurityID2]
            wd_candidates = [t for t in wd_candidates if abs(t.TradeAmountLocal - dv.TradeAmountLocal) < 0.015]  # APXTxns.pm line 11

            if len(wd_candidates):
                wd = wd_candidates[0]

                # Remove this one from the transactions, since it has been "merged" into the dividend
                # transactions = [t for t in transactions if t != wd]
                dv.add_lineage(f'{dv.TransactionCode} -> Merged dp/wd {wd.PortfolioTransactionID}... but did not remove the wd... see APXTxns.pm line 462', source_callable=get_current_callable())

            # APXTxns.pm line 465-519: Find a sl from cash to cash, and having very similar amount
            sl_candidates = [t for t in transactions if t.TransactionCode == 'sl']
            sl_candidates = [t for t in sl_candidates if t.SettleDate == dv.SettleDate]
            sl_candidates = [t for t in sl_candidates if t.SecTypeCode1 == 'ca']
            sl_candidates = [t for t in sl_candidates if t.SecTypeCode2 == 'ca']
            sl_candidates = [t for t in sl_candidates if abs(t.TradeAmountLocal - dv.TradeAmountLocal) < 0.015]  # APXTxns.pm line 11

            if len(sl_candidates):
                sl = sl_candidates[0]

                # Remove this one from the transactions, since it has been "merged" into the dividend
                transactions = [t for t in transactions if t != sl]
                dv.add_lineage(f'{dv.TransactionCode} -> Merged sl {sl.PortfolioTransactionID}', source_callable=get_current_callable())

                # update the 'dv' txn row to consolidate in the FX (line 468-476)
                for attr in ['SecurityID2', 'TradeAmount', 'TradeDateFX', 'SettleDateFX', 'SpotRate', 'FXDenominatorCurrencyCode', 'FXNumeratorCurrencyCode', 'SecTypeCode2', 'FxRate']:
                    if hasattr(sl, attr):
                        sl_value = getattr(sl, attr)
                        setattr(dv, attr, sl_value)
                    # TODO_EH: possible that the attribute DNE?

                # line 484-518: combine the gains 
                # Need to find realized gains first:                
                realized_gains_transactions = [Transaction(**d) for d in realized_gains_df.to_dict('records') if d['PortfolioTransactionID'] == sl.PortfolioTransactionID]
                if len(realized_gains_transactions):
                    # If we reached here, there is a relevant "realized gain" to combine with:
                    realized_gains_transaction = realized_gains_transactions[0]
                    if hasattr(dv, 'RealizedGainLoss'):
                        if dv.RealizedGainLoss is not None:
                            dv.RealizedGainLoss += realized_gains_transaction.RealizedGainLoss
                        else:
                            dv.RealizedGainLoss = realized_gains_transaction.RealizedGainLoss
                    else:
                        dv.RealizedGainLoss = realized_gains_transaction.RealizedGainLoss

        # Finally, we have:
        # transactions: excludes any sl/wd which have been "merged" into dividends above.
        # Note this still includes historical, hence the need to filter based on TradeDate below
        # dividends: updated above based on any identified sl/wd to "merge" in
        # Combine these two, then return the combined result
        res_transactions = [t for t in transactions
            if from_date <= t.TradeDate <= to_date and t.TransactionCode != 'dv']
        res_transactions.extend(dividends)

        # Order by PortfolioTransactionID and return
        # Doing this ordering should ensure consistency with Perl code in ordering for the for loops.
        # This is relevant when grouping dp/wd's and assigning the group a LocalTranKey of the first dp/wd,
        # for example. See application\engines.py::net_deposits_withdrawals.
        sorted_transactions = sorted(res_transactions, key=lambda x: x.PortfolioTransactionID)
        return sorted_transactions

    def __str__(self):
        return str(self.txn_source)


class CoreDBTransactionActivityRepository_OLD(TransactionRepository):
    # TODO_CLEANUP: remove once confirmed the new CoreDBTransactionActivityRepository works
    table = COREDBAPXfTransactionActivityTable()

    def create(self, transactions: Union[List[Transaction],Transaction]) -> int:
        # May be a Transaction. If so, make it a list:
        if isinstance(transactions, Transaction):
            transactions = [transactions]

        now = datetime.datetime.now()
        delete_stmts = []
        for txn in transactions:
            # Supplement transactions
            txn.modified_by = os.environ.get('APP_NAME') if not hasattr(txn, 'modified_by') else txn.modified_by
            txn.modified_at = now

            # Also create & append delete stmt, if it's not already there:
            delete_stmt = sql.delete(self.table.table_def)
            if hasattr(txn, 'portfolio_code'):
                delete_stmt = delete_stmt.where(self.table.c.portfolio_code == txn.portfolio_code)
            if hasattr(txn, 'PortfolioCode'):
                delete_stmt = delete_stmt.where(self.table.c.portfolio_code == txn.PortfolioCode)
            if hasattr(txn, 'PortfolioBaseCode'):
                delete_stmt = delete_stmt.where(self.table.c.portfolio_code == txn.PortfolioBaseCode)
            if hasattr(txn, 'TradeDate'):
                delete_stmt = delete_stmt.where(self.table.c.TradeDate == txn.TradeDate)
            elif hasattr(txn, 'trade_date'):
                delete_stmt = delete_stmt.where(self.table.c.TradeDate == txn.trade_date)
            else:
                logging.error(f'Txn has no trade date!? {txn}')
                # TODO_EH: raise exception?
            if delete_stmt not in delete_stmts:
                delete_stmts.append(delete_stmt)

        # Convert list of SimpleNamespace instances to list of dictionaries
        data = [{k: getattr(txn, k) for k in txn.__dict__} for txn in transactions]

        # Create DataFrame from list of dictionaries
        df = pd.DataFrame(data)

        # Delete old results
        for stmt in delete_stmts:
            logging.debug(f'Deleting old results from {self.table.cn}... {str(stmt)}')
            delete_res = self.table.execute_write(stmt)

        # Bulk insert df
        logging.info(f'Inserting new results to {self.table.cn}...')
        res = self.table.bulk_insert(df)

        # Reutrn row count
        return res.rowcount

    def get(self, portfolio_code: Union[str,None]=None, trade_date: Union[datetime.date, Tuple[datetime.date, datetime.date], None]=None) -> List[Transaction]:
        if isinstance(trade_date, tuple):
            from_date, to_date = trade_date
        elif isinstance(trade_date, datetime.date):
            from_date = to_date = trade_date
        elif trade_date:
            logging.error(f'Invalid arg for {self.cn} GET: {trade_date}')
        res_df = self.table.read(portfolio_code=portfolio_code, from_date=from_date, to_date=to_date)
        res_transactions = [Transaction(**r) for r in res_df.to_dict('records')]
        return res_transactions


class CoreDBLWTransactionSummaryRepository(TransactionRepository):
    table = COREDBLWTransactionSummaryTable()

    def create(self, transactions: Union[List[Transaction],Transaction]) -> int:
        # May be a Transaction. If so, make it a list:
        if isinstance(transactions, Transaction):
            transactions = [transactions]

        now = datetime.datetime.now()
        delete_stmts = []
        for txn in transactions:
            # Supplement transactions
            txn.modified_by = os.environ.get('APP_NAME') if not hasattr(txn, 'modified_by') else txn.modified_by
            txn.modified_at = now

            # Also create & append delete stmt, if it's not already there:
            delete_stmt = sql.delete(self.table.table_def)
            delete_stmt = delete_stmt.where(self.table.c.portfolio_code == txn.portfolio_code)
            delete_stmt = delete_stmt.where(self.table.c.TradeDate == txn.trade_date)
            if delete_stmt not in delete_stmts:
                delete_stmts.append(delete_stmt)

        # Convert list of SimpleNamespace instances to list of dictionaries
        data = [{k: getattr(txn, k) for k in txn.__dict__} for txn in transactions]

        # Create DataFrame from list of dictionaries
        df = pd.DataFrame(data)

        # Delete old results
        for stmt in delete_stmts:
            logging.debug(f'Deleting old results from {self.table.cn}... {str(stmt)}')
            delete_res = self.table.execute_write(stmt)

        # Bulk insert df
        logging.info(f'Inserting new results to {self.table.cn}...')
        res = self.table.bulk_insert(df)

        # Reutrn row count
        return res.rowcount

    def get(self, portfolio_code: Union[str,None]=None, trade_date: Union[datetime.date, Tuple[datetime.date, datetime.date], None]=None) -> List[Transaction]:
        return []  # TODO: implement


class COREDBLWTxnSummaryRepository(TransactionRepository):
    table = COREDBLWTxnSummaryTable()
    readable_name = table.readable_name
    txn2table_column_mappings = [
        # apx2sf.pl @SF_TRANSACTIONS_feed
        # ({transaction attribute}, {table column}, {txn2table_format_function}, {table2txn_format_function})
        # In cases where the same txn attribute maps to multiple table columns, the more desirable column should be listed first (higher)
        # In cases where the same table column maps to multiple txn attributes, the more desirable attribute should be listed first (higher)
        Txn2TableColMap('lw_lineage'       , 'lw_lineage'),
        Txn2TableColMap('PortfolioCode'    , 'portfolio_code'),
        Txn2TableColMap('PortfolioName'    , 'portfolio_name'),
        Txn2TableColMap('TransactionCode'  , 'tran_code'),
        Txn2TableColMap('TradeDate'        , 'trade_date'),
        Txn2TableColMap('SettleDate'       , 'settle_date'),
        Txn2TableColMap('Symbol1'          , 'symbol'),
        # Txn2TableColMap('Cusip'    , 'cusip'),  # TODO_CLEANUP: seems cusip is always null in current table ... remove once confirmed not needed
        Txn2TableColMap('Name4Stmt'        , 'name4stmt'),
        Txn2TableColMap('Quantity'         , 'quantity'
                            , lambda x: x if not x else normal_round(x, 9)),
        Txn2TableColMap('TradeAmount'      , 'trade_amount'
                            , lambda x: x if not x else normal_round(x, 2)),
        Txn2TableColMap('CashFlow'         , 'cash_flow'
                            , lambda x: x if not x else normal_round(x, 2)),
        Txn2TableColMap('BrokerName'       , 'broker_name'),
        Txn2TableColMap('CustodianName'    , 'custodian_name'),
        Txn2TableColMap('CustAcctNotify'   , 'cust_acct_notify'),
        Txn2TableColMap('PricePerUnit'     , 'price_per_unit'
                            , lambda x: x if not x else normal_round(x, 9)),
        Txn2TableColMap('Commission'       , 'commission'
                            , lambda x: 0.0 if (not x or pd.isna(x)) else normal_round(x, 9)),
        Txn2TableColMap('NetInterest'      , 'net_interest'
                            , lambda x: 0.0 if (not x or pd.isna(x)) else normal_round(x, 2)),
        Txn2TableColMap('NetDividend'      , 'net_dividend'
                            , lambda x: 0.0 if (not x or pd.isna(x)) else normal_round(x, 2)),
        Txn2TableColMap('NetFgnIncome'     , 'net_fgn_income'
                            , lambda x: 0.0 if (not x or pd.isna(x)) else normal_round(x, 2)),
        Txn2TableColMap('CapGainsDistrib'  , 'cap_gains_distrib'
                            , lambda x: 0.0 if (not x or pd.isna(x)) else x),
        Txn2TableColMap('TotalIncome'      , 'tot_income'
                            , lambda x: 0.0 if (not x or pd.isna(x)) else normal_round(x, 2)),
        Txn2TableColMap('RealizedGain'     , 'realized_gain'
                            , lambda x: x if not x else normal_round(x, 2)),
        Txn2TableColMap('TfsaContribAmt'   , 'tfsa_contrib_amt'
                            , lambda x: 0.0 if (not x or pd.isna(x)) else normal_round(x, 2)),
        Txn2TableColMap('RspContribAmt'    , 'rsp_contrib_amt'
                            , lambda x: 0.0 if (not x or pd.isna(x)) else normal_round(x, 2)),
        Txn2TableColMap('RetOfCapital'     , 'net_return_of_capital'
                            , lambda x: 0.0 if (not x or pd.isna(x)) else normal_round(x, 2)),
        Txn2TableColMap('SecurityID1'      , 'security_id1'),
        Txn2TableColMap('SecurityID2'      , 'security_id2'),
        Txn2TableColMap('Symbol1'          , 'symbol1'),
        # Txn2TableColMap('Symbol2'          , 'symbol2'),
        Txn2TableColMap('SecTypeCode1'     , 'sectype1'),
        Txn2TableColMap('SecTypeCode2'     , 'sectype2'),
        # Txn2TableColMap('TxnUserDef3Name'  , 'source'),  # TODO_CLEANUP: seems source is always null in current table ... remove once confirmed not needed
        # Txn2TableColMap('PortfolioCode'    , 'market_name'),  # TODO_CLEANUP: seems market_name is always null in current table ... remove once confirmed not needed
        Txn2TableColMap('CostPerUnit'      , 'cost_per_unit'
                            , lambda x: x if not x else normal_round(x, 9)),
        Txn2TableColMap('CostBasis'        , 'total_cost'
                            , lambda x: x if not x else normal_round(x, 2)),
        Txn2TableColMap('LocalTranKey'     , 'local_tran_key'),
        Txn2TableColMap('TransactionName'  , 'tran_desc'),
        Txn2TableColMap('Comment01'        , 'comment01'),
        Txn2TableColMap('SectionDesc'      , 'section_desc'),
        Txn2TableColMap('StmtTranDesc'     , 'stmt_tran_desc'),
        Txn2TableColMap('NetEligDividend'  , 'net_elig_dividend'
                            , lambda x: 0.0 if (not x or pd.isna(x)) else normal_round(x, 2)),
        Txn2TableColMap('NetNonEligDividend', 'net_non_elig_dividend'
                            , lambda x: 0.0 if (not x or pd.isna(x)) else normal_round(x, 2)),
        Txn2TableColMap('FxRate'           , 'fx_rate'
                            , lambda x: x if not x else normal_round(x, 9)),
        Txn2TableColMap('PrincipalCurrencyISOCode1', 'fx_denom_ccy'),
        Txn2TableColMap('ReportingCurrencyISOCode', 'fx_numer_ccy'),
        Txn2TableColMap('WhFedTaxAmt'      , 'whfedtax_amt'
                            , lambda x: 0.0 if (not x or pd.isna(x)) else normal_round(x, 2)),
        Txn2TableColMap('WhNrTaxAmt'       , 'whnrtax_amt'
                            , lambda x: 0.0 if (not x or pd.isna(x)) else normal_round(x, 2)),
        Txn2TableColMap('TradeAmountLocal' , 'trade_amount_local'
                            , lambda x: x if not x else normal_round(x, 2)),
        Txn2TableColMap('CostPerUnitLocal' , 'cost_per_unit_local'
                            , lambda x: x if not x else normal_round(x, 9)),
        Txn2TableColMap('CostBasisLocal'   , 'total_cost_local'
                            , lambda x: x if not x else normal_round(x, 2)),
        # below are new columns in new stuff - not saved to DB by existing LW Txn Summary
        Txn2TableColMap('trade_date_original', 'trade_date_original'),
        Txn2TableColMap('PortfolioBaseID'   , 'portfolio_id'),
        Txn2TableColMap('PortfolioID'       , 'portfolio_id'),
        Txn2TableColMap('PricePerUnitLocal' , 'price_per_unit_local'
                            , lambda x: x if not x else normal_round(x, 9)),
    ]


    def create(self, transactions: Union[List[Transaction],Transaction]) -> int:
        if isinstance(transactions, Transaction):
            transactions = [transactions]

        # Loop thru; produce list of dicts. Each will have keys matching table column names
        table_ready_dicts = []
        delete_stmts = []
        now = datetime.datetime.now()
        common_dict = {
            'scenariodate': now,
            'asofdate': now,
            'asofuser': (f"{os.getlogin()}_{os.environ.get('APP_NAME') or os.path.basename(__file__)}")[:32],
            'computer': socket.gethostname().upper()
        }
        for txn in transactions:
            table_ready_dict = common_dict.copy()
            for cm in reversed(self.txn2table_column_mappings):
                # if hasattr(txn, cm.transaction_attribute):
                # If the attribute exists for this transaction, populate the dict with its value
                attr_value = getattr(txn, cm.transaction_attribute, None)

                # # Round, if specified
                # if cm.round_to_decimal_places and attr_value:
                #     attr_value = normal_round(attr_value, cm.round_to_decimal_places)

                # # Replace None with 0.0, if specified
                # if cm.populate_none_with_zero and (not attr_value or pd.isna(attr_value)):
                #     attr_value = 0.0

                # Apply formatting function, if specified
                if cm.txn2table_format_function:
                    attr_value = cm.txn2table_format_function(attr_value)

                # Assign value in dict
                table_ready_dict[cm.table_column] = attr_value
            
            # Now we have the dict containing all desired values for the row. Append it:
            table_ready_dicts.append(table_ready_dict)

            # Also create & append delete stmt, if it's not already there:
            delete_stmt = sql.delete(self.table.table_def)
            delete_stmt = delete_stmt.where(self.table.c.portfolio_code == txn.portfolio_code)
            delete_stmt = delete_stmt.where(self.table.c.trade_date_original == txn.trade_date_original)
            if delete_stmt not in delete_stmts:
                delete_stmts.append(delete_stmt)

        # Now we have a list of dicts. Convert to df to facilitate bulk insert: 
        df = pd.DataFrame(table_ready_dicts)

        # Delete old results
        for stmt in delete_stmts:
            logging.debug(f'Deleting old results from {self.table.cn}... {str(stmt)}')
            delete_res = self.table.execute_write(stmt)

        # Bulk insert df
        logging.info(f'Inserting {len(df)} new results to {self.table.cn}...')
        res = self.table.bulk_insert(df)

        # Reutrn row count
        return res.rowcount

    def get(self, portfolio_code: Union[str,None]=None, trade_date: Union[datetime.date, Tuple[datetime.date, datetime.date], None]=None) -> List[Transaction]:
        # Assign from & to dates based on type of trade_date
        if isinstance(trade_date, tuple):
            from_date, to_date = trade_date[0], trade_date[1]
        elif isinstance(trade_date, datetime.date):
            from_date = to_date = trade_date
        else:
            from_date = to_date = None

        # Query table
        res_df = self.table.read(portfolio_code=portfolio_code, from_date=from_date, to_date=to_date)

        # Loop thru df and build a list of transactions:
        transactions = []
        for i, row in res_df.iterrows():
            txn_dict = {}
            for cm in reversed(self.txn2table_column_mappings):
                col_value = row.get(cm.table_column)
                txn_dict[cm.transaction_attribute] = col_value

            # Now we have a dict containing the desired values, with attribute names as the keys. 
            # Create the Transaction and append to the transaction list:
            txn = Transaction(**txn_dict)
            transactions.append(txn)

        return transactions


class COREDBSFTransactionRepository(TransactionRepository):
    table = COREDBSFTransactionTable()
    readable_name = table.readable_name
    txn2table_column_mappings = [
        # apx2sf.pl @SF_TRANSACTIONS_feed
        # ({transaction attribute}, {table column}, {txn2table_format_function}, {table2txn_format_function})
        # In cases where the same txn attribute maps to multiple table columns, the more desirable column should be listed first (higher)
        # In cases where the same table column maps to multiple txn attributes, the more desirable attribute should be listed first (higher)
        Txn2TableColMap('lw_lineage'       , 'lw_lineage'),
        Txn2TableColMap('TradeDate'        , 'data_dt'),
        Txn2TableColMap('PortfolioCode'    , 'portfolio_code'),
        Txn2TableColMap('ProprietarySymbol1', 'security_id__c'
                            , lambda x: '' if x is None or not x else x),
        Txn2TableColMap('ProprietarySymbol', 'security_id__c'
                            , lambda x: '' if x is None or not x else x),
        Txn2TableColMap('Symbol1'          , 'symbol__c'),
        Txn2TableColMap('Symbol'           , 'symbol__c'),
        Txn2TableColMap('Name4Stmt'        , 'name4stmt__c'),
        Txn2TableColMap('TransactionCode'  , 'tran_code__c'),
        Txn2TableColMap('TransactionName'  , 'tran_desc__c'),
        Txn2TableColMap('Comment01'        , 'comment01__c'),
        Txn2TableColMap('TradeDateDT'      , 'trade_date__c'),
        Txn2TableColMap('TradeDate'        , 'trade_date_string__c'
                            , lambda x: x.strftime('%b %e, %Y').replace('  ', ' ') if isinstance(x, datetime.date) else x
                            , lambda x: datetime.strptime(x, '%b %e, %Y').date() if len(x) else None),
        Txn2TableColMap('SettleDateDT'     , 'settle_date__c'),
        Txn2TableColMap('SettleDate'       , 'settle_date_string__c'
                            , lambda x: x.strftime('%b %e, %Y').replace('  ', ' ') if isinstance(x, datetime.date) else x
                            , lambda x: datetime.strptime(x, '%b %e, %Y').date() if len(x) else None),
        Txn2TableColMap('PrincipalCurrencyISOCode1', 'sec_ccy__c'),
        Txn2TableColMap('ReportingCurrencyISOCode', 'port_ccy__c'),
        Txn2TableColMap('TradeDate'        , 'group_ccy', lambda x: 'CAD'),  # TODO: remove this? Or figure out its relevance if always CAD?
        Txn2TableColMap('TradeDate'        , 'firm_ccy', lambda x: 'CAD'),  # TODO: remove this? Or figure out its relevance if always CAD?
        Txn2TableColMap('SfPortfolioID'    , 'Portfolio__c'),
        # Txn2TableColMap('SfGroupID'       , 'sf_statement_group_Id'), # Commented out in Perl
        Txn2TableColMap('SfGroupCode'      , 'sf_statement_group'),  # TODO: remove this? (not required)
        Txn2TableColMap('Quantity'         , 'quantity__c'
                            , lambda x: x if not x else normal_round(x, 2)),
        Txn2TableColMap('TradeAmountLocal' , 'trade_amt_sec__c'
                            , lambda x: x if not x else normal_round(x, 2)),
        Txn2TableColMap('TradeAmount'      , 'trade_amt_port__c'
                            , lambda x: x if not x else normal_round(x, 2)),
        Txn2TableColMap('TradeAmountFirm'  , 'trade_amt_firm__c'
                            , lambda x: x if not x else normal_round(x, 2)),
        Txn2TableColMap('CashFlowLocal'    , 'cash_flow_sec__c'
                            , lambda x: x if not x else normal_round(x, 2)),
        Txn2TableColMap('CashFlow'         , 'cash_flow_port__c'
                            , lambda x: x if not x else normal_round(x, 2)),
        Txn2TableColMap('CashFlowFirm'     , 'cash_flow_firm__c'
                            , lambda x: x if not x else normal_round(x, 2)),
        Txn2TableColMap('PricePerUnit'     , 'price_per_unit_port__c'
                            , lambda x: x if not x else normal_round(x, 9)),
        Txn2TableColMap('PricePerUnitLocal', 'price_per_unit_sec__c'
                            , lambda x: x if not x else normal_round(x, 9)),
        Txn2TableColMap('RealizedGain'     , 'realized_gain_port__c'
                            , lambda x: x if not x else normal_round(x, 2)),
        Txn2TableColMap('Commission'       , 'commission__c'
                            , lambda x: x if not x else normal_round(x, 2)),
        Txn2TableColMap('DataHandle'       , 'data_handle'),
        Txn2TableColMap('LocalTranKey'     , 'lw_tran_id__c'),
        Txn2TableColMap('WarnCode'         , 'warn_code'
                            , lambda x: 0 if not x else x),
        Txn2TableColMap('Warning'          , 'warning'
                            , lambda x: '' if x is None or not x else x),
        Txn2TableColMap('trade_date_original', 'trade_date_original'),
    ]

    def create(self, transactions: Union[List[Transaction],Transaction]) -> int:
        if isinstance(transactions, Transaction):
            transactions = [transactions]

        # Loop thru; produce list of dicts. Each will have keys matching table column names
        table_ready_dicts = []
        delete_stmts = []
        now = datetime.datetime.now()
        common_dict = {
            'gendate': now,
            'moddate': now,
            'genuser': (f"{os.getlogin()}_{os.environ.get('APP_NAME') or os.path.basename(__file__)}")[:32],
            'moduser': (f"{os.getlogin()}_{os.environ.get('APP_NAME') or os.path.basename(__file__)}")[:32],
            'computer': socket.gethostname().upper()
        }
        for txn in transactions:
            table_ready_dict = common_dict.copy()
            for cm in reversed(self.txn2table_column_mappings):
                # if hasattr(txn, cm.transaction_attribute):
                # If the attribute exists for this transaction, populate the dict with its value
                attr_value = getattr(txn, cm.transaction_attribute, None)

                # # Round, if specified
                # if cm.round_to_decimal_places and attr_value:
                #     attr_value = normal_round(attr_value, cm.round_to_decimal_places)

                # # Replace None with 0.0, if specified
                # if cm.populate_none_with_zero and (not attr_value or pd.isna(attr_value)):
                #     attr_value = 0.0

                # Apply formatting function, if specified
                if cm.txn2table_format_function:
                    attr_value = cm.txn2table_format_function(attr_value)

                # Assign value in dict
                table_ready_dict[cm.table_column] = attr_value
            
            # Now we have the dict containing all desired values for the row. Append it:
            table_ready_dicts.append(table_ready_dict)

            # Also create & append delete stmt, if it's not already there:
            delete_stmt = sql.delete(self.table.table_def)
            delete_stmt = delete_stmt.where(self.table.c.portfolio_code == txn.portfolio_code)
            delete_stmt = delete_stmt.where(self.table.c.trade_date_original == txn.trade_date_original)
            if delete_stmt not in delete_stmts:
                delete_stmts.append(delete_stmt)

        # Now we have a list of dicts. Convert to df to facilitate bulk insert: 
        df = pd.DataFrame(table_ready_dicts)

        # Delete old results
        for stmt in delete_stmts:
            logging.debug(f'Deleting old results from {self.table.cn}... {str(stmt)}')
            delete_res = self.table.execute_write(stmt)

        # Bulk insert df
        logging.info(f'Inserting {len(df)} new results to {self.table.cn}...')
        res = self.table.bulk_insert(df)

        # Reutrn row count
        return res.rowcount


    def get(self, portfolio_code: Union[str,None]=None, trade_date: Union[datetime.date, Tuple[datetime.date, datetime.date], None]=None) -> List[Transaction]:
        pass

