
# core python
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
from domain.repositories import HeartbeatRepository, TransactionRepository, TransactionProcessingQueueRepository, SupplementaryRepository
from infrastructure.in_memory_repositories import CoreDBRealizedGainLossInMemoryRepository
from infrastructure.models import MGMTDBHeartbeat
from infrastructure.sql_procs import APXDBRealizedGainLossProcAndFunc, APXDBTransactionActivityProcAndFunc
from infrastructure.sql_tables import (
    APXRepDBLWTxnSummaryTable,
    MGMTDBMonitorTable, 
    COREDBSFTransactionTable, 
    COREDBAPXfRealizedGainLossQueueTable, COREDBAPXfRealizedGainLossTable, 
    COREDBAPXfTransactionActivityQueueTable, COREDBAPXfTransactionActivityTable, 
    COREDBLWTransactionSummaryQueueTable, LWDBAPXAppraisalTable, COREDBLWTransactionSummaryTable,
    COREDBLWTxnSummaryTable,
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
            logging.info(f"Found 0 rows in {self.table.cn} for {prev_bday} {pk_column_values.get('PortfolioCode')} {pk_column_values.get('SecurityID')}!")
            return {}

    def supplement(self, transaction: Transaction):
        super().supplement(transaction)

        # Additionally, update the transaction's per-unit values:
        transaction.LocalCostBasis = transaction.LocalCostPerUnit * transaction.Quantity
        transaction.RptCostBasis = transaction.RptCostPerUnit * transaction.Quantity

""" APXDB """

class APXDBRealizedGainLossRepository(TransactionRepository):
    proc = APXDBRealizedGainLossProcAndFunc()
    
    def create(self, transactions: Union[List[Transaction],Transaction]) -> int:
        raise NotImplementedError(f'Cannot create in {self.cn}!')

    def get(self, portfolio_code: Union[str,None]=None, trade_date: Union[datetime.date, Tuple[datetime.date, datetime.date], None]=None) -> List[Transaction]:
        res_df = self.proc.read(Portfolios=portfolio_code, FromDate=trade_date, ToDate=trade_date)
        transactions = [Transaction(**d) for d in res_df.to_dict('records')]
        return transactions

    def __str__(self):
        return str(self.proc)


class APXDBTransactionActivityRepository(TransactionRepository):
    proc = APXDBTransactionActivityProcAndFunc()
    
    def create(self, transactions: Union[List[Transaction],Transaction]) -> int:
        raise NotImplementedError(f'Cannot create in {self.cn}!')

    def get(self, portfolio_code: Union[str,None]=None, trade_date: Union[datetime.date, Tuple[datetime.date, datetime.date], None]=None) -> List[Transaction]:
        res_df = self.proc.read(Portfolios=portfolio_code, FromDate=trade_date, ToDate=trade_date)
        transactions = [Transaction(**d) for d in res_df.to_dict('records')]
        return transactions

    def __str__(self):
        return str(self.proc)


class APXDBDividendRepository(TransactionRepository):
    txn_proc = APXDBTransactionActivityProcAndFunc()
    realized_gains_proc = APXDBRealizedGainLossProcAndFunc()
    
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
        
        res_df = self.txn_proc.read(Portfolios=portfolio_code, FromDate=from_date, ToDate=trade_date)
        transactions = [Transaction(**d) for d in res_df.to_dict('records')]

        # Find dividends with SETTLE date with the specified trade_date
        dividends = [t for t in transactions if t.SettleDate.date() == trade_date and t.TransactionCode == 'dv']

        if not len(dividends):
            return []

        # Read realized gains proc once (avoids reading it for every dividend separately)
        realized_gains_df = self.realized_gains_proc.read(Portfolios=portfolio_code, FromDate=trade_date, ToDate=trade_date)

        for dv in dividends:
            # APXTxns.pm line 353: jam on the LW blinders:  dv are all about SettleDate
            dv.TradeDate = dv.SettleDate

            # APXTxns.pm line 453-464: Find a wd matching the settle date, security (i.e. divacc), and having very similar amount
            wd_candidates = [t for t in transactions if t.TransactionCode in ('wd', 'dp')] 
            wd_candidates = [t for t in wd_candidates if t.SettleDate == dv.SettleDate]
            wd_candidates = [t for t in wd_candidates if t.SecurityID1 == dv.SecurityID2]
            wd_candidates = [t for t in wd_candidates if abs(t.TradeAmountLocal - dv.TradeAmountLocal) < 0.015]  # APXTxns.pm line 11

            # APXTxns.pm line 465-519: Find a sl from cash to cash, and having very similar amount
            sl_candidates = [t for t in transactions if t.TransactionCode == 'sl']
            sl_candidates = [t for t in sl_candidates if t.SettleDate == dv.SettleDate]
            sl_candidates = [t for t in sl_candidates if t.SecTypeCode1 == 'ca']
            sl_candidates = [t for t in sl_candidates if t.SecTypeCode2 == 'ca']
            sl_candidates = [t for t in sl_candidates if abs(t.TradeAmountLocal - dv.TradeAmountLocal) < 0.015]  # APXTxns.pm line 11

            if len(sl_candidates):
                sl = sl_candidates[0]

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
        return str(self.proc)


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

class CoreDBRealizedGainLossQueueRepository(TransactionProcessingQueueRepository):
    table = COREDBAPXfRealizedGainLossQueueTable()

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


class CoreDBTransactionActivityQueueRepository(TransactionProcessingQueueRepository):
    table = COREDBAPXfTransactionActivityQueueTable()

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


class CoreDBLWTransactionSummaryQueueRepository(TransactionProcessingQueueRepository):
    table = COREDBLWTransactionSummaryQueueTable()

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


class CoreDBRealizedGainLossRepository(TransactionRepository):
    table = COREDBAPXfRealizedGainLossTable()
    repo_to_refresh = CoreDBRealizedGainLossInMemoryRepository()

    def create(self, transactions: Union[List[Transaction],Transaction]) -> int:
        # May be a Transaction. If so, make it a list:
        if isinstance(transactions, Transaction):
            transactions = [transactions]

        now = datetime.datetime.now()
        delete_stmts = []
        refresh_criterias = []
        for txn in transactions:
            # Supplement transactions
            txn.modified_by = os.environ.get('APP_NAME') if not hasattr(txn, 'modified_by') else txn.modified_by
            txn.modified_at = now

            # Also create & append delete stmt, if it's not already there:
            delete_stmt = sql.delete(self.table.table_def)
            refresh_criteria = {}
            if hasattr(txn, 'portfolio_code'):
                delete_stmt = delete_stmt.where(self.table.c.portfolio_code == txn.portfolio_code)
                refresh_criteria['portfolio_code'] = txn.portfolio_code
            if hasattr(txn, 'PortfolioCode'):
                delete_stmt = delete_stmt.where(self.table.c.portfolio_code == txn.PortfolioCode)
                refresh_criteria['portfolio_code'] = txn.PortfolioCode
            if hasattr(txn, 'PortfolioBaseCode'):
                delete_stmt = delete_stmt.where(self.table.c.portfolio_code == txn.PortfolioBaseCode)
                refresh_criteria['portfolio_code'] = txn.PortfolioBaseCode            
            if hasattr(txn, 'CloseDate'):
                delete_stmt = delete_stmt.where(self.table.c.CloseDate == txn.CloseDate)
                refresh_criteria['from_date'] = txn.CloseDate
            elif hasattr(txn, 'trade_date'):
                delete_stmt = delete_stmt.where(self.table.c.CloseDate == txn.trade_date)
                refresh_criteria['from_date'] = txn.trade_date
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
        for stmt in delete_stmts:
            logging.debug(f'Deleting old results from {self.table.cn}... {str(stmt)}')
            delete_res = self.table.execute_write(stmt)

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
        if isinstance(trade_date, tuple):
            from_date, to_date = trade_date
        elif isinstance(trade_date, datetime.date):
            from_date = to_date = trade_date
        elif trade_date:
            logging.error(f'Invalid arg for {self.cn} GET: {trade_date}')
        res_df = self.table.read(portfolio_code=portfolio_code, from_date=from_date, to_date=to_date)
        res_transactions = [Transaction(**r) for r in res_df.to_dict('records')]
        return res_transactions


class CoreDBTransactionActivityRepository(TransactionRepository):
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


class COREDBSFTransactionRepository(TransactionRepository):
    table = COREDBSFTransactionTable()
    
    def create(self, transactions: Union[List[Transaction],Transaction]) -> int:
        # 1. if transaction is a dividend (dv) then blank out the following fields: Quantity, PricePerUnit, CostPerUnit & CostBasis
        # 2. if transaction is a long-out, withdrawal, management fee, custodian fee or withholding tax (lo, wd, ep, ex, wt) then flip the sign on the TradeAmount
        # 3. if transaction is a client withdrawal (wd) then change security name to 'CASH WITHDRAWAL'
        # 4. if transaction is a client deposit (dp) then change security name to 'CASH DEPOSIT'
        # 5. define new attributes 'CashFlow' and 'CashFlowLocal' to reflect the cash flow impact of the transaction on the portfolio in reporting currency and local currency, respectively.
            # 5.a. if transaction is a buy then CashFlow = -1* TradeAmount, else CashFlow = TradeAmount
            # 5.b. if transaction is a buy then CashFlowLocal = -1* TradeAmountLocal, else CashFlowLocal = TradeAmountLocal
        # 6. Compare the portfolio reporting currency for the transaction as specified in APX vs SF
            # if there is a mismatch, then {
            # use the SF reporting currency
            # create a warning for the transaction (see 'warn_code' and 'warning' in the staging table)
            # }
        # 7. Compare the statement group reporting currency for the transaction in APX vs SF
            # if there is a mismatch, then {
            # use the SF reporting currency
            # create a warning for the transaction (see 'warn_code' and 'warning' in the staging table)
            # }
        pass  # TODO: implement?

    def get(self, portfolio_code: Union[str,None]=None, trade_date: Union[datetime.date, Tuple[datetime.date, datetime.date], None]=None) -> List[Transaction]:
        pass

    @classmethod
    def readable_name(self):
        return 'COREDB sf_transaction table'


class COREDBLWTxnSummaryRepository(TransactionRepository):
    table = COREDBLWTxnSummaryTable()
    txn2table_columns = [
        # ({transaction attribute}, {table column}, {number of decimal places})
        ('PortfolioCode'    , 'portfolio_code'),
        ('PortfolioName'    , 'portfolio_name'),
        ('TransactionCode'  , 'tran_code'),
        ('TradeDate'        , 'trade_date'),
        ('SettleDate'       , 'settle_date'),
        ('Symbol1'          , 'symbol'),
        # ('Cusip'    , 'cusip'),  # TODO_CLEANUP: seems cusip is always null in current table ... remove once confirmed not needed
        ('Name4Stmt'        , 'name4stmt'),
        ('Quantity'         , 'quantity', 9),
        ('TradeAmount'      , 'trade_amount', 2),
        ('CashFlow'         , 'cash_flow', 2),
        ('BrokerName'       , 'broker_name'),
        ('CustodianName'    , 'custodian_name'),
        ('CustAcctNotify'   , 'cust_acct_notify'),
        ('PricePerUnit'     , 'price_per_unit', 9),
        ('Commission'       , 'commission', 2),
        ('NetInterest'      , 'net_interest', 2),
        ('NetDividend'      , 'net_dividend', 2),
        ('NetFgnIncome'     , 'net_fgn_income', 2),
        ('CapGainsDistrib'  , 'cap_gains_distrib'),
        ('TotalIncome'      , 'tot_income', 2),
        ('RealizedGain'     , 'realized_gain', 2),
        ('TfsaContribAmt'   , 'tfsa_contrib_amt', 2),
        ('RspContribAmt'    , 'rsp_contrib_amt', 2),
        ('RetOfCapital'     , 'net_return_of_capital', 2),
        # ('SecurityID1'      , 'security_id1'),
        # ('SecurityID2'      , 'security_id2'),
        ('Symbol1'          , 'symbol1'),
        # ('Symbol2'          , 'symbol2'),
        ('SecTypeCode1'     , 'sectype1'),
        ('SecTypeCode2'     , 'sectype2'),
        # ('TxnUserDef3Name'  , 'source'),  # TODO_CLEANUP: seems source is always null in current table ... remove once confirmed not needed
        # ('PortfolioCode'    , 'market_name'),  # TODO_CLEANUP: seems market_name is always null in current table ... remove once confirmed not needed
        ('CostPerUnit'      , 'cost_per_unit', 9),
        ('CostBasis'        , 'total_cost', 2),
        ('LocalTranKey'     , 'local_tran_key'),
        ('TransactionName'  , 'tran_desc'),
        ('Comment01'        , 'comment01'),
        ('SectionDesc'      , 'section_desc'),
        ('StmtTranDesc'     , 'stmt_tran_desc'),
        ('NetEligDividend'  , 'net_elig_dividend', 2),
        ('NetNonEligDividend', 'net_non_elig_dividend', 2),
        ('FxRate'           , 'fx_rate', 4),
        ('PrincipalCurrencyISOCode1', 'fx_denom_ccy'),
        ('ReportingCurrencyISOCode', 'fx_numer_ccy'),
        ('WhFedTaxAmt'      , 'whfedtax_amt', 2),
        ('WhNrTaxAmt'       , 'whnrtax_amt', 2),
        ('TradeAmountLocal' , 'trade_amount_local', 2),
        ('CostPerUnitLocal' , 'cost_per_unit_local', 9),
        ('CostBasisLocal'   , 'total_cost_local', 2),
        ('trade_date_original', 'trade_date_original'),
    ]

    def create(self, transactions: Union[List[Transaction],Transaction]) -> int:
        if isinstance(transactions, Transaction):
            transactions = [transactions]

        # Loop thru; produce list of dicts. Each will have keys matching table column names
        txn_dicts = []
        delete_stmts = []
        now = datetime.datetime.now()
        common_dict = {
            'scenariodate': now,
            'asofdate': now,
            'asofuser': (f"{os.getlogin()}_{os.environ.get('APP_NAME') or os.path.basename(__file__)}")[:32],
            'computer': socket.gethostname().upper()
        }
        for txn in transactions:
            txn_dict = common_dict.copy()
            for cm in self.txn2table_columns:
                if hasattr(txn, cm[0]):
                    # If the attribute exists for this transaction, populate the dict with its value
                    attr_value = getattr(txn, cm[0])
                    if len(cm) > 2 and attr_value:
                        attr_value = normal_round(attr_value, cm[2])
                    txn_dict[cm[1]] = attr_value
            txn_dicts.append(txn_dict)

            # Also create & append delete stmt, if it's not already there:
            delete_stmt = sql.delete(self.table.table_def)
            delete_stmt = delete_stmt.where(self.table.c.portfolio_code == txn.portfolio_code)
            delete_stmt = delete_stmt.where(self.table.c.trade_date_original == txn.trade_date_original)
            if delete_stmt not in delete_stmts:
                delete_stmts.append(delete_stmt)

        # Now we have a list of dicts. Convert to df to facilitate bulk insert: 
        df = pd.DataFrame(txn_dicts)

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




