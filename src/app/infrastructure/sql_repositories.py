
# core python
import datetime
import logging
from typing import List, Union

# native
from domain.models import Heartbeat, Transaction
from domain.repositories import HeartbeatRepository, TransactionRepository
from infrastructure.models import MGMTDBHeartbeat
from infrastructure.sql_tables import MGMTDBMonitorTable, COREDBSFTransactionTable



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


""" COREDB """

class COREDBSFTransactionRepository(TransactionRepository):
    table = COREDBSFTransactionTable()
    
    def create(self, transaction: Transaction) -> int:
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

    def get(self, data_date: Union[datetime.date,None]=None) -> List[Transaction]:
        pass

    @classmethod
    def readable_name(self):
        return 'COREDB sf_transaction table'




