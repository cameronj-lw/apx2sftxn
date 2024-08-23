
# core python
from abc import ABC
from dataclasses import dataclass, field
import datetime
import logging
from typing import Any, Dict, List, Tuple, Type, Union

# native
from domain.models import PKColumnMapping, Transaction
from domain.repositories import SupplementaryRepository
from infrastructure.sql_procs import (
    APXRepDBpAPXReadSecurityHashProc, APXRepDBGroupMembersFlattenedFunc,
    APXDBRealizedGainLossProcAndFunc,
)
from infrastructure.sql_tables import (
    APXDBvPortfolioView, APXDBvPortfolioBaseView, APXDBvPortfolioBaseCustomView, APXDBvPortfolioSettingExView, APXDBvPortfolioBaseSettingExView, 
    APXDBvCurrencyView, APXDBvSecurityView, APXDBvFXRateView, APXDBvCustodianView,
    COREDBAPXfRealizedGainLossTable,
    APXRepDBvPortfolioAndStmtGroupCurrencyView, 
    CoreDBSFPortfolioLatestView,
)
from infrastructure.util.dataframe import df_to_dict
from infrastructure.util.stored_proc import BaseStoredProc
from infrastructure.util.table import BaseTable


@dataclass
class InMemoryRepository(SupplementaryRepository):
    current_data: Dict[str, Any] = field(default_factory=dict)

    def create(self, data: Dict[str, Any]) -> int:
        # Build composite PK values
        composite_pk_values = self._get_composite_pk_values(data)
        
        # Now we have the composite PK, which will be used as the key in current_data
        self.current_data[composite_pk] = data

        # Return 1 to indicate "successful" saving of 1 row of data
        return 1

    def get(self, pk_column_values: Dict[str, Any]) -> dict:
        # Build composite PK values
        composite_pk_values = self._get_composite_pk_values(pk_column_values)

        # Now we have the composite PK, which will be used as the key in current_data
        return self.current_data.get(composite_pk_values)

    def _get_composite_pk_values(self, data: Dict[str, Any]) -> Tuple:
        # Build composite PK values, as a tuple
        values = ()
        for m in self.pk_columns:
            values += (data.get(m.supplementary_column_name),)
        return values


class InMemorySingletonSQLRepository(InMemoryRepository):
    _instance = None
    _initialized = False

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(InMemorySingletonSQLRepository, cls).__new__(cls)
        return cls._instance

    def __init__(self, pk_columns=None, sql_source=None, portfolio_code_columns=None, trade_date_columns=None, relevant_columns=None, initialize_from_sql=True):
        if self._initialized:
            return
        self._initialized = True

        self.pk_columns = pk_columns if pk_columns else []
        self.sql_source = sql_source
        self.portfolio_code_columns = portfolio_code_columns if portfolio_code_columns else ['PortfolioCode', 'PortfolioBaseCode']
        self.trade_date_columns = trade_date_columns if trade_date_columns else ['TradeDate']
        self.relevant_columns = relevant_columns if relevant_columns else []
        self.current_data = {}

        if self.sql_source and initialize_from_sql:
            logging.info(f'Initializing {self.__class__.__name__} with data from {self.sql_source.__name__}')
            self.refresh()
        
    def refresh(self, params: Dict = {}):
        """ Refresh in-memory data for provided criteria """
        if not self.sql_source:
            raise ValueError("sql_source is not defined.")
        
        new_data_df = self.sql_source().read(**params)

        if len(self.relevant_columns):
            all_relevant_columns = (self.relevant_columns + self.portfolio_code_columns + self.trade_date_columns
                                    + [cm.supplementary_column_name for cm in self.pk_columns])
            new_data_df = new_data_df.reindex(columns=set(all_relevant_columns).intersection(new_data_df.columns))

        if 'portfolio_code' not in new_data_df.columns:
            for col in self.portfolio_code_columns:
                if col in new_data_df.columns:
                    new_data_df['portfolio_code'] = new_data_df[col]

        if 'trade_date' not in new_data_df.columns:
            for col in self.trade_date_columns:
                if col in new_data_df.columns:
                    new_data_df['trade_date'] = new_data_df[col]

        pk_col_names = [cm.supplementary_column_name for cm in self.pk_columns]
        new_data = df_to_dict(df=new_data_df, pk_col_names=pk_col_names)
        logging.debug(f'{self.cn} refreshing for {new_data}')
        self.current_data.update(new_data)


class APXRepDBSecurityHashInMemoryRepository(InMemorySingletonSQLRepository):
    def __init__(self):
        super().__init__(pk_columns=[PKColumnMapping('SecurityID')], sql_source=APXRepDBpAPXReadSecurityHashProc
                            , relevant_columns=['Name4Stmt', 'Name4Trading'])

    def supplement(self, transaction: Transaction) -> Union[Dict, None]:
        for suffix in ('1', '2'):                
            # Get PK column values
            pk_column_values = {'SecurityID': getattr(transaction, f'SecurityID{suffix}')}

            # Now we have a dict containing all desired filtering criteria. Get with that criteria:
            supplemental_data = self.get(pk_column_values=pk_column_values)

            if isinstance(supplemental_data, dict):
                # Update the transaction
                for key, value in supplemental_data.items():
                    setattr(transaction, f'{key}{suffix}', value)

        return supplemental_data

class APXDBvPortfolioInMemoryRepository(InMemorySingletonSQLRepository):    
    def __init__(self):
        super().__init__(pk_columns=[PKColumnMapping('PortfolioID', 'PortfolioID')], sql_source=APXDBvPortfolioView
                            , relevant_columns=['PortfolioCode', 'PortfolioTypeCode'])
    
class APXDBvPortfolioBaseInMemoryRepository(InMemorySingletonSQLRepository):
    def __init__(self):
        super().__init__(pk_columns=[PKColumnMapping('PortfolioBaseID', 'PortfolioBaseID')], sql_source=APXDBvPortfolioBaseView
                            , relevant_columns=['ReportHeading1'])

class APXDBvPortfolioBaseCustomInMemoryRepository(InMemorySingletonSQLRepository):
    def __init__(self):
        super().__init__(pk_columns=[PKColumnMapping('PortfolioBaseID', 'PortfolioBaseID')], sql_source=APXDBvPortfolioBaseCustomView
                            , relevant_columns=['CustAcctNotify'])

class APXDBvPortfolioSettingExInMemoryRepository(InMemorySingletonSQLRepository):
    def __init__(self):
        super().__init__(pk_columns=[PKColumnMapping('PortfolioBaseID', 'PortfolioID')], sql_source=APXDBvPortfolioSettingExView
                            , relevant_columns=['CustodianID'])

class APXDBvPortfolioBaseSettingExInMemoryRepository(InMemorySingletonSQLRepository):
    def __init__(self):
        super().__init__(pk_columns=[PKColumnMapping('PortfolioBaseID', 'PortfolioBaseID')], sql_source=APXDBvPortfolioBaseSettingExView
                            , relevant_columns=['ReportingCurrencyCode'])
                            

class APXDBvCurrencyInMemoryRepository(InMemorySingletonSQLRepository):
    def __init__(self):
        super().__init__(pk_columns=[PKColumnMapping('FXNumeratorCurrencyCode', 'CurrencyCode')], sql_source=APXDBvCurrencyView
                            , relevant_columns=['ISOCode'])

    def supplement(self, transaction: Transaction) -> Union[Dict, None]:
        if hasattr(transaction, 'FXNumeratorCurrencyCode'):
            # Don't do the superclass supplement unless the required txn attribute exists
            # TODO: better way to avoid the error when using this class as part of APX2SFTxn engine?
            super().supplement(transaction)
        
        # Also supplement ReportingCurrencyCode with ReportingCurrencyISOCode
        if hasattr(transaction, 'ReportingCurrencyCode'):
            pk_column_values = {'CurrencyCode': transaction.ReportingCurrencyCode}

            # Now we have a dict containing all desired filtering criteria. Get with that criteria:
            supplemental_data = self.get(pk_column_values=pk_column_values)

            # Update the transaction
            transaction.ReportingCurrencyISOCode = supplemental_data['ISOCode']

        # Also supplement PrincipalCurrencyCode1 with PrincipalCurrencyISOCode1
        if hasattr(transaction, 'PrincipalCurrencyCode1'):
            pk_column_values = {'CurrencyCode': transaction.PrincipalCurrencyCode1}

            # Now we have a dict containing all desired filtering criteria. Get with that criteria:
            supplemental_data = self.get(pk_column_values=pk_column_values)

            # Update the transaction
            transaction.PrincipalCurrencyISOCode1 = supplemental_data['ISOCode']

class APXDBvSecurityInMemoryRepository(InMemorySingletonSQLRepository):
    def __init__(self):
        super().__init__(pk_columns=[PKColumnMapping('SecurityID')], sql_source=APXDBvSecurityView
                            , relevant_columns=['ProprietarySymbol', 'PrincipalCurrencyCode', 'FullName', 'Symbol', 'SecTypeBaseCode'
                                                , 'CouponDelayDays', 'MaturityDate', ])  
        
    def supplement(self, transaction: Transaction) -> Union[Dict, None]:
        for suffix in ('1', '2'):                
            # Get PK column values
            pk_column_values = {'SecurityID': getattr(transaction, f'SecurityID{suffix}')}

            # Now we have a dict containing all desired filtering criteria. Get with that criteria:
            supplemental_data = self.get(pk_column_values=pk_column_values)

            if isinstance(supplemental_data, dict):
                # Update the transaction
                for key, value in supplemental_data.items():
                    setattr(transaction, f'{key}{suffix}', value)

                # For CouponDelayDays: 253 is the APX internal value for "use Sec Type". 
                # If we find this value is 253, set a separate attribute of the transaction.
                # This would allow application layer logic to use it, without infrastructure layer bleeding into application layer.
                if supplemental_data['CouponDelayDays'] == 253:
                    setattr(transaction, f'UseSecTypeForCouponDelayDays{suffix}', True)

        return supplemental_data


# class APXDBvFXRateByPortfolioAndDateInMemoryRepository(InMemorySingletonSQLRepository):
#     def __init__(self):
#         super().__init__(pk_columns=[
#                             PKColumnMapping('portfolio_code'),
#                             PKColumnMapping('TradeDate', 'PriceDate'), 
#                         ], sql_source=APXDBvFXRateView
#                         , relevant_columns=['SpotRate'])


class APXDBvFXRateInMemoryRepository(InMemorySingletonSQLRepository):
    def __init__(self):
        super().__init__(pk_columns=[
                            PKColumnMapping('TradeDate', 'PriceDate'),
                            PKColumnMapping('FXNumeratorCurrencyCode', 'NumeratorCurrencyCode'), 
                            PKColumnMapping('FXDenominatorCurrencyCode', 'DenominatorCurrencyCode'),
                        ], sql_source=APXDBvFXRateView
                        , relevant_columns=['SpotRate'])

    def pre_supplement(self, portfolio_code: Union[str,None]=None, trade_date: Union[datetime.date, Tuple[datetime.date, datetime.date], None]=None):
        # Infer from date & to date from trade_date
        if isinstance(trade_date, tuple):
            from_date, to_date = trade_date
        elif isinstance(trade_date, datetime.date):
            from_date = to_date = trade_date
        elif trade_date:
            logging.error(f'{type(trade_date).__name__}: invalid arg for {self.cn} pre_supplement trade_date: {trade_date}')
        else:
            from_date = to_date = None

        # Populate for trade date(s)
        d = from_date
        while d <= to_date:
            self.refresh(params={'PriceDate': d})
            self.current_data.update({(d, None, None): {'SpotRate': 1.0}})  # Also add spot rate 1.0 for transactions with no FX***CurrencyCode
            d += datetime.timedelta(days=1)

    def supplement(self, transaction: Transaction) -> Union[Dict, None]:
        supplemental_data = super().supplement(transaction)

        # Additionally, populate portfolio2firm_fx_rate
        
        # If CAD portfolio, portfolio2firm_fx_rate is 1.0
        if transaction.ReportingCurrencyCode == 'ca':
            transaction.portfolio2firm_fx_rate = 1.0
        # Otherwise, need to try to find the SpotRate:
        else:
            # Query provided repo for these values
            pk_column_values = {
                'PriceDate'                 : transaction.TradeDate,
                'NumeratorCurrencyCode'     : 'ca',  # Because it's the firm currency (CAD)
                'DenominatorCurrencyCode'   : transaction.ReportingCurrencyCode,
            }   
            get_res = self.get(pk_column_values=pk_column_values)

            # Assign as spot rate and return
            transaction.portfolio2firm_fx_rate = get_res.get('SpotRate')
        
        return supplemental_data

    def post_supplement(self, portfolio_code: Union[str,None]=None, trade_date: Union[datetime.date, Tuple[datetime.date, datetime.date], None]=None):
        # Remove in-memory data as cleanup
        self.current_data = {}

    def refresh(self, params: Dict={}):
        """ Avoid refreshing if no criteria are provided """
        if params.get('PriceDate'):
            super().refresh(params=params)
        else:
            # Since the view contains many FX rates for every day, refreshing without specifying a date is not feasible
            pass  # TODO_EH: any logging or other behaviour desired here?

    def get(self, pk_column_values: Dict[str, Any]) -> dict:
        # First, try to get values from existing in-memory:
        get_res = super().get(pk_column_values=pk_column_values)
        if get_res and len(get_res):
            # If we got data, return it.
            return get_res
        else:
            # If no data, refresh and then try:
            self.refresh(params=pk_column_values)
            return super().get(pk_column_values=pk_column_values)


class APXDBvCustodianInMemoryRepository(InMemorySingletonSQLRepository):
    def __init__(self):
        super().__init__(pk_columns=[PKColumnMapping('CustodianID')], sql_source=APXDBvCustodianView
                            , relevant_columns=['CustodianName'])

    def supplement(self, transaction: Transaction) -> Union[Dict, None]:
        # Combine the CustodianID into the name (APXTxns.pm line 984)
        supplemental_data = super().supplement(transaction)
        if transaction.CustodianName:
            transaction.CustodianName = f"{transaction.CustodianName} ({int(transaction.CustodianID)})"

        return supplemental_data

class APXRepDBvPortfolioAndStmtGroupCurrencyInMemoryRepository(InMemorySingletonSQLRepository):
    def __init__(self):
        super().__init__(pk_columns=[PKColumnMapping('PortfolioCode')], sql_source=APXRepDBvPortfolioAndStmtGroupCurrencyView
                            , relevant_columns=['PortfolioISOCode', 'PortfolioGroupISOCode'])


class CoreDBSFPortfolioLatestInMemoryRepository(InMemorySingletonSQLRepository):
    def __init__(self):
        super().__init__(pk_columns=[PKColumnMapping('PortfolioCode', 'LW_Portfolio_ID__c')], sql_source=CoreDBSFPortfolioLatestView
                            , relevant_columns=['PortfolioCurrencyISOCode', 'StatementGroupCurrencyISOCode', 'Id'])

    def pre_supplement(self, portfolio_code: Union[str,None]=None, trade_date: Union[datetime.date, Tuple[datetime.date, datetime.date], None]=None):
        # If portfolio_code is a group, refresh for all. Otherwise, refresh for only the portfolio_code
        if isinstance(portfolio_code, str):
            if portfolio_code[0] == '@':
                self.refresh()
            else:
                self.refresh(params={'LW_Portfolio_ID__c': portfolio_code})
        else:
            self.refresh()

    def supplement(self, transaction: Transaction) -> Union[Dict, None]:
        # Also assign the SF Portfolio ID
        supplemental_data = super().supplement(transaction)
        if supplemental_data:
            if sf_portfolio_id := supplemental_data.get('Id'):
                transaction.SfPortfolioID = sf_portfolio_id

        return supplemental_data


class CoreDBRealizedGainLossInMemoryRepository(InMemorySingletonSQLRepository):
    portfolio_code_expander = APXRepDBGroupMembersFlattenedFunc()

    def __init__(self):
        super().__init__(pk_columns=[
                                    PKColumnMapping('PortfolioTransactionID'), 
                                    PKColumnMapping('TranID'), 
                                    PKColumnMapping('LotNumber'), 
                                ], sql_source=COREDBAPXfRealizedGainLossTable
                            , relevant_columns=['RealizedGainLoss', 'RealizedGainLossLocal', 'CostBasis', 'CostBasisLocal', 'Quantity']
                            , initialize_from_sql=False)
    
    def pre_supplement(self, portfolio_code: Union[str,None]=None, trade_date: Union[datetime.date, Tuple[datetime.date, datetime.date], None]=None):
        # Infer from date & to date from trade_date
        if isinstance(trade_date, tuple):
            from_date, to_date = trade_date
        elif isinstance(trade_date, datetime.date):
            from_date = to_date = trade_date
        elif trade_date:
            logging.error(f'{type(trade_date).__name__}: invalid arg for {self.cn} pre_supplement trade_date: {trade_date}')
        else:
            from_date = to_date = None

        # Refresh for specified portfolio_code and from/to dates
        self.refresh(params={'portfolio_code': portfolio_code, 'from_date': from_date, 'to_date': to_date})

    def supplement(self, transaction: Transaction) -> Union[Dict, None]:
        # Save original quantity (we need to save it back after to avoid it getting overwritten)
        quantity_orig = transaction.Quantity

        # Supplement as normal
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

        # Return supplemental data
        return supplemental_data

    def post_supplement(self, portfolio_code: Union[str,None]=None, trade_date: Union[datetime.date, Tuple[datetime.date, datetime.date], None]=None):
        # Remove in-memory data as cleanup
        self.current_data = {}


class APXDBRealizedGainLossInMemoryRepository(InMemorySingletonSQLRepository):

    def __init__(self):
        super().__init__(pk_columns=[
                                    PKColumnMapping('PortfolioTransactionID'), 
                                    PKColumnMapping('TranID'), 
                                    PKColumnMapping('LotNumber'), 
                                ], sql_source=APXDBRealizedGainLossProcAndFunc
                            , relevant_columns=['RealizedGainLoss', 'RealizedGainLossLocal', 'CostBasis', 'CostBasisLocal', 'Quantity']
                            , initialize_from_sql=False)
    
    def pre_supplement(self, portfolio_code: Union[str,None]=None, trade_date: Union[datetime.date, Tuple[datetime.date, datetime.date], None]=None):
        # Infer from date & to date from trade_date
        if isinstance(trade_date, tuple):
            from_date, to_date = trade_date
        elif isinstance(trade_date, datetime.date):
            from_date = to_date = trade_date
        elif trade_date:
            logging.error(f'{type(trade_date).__name__}: invalid arg for {self.cn} pre_supplement trade_date: {trade_date}')
        else:
            from_date = to_date = None

        # Refresh for specified portfolio_code and from/to dates
        self.refresh(params={'Portfolios': portfolio_code, 'FromDate': from_date, 'ToDate': to_date})

    def supplement(self, transaction: Transaction) -> Union[Dict, None]:
        # Save original quantity (we need to save it back after to avoid it getting overwritten)
        quantity_orig = transaction.Quantity

        # Supplement as normal
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

        # Return supplemental data
        return supplemental_data

    def post_supplement(self, portfolio_code: Union[str,None]=None, trade_date: Union[datetime.date, Tuple[datetime.date, datetime.date], None]=None):
        # Remove in-memory data as cleanup
        self.current_data = {}

