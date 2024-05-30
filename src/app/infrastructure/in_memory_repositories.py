
# core python
from abc import ABC
from dataclasses import dataclass, field
import logging
from typing import Any, Dict, List, Tuple, Type, Union

# native
from domain.models import PKColumnMapping, Transaction
from domain.repositories import SupplementaryRepository
from infrastructure.sql_procs import APXRepDBpAPXReadSecurityHashProc, APXRepDBpReadvPortfolioBaseSettingExProc
from infrastructure.sql_tables import (
    APXDBvPortfolioView, APXDBvPortfolioBaseView, APXDBvPortfolioBaseCustomView, APXDBvPortfolioSettingExView, APXDBvPortfolioBaseSettingExView, 
    APXDBvCurrencyView, APXDBvSecurityView, APXDBvFXRateView, APXDBvCustodianView,
    COREDBAPXfRealizedGainLossTable,
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
        # Build composite PK values
        values = ()
        for m in self.pk_columns:
            values += (data.get(m.supplementary_column_name),)
        return values


@dataclass
class InMemorySingletonSQLRepository(InMemoryRepository):
    sql_source: Type[Union[BaseTable,BaseStoredProc,None]] = None
    _instance = None
    portfolio_code_columns = ['PortfolioCode', 'PortfolioBaseCode']
    trade_date_columns = ['TradeDate']
    relevant_columns: List[str] = field(default_factory=list)

    def __post_init__(self):
        # Read initial data into df
        logging.info(f'Initializing {self.cn} with data from {self.sql_source.__name__}')
        self.refresh()

        # initial_data_df = self.sql_source().read()

        # # Convert df to dict. Keys in dict wil be tuples with values of each PK column
        # pk_col_names = [cm.supplementary_column_name for cm in self.pk_columns]
        # initial_data = df_to_dict(df=initial_data_df, pk_col_names=pk_col_names)

        # # Now we have the dict. Store it as current_data.
        # self.current_data = initial_data

    def refresh(self, params: Dict={}):
        """ Refresh in-memory data for provided criteria """
        new_data_df = self.sql_source().read(**params)

        # condense to only relevant columns
        # If there are no relevant_columns provided, the assumption is all columns should remain.
        if len(self.relevant_columns):
            all_relevant_columns = (self.relevant_columns + self.portfolio_code_columns + self.trade_date_columns
                                        + [cm.supplementary_column_name for cm in self.pk_columns])
            new_data_df = new_data_df.reindex(columns=set(all_relevant_columns).intersection(new_data_df.columns))

        # Add portfolio_code and trade_date, if they DNE
        if 'portfolio_code' not in new_data_df.columns:
            for col in self.portfolio_code_columns:
                if col in new_data_df.columns:
                    new_data_df['portfolio_code'] = new_data_df[col]

        if 'trade_date' not in new_data_df.columns:
            for col in self.trade_date_columns:
                if col in new_data_df.columns:
                    new_data_df['trade_date'] = new_data_df[col]

        # Convert df to dict. Keys in dict wil be tuples with values of each PK column
        pk_col_names = [cm.supplementary_column_name for cm in self.pk_columns]
        new_data = df_to_dict(df=new_data_df, pk_col_names=pk_col_names)

        # Now we have the dict. Update the existing one.
        self.current_data.update(new_data)

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __new__(cls, *args, **kwargs):
        # Override default python method to enforce singularity
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance


class APXRepDBSecurityHashInMemoryRepository(InMemorySingletonSQLRepository):
    def __init__(self):
        super().__init__(pk_columns=[PKColumnMapping('SecurityID')], sql_source=APXRepDBpAPXReadSecurityHashProc
                            , relevant_columns=['Name4Stmt', 'Name4Trading'])

    def supplement(self, transaction: Transaction):
        for suffix in ('1', '2'):                
            # Get PK column values
            pk_column_values = {'SecurityID': getattr(transaction, f'SecurityID{suffix}')}

            # Now we have a dict containing all desired filtering criteria. Get with that criteria:
            supplemental_data = self.get(pk_column_values=pk_column_values)

            # Update the transaction
            for key, value in supplemental_data.items():
                setattr(transaction, f'{key}{suffix}', value)

class APXDBvPortfolioInMemoryRepository(InMemorySingletonSQLRepository):    
    def __init__(self):
        super().__init__(pk_columns=[PKColumnMapping('PortfolioBaseID', 'PortfolioID')], sql_source=APXDBvPortfolioView
                            , relevant_columns=['PortfolioTypeCode'])
    
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
        # TODO: any different PK columns desired, e.g. also for DenomCurrCode?
        # TODO: should this class override the SupplementaryRepository.supplement?
        super().__init__(pk_columns=[PKColumnMapping('FXNumeratorCurrencyCode', 'CurrencyCode')], sql_source=APXDBvCurrencyView
                            , relevant_columns=['ISOCode'])

    def supplement(self, transaction: Transaction):
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
        # The instance shall not have pk_columns, since we implement our own "Supplement" in order to supplement both Security1 and Security2 of the txn
        super().__init__(pk_columns=[PKColumnMapping('SecurityID')], sql_source=APXDBvSecurityView
                            , relevant_columns=['ProprietarySymbol', 'PrincipalCurrencyCode', 'FullName', 'Symbol', 'SecTypeBaseCode'
                                                , 'CouponDelayDays', 'MaturityDate', ])  
        
    def supplement(self, transaction: Transaction):
        for suffix in ('1', '2'):                
            # Get PK column values
            pk_column_values = {'SecurityID': getattr(transaction, f'SecurityID{suffix}')}

            # Now we have a dict containing all desired filtering criteria. Get with that criteria:
            supplemental_data = self.get(pk_column_values=pk_column_values)

            # Update the transaction
            for key, value in supplemental_data.items():
                setattr(transaction, f'{key}{suffix}', value)

class APXDBvFXRateInMemoryRepository(InMemorySingletonSQLRepository):
    def __init__(self):
        super().__init__(pk_columns=[
                            PKColumnMapping('TradeDate', 'AsOfDate'),
                            PKColumnMapping('NumeratorCurrCode'), 
                            PKColumnMapping('DenominatorCurrCode'),
                        ], sql_source=APXDBvFXRateView)

class APXDBvCustodianInMemoryRepository(InMemorySingletonSQLRepository):
    def __init__(self):
        super().__init__(pk_columns=[PKColumnMapping('CustodianID')], sql_source=APXDBvCustodianView
                            , relevant_columns=['CustodianName'])

    def supplement(self, transaction: Transaction):
        # Combine the CustodianID into the name (APXTxns.pm line 984)
        super().supplement(transaction)
        if transaction.CustodianName:
            transaction.CustodianName = f"{transaction.CustodianName} ({int(transaction.CustodianID)})"


class CoreDBRealizedGainLossInMemoryRepository(InMemorySingletonSQLRepository):
    def __init__(self):
        super().__init__(pk_columns=[
                                    PKColumnMapping('PortfolioTransactionID'), 
                                    PKColumnMapping('TranID'), 
                                    PKColumnMapping('LotNumber'), 
                                ], sql_source=COREDBAPXfRealizedGainLossTable
                            , relevant_columns=['RealizedGainLoss', 'RealizedGainLossLocal', 'CostBasis', 'CostBasisLocal', 'Quantity'])
    
    def supplement(self, transaction: Transaction):
        # Save original quantity (we need to save it back after to avoid it getting overwritten)
        quantity_orig = transaction.Quantity

        # Supplement as normal
        super().supplement(transaction)

        # We need to check if there is a quantity in the supplemental data, and if so, then supplement further:
        supplemental_data = self._get_supplemental_data(transaction)
        print(supplemental_data)
        if isinstance(supplemental_data, dict):
            if supplemental_quantity := supplemental_data.get('Quantity'):
                if transaction.PortfolioTransactionID == 13343050:
                    logging.info(f'{transaction.PortfolioTransactionID} has a quantity from {self.cn}: {supplemental_quantity}')
                    print(f'{transaction.PortfolioTransactionID} has a quantity from {self.cn}: {supplemental_quantity}')
                if hasattr(transaction, 'CostBasis'):
                    if transaction.PortfolioTransactionID == 13343050:
                        logging.info(f'{transaction.PortfolioTransactionID} has a CostBasis {transaction.CostBasis}')
                        print(f'{transaction.PortfolioTransactionID} has a CostBasis {transaction.CostBasis}')
                    transaction.RptCostBasis = transaction.CostBasis
                    transaction.RptCostPerUnit = transaction.RptCostBasis / supplemental_quantity
                else:
                    logging.info(f'{transaction.PortfolioTransactionID} has no CostBasis')
                if hasattr(transaction, 'CostBasisLocal'):
                    transaction.LocalCostBasis = transaction.CostBasisLocal
                    transaction.LocalCostPerUnit = transaction.LocalCostBasis / supplemental_quantity

        # Save back the original quantity 
        transaction.Quantity = quantity_orig



