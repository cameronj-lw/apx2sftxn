
from typing import Any, Dict

from domain.models import Transaction
from domain.repositories import SupplementaryRepository



class TransactionNameRepository(SupplementaryRepository):
    """ Add the transaction name """

    def create(self, data: Dict[str, Any]) -> int:
        raise NotImplementedError(f'{self.cn} does not require or provide a CREATE!')

    def get(self, pk_column_values: Dict[str, Any]) -> dict:
        raise NotImplementedError(f'{self.cn} does not require or provide a GET!')

    # APXTxns.pm::get_transaction_name
    def supplement(self, transaction: Transaction):
        if transaction.TransactionCode == 'dv':
            if transaction.SecTypeBaseCode1 == 'lw':
                # TODO: better way of identifying equity/balanced/FI funds? At least move this to config?
                if transaction.Symbol1 in ('AVBF', 'DPA', 'DPB', 'IAFA', 'IAFB', 'IPFA', 'UDPA', 'UDPB', 'BFA', 'BFB', 'BFF', 'IAFF'):
                    transaction.TransactionName = 'Distribution'
                elif transaction.Symbol1 in ('CFIA', 'TRFA', 'TRFB', 'CPFIA', 'CPFIB', 'FIA', 'FIB', 'LTFA', 'MMF', 'TRLA', 
                                                'CPPA', 'CPPB', 'HYA', 'HYAH', 'HYB', 'HYBH', 'CPBFA', 'CPFIF', 'HYF', 'HYFH', 
                                                'MMA', 'USSMA', 'USSMB', 'USSMF', 'IBHA', 'IBHB', 'MCA', 'MCB', 'MCF', 
                                                'STIFA', 'STIFB', 'STIFF', 'STIFI1', 'UMMA', 'UMMB', 'UMMF', 'TRFI1'):
                    transaction.TransactionName = 'Interest Received'
            else:
                transaction.TransactionName = 'Dividend'

        elif transaction.TransactionCode in ('by', 'bc'):
            transaction.TransactionName = 'Purchase'
        
        elif transaction.TransactionCode in ('sl', 'ss'):
            transaction.TransactionName = 'Sale'
        
        elif transaction.TransactionCode in ('rc'):
            transaction.TransactionName = 'Return Of Capital'
        
        elif transaction.TransactionCode in ('pd'):
            transaction.TransactionName = 'Paydown'
        
        elif transaction.TransactionCode in ('mt'):
            transaction.TransactionName = 'Maturity'
        
        elif transaction.TransactionCode in ('wd', 'lo'):
            transaction.TransactionName = 'Withdrawal'
        
        elif transaction.TransactionCode in ('dp', 'li'):
            transaction.TransactionName = 'Contribution'
        
        elif transaction.TransactionCode in ('in', 'sa'):
            transaction.TransactionName = 'Interest Received'
        
        elif transaction.TransactionCode in ('pa'):
            transaction.TransactionName = 'Interest Paid'
        
        elif transaction.TransactionCode in ('ex', 'ep'):
            transaction.TransactionName = 'Expense'
        
        elif transaction.TransactionCode in ('wt'):
            transaction.TransactionName = 'Withholding Tax'
        
        elif transaction.TransactionCode in ('ac'):
            transaction.TransactionName = 'Cost Adjustment'
        
        else:
            transaction.TransactionName = 'Unknown'  # pass  # TODO_EH: exception?


class TransactionSectionAndStmtTranRepository(SupplementaryRepository):
    """ Add the transaction section and statement descriptions """

    def create(self, data: Dict[str, Any]) -> int:
        raise NotImplementedError(f'{self.cn} does not require or provide a CREATE!')

    def get(self, pk_column_values: Dict[str, Any]) -> dict:
        raise NotImplementedError(f'{self.cn} does not require or provide a GET!')

    # apx2txnrpts.pl line 1414-1421
    def supplement(self, transaction: Transaction):
        if transaction.TransactionCode in ('by'):
            transaction.SectionDesc = 'Buys'
            transaction.StmtTranDesc = 'Buy'
        elif transaction.TransactionCode in ('sl'):
            transaction.SectionDesc = 'Sells'
            transaction.StmtTranDesc = 'Sell'
        elif transaction.TransactionCode in ('pd'):
            transaction.SectionDesc = 'Repayment'
            transaction.StmtTranDesc = 'Repayment'
        elif transaction.TransactionCode in ('mt'):
            transaction.SectionDesc = 'Maturity'
            transaction.StmtTranDesc = 'Maturity'
        elif transaction.TransactionCode in ('dp', 'li'):
            transaction.SectionDesc = 'Deposits'
            transaction.StmtTranDesc = 'Deposit'
        elif transaction.TransactionCode in ('wd', 'lo'):
            transaction.SectionDesc = 'Withdrawals'
            transaction.StmtTranDesc = 'Withdrawal'
        elif transaction.TransactionCode in ('ex', 'ep'):
            transaction.SectionDesc = 'Fees'
            transaction.StmtTranDesc = 'Fee'
        elif transaction.TransactionCode in ('dv'):
            transaction.SectionDesc = 'Dividend'
            transaction.StmtTranDesc = 'Dividend'
        elif transaction.TransactionCode in ('dr'):
            transaction.SectionDesc = 'Dividend Reclaim'
            transaction.StmtTranDesc = 'Dividend Reclaim'
        elif transaction.TransactionCode in ('in'):
            transaction.SectionDesc = 'Interest'
            transaction.StmtTranDesc = 'Interest'
        elif transaction.TransactionCode in ('pa'):
            transaction.SectionDesc = 'Accrued Interest Bought'
            transaction.StmtTranDesc = 'Accrued Interest Bought'
        elif transaction.TransactionCode in ('sa'):
            transaction.SectionDesc = 'Accrued Interest Sold'
            transaction.StmtTranDesc = 'Accrued Interest Sold'
        elif transaction.TransactionCode in ('rc'):
            transaction.SectionDesc = 'Return of Capital'
            transaction.StmtTranDesc = 'Return of Capital'
        elif transaction.TransactionCode in ('ti'):
            transaction.SectionDesc = 'Transfer In'
            transaction.StmtTranDesc = 'Transfer In'
        elif transaction.TransactionCode in ('to'):
            transaction.SectionDesc = 'Transfer Out'
            transaction.StmtTranDesc = 'Transfer Out'
        elif transaction.TransactionCode in ('ss'):
            transaction.SectionDesc = 'Sell Short'
            transaction.StmtTranDesc = 'Sell Short'
        elif transaction.TransactionCode in ('cs'):
            transaction.SectionDesc = 'Cover Short'
            transaction.StmtTranDesc = 'Cover Short'
        elif transaction.TransactionCode in ('si'):
            transaction.SectionDesc = 'Deposit Security (Short)'
            transaction.StmtTranDesc = 'Deposit Security (Short)'
        elif transaction.TransactionCode in ('ac'):
            transaction.SectionDesc = 'Adjust Cost'
            transaction.StmtTranDesc = 'Adjust Cost'
        else:
            pass  # TODO_EH: exception?


class TransactionOtherPostSupplementRepository(SupplementaryRepository):
    """ Do other misc post-processing supplementing """

    def create(self, data: Dict[str, Any]) -> int:
        raise NotImplementedError(f'{self.cn} does not require or provide a CREATE!')

    def get(self, pk_column_values: Dict[str, Any]) -> dict:
        raise NotImplementedError(f'{self.cn} does not require or provide a GET!')

    # apx2txnrpts.pl line 1422-1465
    def supplement(self, transaction: Transaction):
        if transaction.TransactionCode in ('dv'):
            transaction.Quantity = None
            transaction.PricePerUnit = None
            transaction.PricePerUnitLocal = None
            transaction.CostPerUnit = None
            transaction.CostPerUnitLocal = None
            transaction.CostBasis = None
            transaction.CostBasisLocal = None
        elif transaction.TransactionCode in ('lo', 'wd', 'ex', 'ep', 'wt'):
            transaction.TradeAmount = -1.0 * transaction.TradeAmount
            transaction.TradeAmountLocal = -1.0 * transaction.TradeAmountLocal
            # TODO: make "tiny" amounts into None? Saem applies to RealizedGain, Proceeds, Quantity?
            if transaction.TransactionCode in ('wd') and transaction.Symbol1 == 'client':
                transaction.Name4Stmt = 'CASH WITHDRAWAL'
        elif transaction.TransactionCode in ('dp') and transaction.Symbol1 == 'client':
            transaction.Name4Stmt = 'CASH DEPOSIT'


class TransactionCashflowRepository(SupplementaryRepository):
    """ Add the cashflow """

    def create(self, data: Dict[str, Any]) -> int:
        raise NotImplementedError(f'{self.cn} does not require or provide a CREATE!')

    def get(self, pk_column_values: Dict[str, Any]) -> dict:
        raise NotImplementedError(f'{self.cn} does not require or provide a GET!')

    # apx2txnrpts.pl line 1467-1478
    def supplement(self, transaction: Transaction):
        if transaction.TransactionCode in ('by'):
            transaction.CashFlow = -1.0 * transaction.TradeAmount
        else:
            transaction.CashFlow = transaction.TradeAmount
        
