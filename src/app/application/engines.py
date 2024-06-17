
# core python
from abc import ABC, abstractmethod
from dataclasses import dataclass
import datetime
import logging
import math
import numbers
import os
from typing import List, Union


# native
from application.exceptions import TransactionShouldBeAddedException, TransactionShouldBeRemovedException
from domain.models import QueueStatus, Transaction, TransactionProcessingQueueItem
from domain.repositories import SupplementaryRepository, TransactionRepository, TransactionProcessingQueueRepository


@dataclass
class TransactionProcessingEngine(ABC):
    source_queue_repo: TransactionProcessingQueueRepository  # we'll read from this queue to detect new transactions for processing, and update status post-processing
    target_txn_repos: List[TransactionRepository]  # we'll save results here
    target_queue_repos: List[TransactionProcessingQueueRepository]  # we'll save as PENDING queue_status here

    def run(self):
        """ Subclasses may override if this default behaviour is not desired """

        # Get new transactions to process
        items_to_process = self.source_queue_repo.get(queue_status=QueueStatus.PENDING)
        logging.debug(f'{self.source_queue_repo.cn} found {len(items_to_process)} PENDING')

        # Build resulting items to save to target_queue_repos
        for item in items_to_process:
            
            # Update to IN_PROGRESS
            old_queue_status = item.queue_status
            item.queue_status = QueueStatus.IN_PROGRESS
            queue_update_res = self.source_queue_repo.update_queue_status(queue_item=item, old_queue_status=old_queue_status)

            result = self.process(queue_item=item)

            # If result is empty, we still want artificially generate a single "result".
            # This will facilitate deleting of old records for the portfolio & trade date,
            # and inserting of a "blank" record to show that the calculation & storing succeeded, but there were 0 transactions.
            if not len(result):
                result = [Transaction(**{'portfolio_code': item.portfolio_code
                                            , 'trade_date': item.trade_date
                                            , 'trade_date_original': item.trade_date
                                            , 'modified_by': f"{os.environ.get('APP_NAME')}_{str(self)}" 
                                        })]
            
            # Now we have the results. Save them:
            for repo in self.target_txn_repos:
                logging.info(f'Creating transactions in {repo.cn}...')
                create_res = repo.create(transactions=result)
            
            for repo in self.target_queue_repos:
                logging.info(f'Creating queue item in {repo.cn}...')
                create_res = repo.create(queue_item=TransactionProcessingQueueItem(portfolio_code=item.portfolio_code, trade_date=item.trade_date, queue_status=QueueStatus.PENDING))

            # Update to SUCCESS
            old_queue_status = item.queue_status
            item.queue_status = QueueStatus.SUCCESS
            queue_update_res = self.source_queue_repo.update_queue_status(queue_item=item, old_queue_status=old_queue_status)

    @property
    def cn(self):  # Class name. Avoids having to print/log type(self).__name__.
        return type(self).__name__

    def __str__(self):
        return self.cn

    @abstractmethod
    def process(self, queue_item: TransactionProcessingQueueItem) -> List[Transaction]:
        """ Subclasses must implement their own processing logic """


@dataclass
class StraightThruTransactionProcessingEngine(TransactionProcessingEngine):
    """ Straightforward engine, to simply get transactions from the source_repo """
    source_txn_repo: TransactionRepository

    def process(self, queue_item: TransactionProcessingQueueItem) -> List[Transaction]:
        # TODO_20240510: rewrite to process a queue item
        logging.info(f'{self.cn} processing {queue_item}')
        res_transactions = self.source_txn_repo.get(portfolio_code=queue_item.portfolio_code, trade_date=queue_item.trade_date)
        
        # Populate the portfolio_code, modified_by, trade_date
        for txn in res_transactions:
            txn.portfolio_code = queue_item.portfolio_code
            txn.trade_date = queue_item.trade_date
            txn.modified_by = f"{os.environ.get('APP_NAME')}_{str(self)}"

        return res_transactions

    def __str__(self):
        return f'{str(self.source_txn_repo)}-Engine'


@dataclass
class LWTransactionSummaryEngine(TransactionProcessingEngine):
    """ Generate LW Transaction Summary. See http://lwweb/wiki/bin/view/Systems/ApxSmes/LWTransactionCustomization """
    source_txn_repo: TransactionRepository  # We'll initially pull the transactions from here
    dividends_repo: TransactionRepository  # We'll separately pull dividends from here
    preprocessing_supplementary_repos: List[SupplementaryRepository]  # We'll supplement with data from these
    prev_bday_cost_repo: SupplementaryRepository  # To retrieve cost info if needed
    postprocessing_supplementary_repos: List[SupplementaryRepository]  # We'll supplement with data from these
    # transaction_name_repo: SupplementaryRepository  # To get transaction names, at the end
    # historical_transaction_repo: TransactionRepository  # We'll query from this when needed, to find historical transactions

    def preprocessing_supplement(self, transactions: List[Transaction]):
        # Supplement with "pre-processing" supplementary repos, to get additional fields
        for txn in transactions:
            txn.trade_date_original = txn.TradeDate  # Since for dividends, we may change the TradeDate later
            for sr in self.preprocessing_supplementary_repos:
                sr.supplement(txn)

    def assign_fx_rate(self, txn: Transaction):
        # APXTxns.pm line 721-734: assign fx rate
        if txn.TradeDateFX and isinstance(txn.TradeDateFX, numbers.Number) and not math.isnan(txn.TradeDateFX):
            txn.FxRate = txn.TradeDateFX
        elif txn.PrincipalCurrencyISOCode1 == txn.ReportingCurrencyISOCode:
            txn.FxRate = 1.0
        elif txn.TradeAmount and txn.TradeAmountLocal:
            txn.FxRate = txn.TradeAmount / txn.TradeAmountLocal

    def massage_deposits_withwrawals(self, txn: Transaction):

    # 2. Client deposits(withdrawals):
        # Flip symbol if required to indicate source(destination) as client
        # In some cases combine multiple transactions in APX into a single transaction to remove activity in 'wash' securities

        if txn.Symbol2.lower() in ('client'):
            # Assign sec1 fields from sec2:
            for col in ['Symbol', 'SecurityID', 'ProprietarySymbol', 'PrincipalCurrencyCode', 'FullName', 'Name4Stmt', 'Name4Trading']:
                sec2_value = getattr(txn, f'{col}2')
                setattr(txn, f'{col}1', sec2_value)
                
    # 3. Taxes:
        # combine multiple transactions in APX through wash securities into single transactions

        elif txn.Symbol2.lower() in ('whnrtax', 'whfedtax'):
            # Make txn code as wt 
            txn.TransactionCode = 'wt'
            # Assign sec1 fields from sec2:
            for col in ['SecurityID', 'ProprietarySymbol', 'PrincipalCurrencyCode', 'FullName', 'Name4Stmt', 'Name4Trading']:
                sec2_value = getattr(txn, f'{col}2')
                setattr(txn, f'{col}1', sec2_value)
        elif txn.Symbol1.lower() in ('whnrtax', 'whfedtax', 'dvshrt', 'dvwash', 'lw.mfr'):
            raise TransactionShouldBeRemovedException(txn)

    # 4. Fees: Custodian, LW Management, management fee reimbursements
        # combine multple transactions in APX through wash securities into single transactions

        elif txn.Symbol1.lower() in ('manfee', 'manrfee'):
            if txn.SecurityID2 is None:
                raise TransactionShouldBeRemovedException(txn)
            elif txn.TransactionCode in ('dp', 'wd'):
                txn.TransactionCode = 'ep'
        elif txn.Symbol1.lower() in ('cust'):
            if txn.TransactionCode in ('dp', 'wd'):
                txn.TransactionCode = 'ex'
        elif txn.Symbol1 == 'cash' and txn.SecTypeBaseCode2 == 'aw':
            raise TransactionShouldBeRemovedException(txn)
        elif txn.Symbol1 == 'income' and txn.SecurityID2 is None and txn.SecTypeBaseCode2 == 'aw':
            raise TransactionShouldBeRemovedException(txn)
        elif txn.Symbol1 == 'income' and txn.Symbol2 == 'cash' and txn.SecTypeBaseCode2 == 'aw':
            txn.Symbol1 = 'client'
        else:
            raise TransactionShouldBeRemovedException(txn)

    def assign_cost_basis(self, txn: Transaction):
        # APXTxns.pm line 860
        if txn.OriginalCostLocalCurrency:
            txn.LocalCostBasis = txn.OriginalCostLocalCurrency
            txn.LocalCostPerUnit = (txn.OriginalCostLocalCurrency / txn.Quantity if txn.Quantity else 0)
        if txn.OriginalCost:
            txn.RptCostBasis = txn.OriginalCost
            txn.RptCostPerUnit = (txn.OriginalCost / txn.Quantity if txn.Quantity else 0)

    def attribute_distribution(self, txn: Transaction):
        # APXTxns.pm line 1027-1133 are irrelevant, since distribution breakdowns were stopped asof June 30, 2023
        # Therefore we should implement only the "fallback" section in APXTxns.pm line 1134-1146:
        if txn.TransactionCode in ('in', 'sa', 'pa'):
            txn.NetInterest = txn.TradeAmount
            txn.TotalIncome = txn.TradeAmount

            # APXTxns.pm line 1149-1153
            txn.Quantity = 0.0
            txn.UnitPrice = 0.0
            txn.UnitPriceLocal = 0.0

        elif txn.TransactionCode in ('dv'):
            txn.TotalIncome = txn.TradeAmount
            
            # APXTxns.pm line 1149-1153
            txn.Quantity = 0.0
            txn.UnitPrice = 0.0
            txn.UnitPriceLocal = 0.0

            if txn.PrincipalCurrencyISOCode1 == 'CAD': 
                # TODO: what's so special about CAD for this? i.e. what if it's a non-CAD portfolio?
                txn.NetDividend = txn.TradeAmount
                txn.NetEligDividend = txn.TradeAmount
            else:
                txn.NetFgnIncome = txn.TradeAmount

    def assign_contribution_amount(self, txn: Transaction):

    # 13. if transaction is a deposit to an 'RRSP' type portfolio then amount is considered an RSP contribution for reporting purposes
            # EXCEPT if the transaction is pre 14Aug2015 and has a comment with the string 'EXCLUDE'. This is/was a hack to support backwards compatibility when the Private Client team changed some workflows.
    
        if 'RRSP' in txn.PortfolioTypeCode:
            if txn.TransactionCode == 'dp' and txn.Symbol2 == 'client' and txn.Comment01 == 'CONTRIBUTION':
                txn.RspContribAmt = txn.TradeAmount
    
    # 14. if transaction is a withdrawal to an 'RRSP' type portfolio then amount is considered an RSP withdrawal for reporting purposes
            # EXCEPT if the transaction is pre 14Aug2015 and has a comment with the string 'EXCLUDE'. This is/was a hack to support backwards compatibility when the Private Client team changed some workflows.

            elif txn.TransactionCode == 'wd' and txn.Symbol1 == 'client' and txn.Comment01 == 'CONTRIBUTION':
                txn.RspContribAmt = txn.TradeAmount

    # 15. if transaction is a deposit to an 'TFSA' type portfolio then amount is considered an TFSA contribution for reporting purposes
            # EXCEPT if the transaction is pre 14Aug2015 and has a comment with the string 'EXCLUDE'. This is/was a hack to support backwards compatibility when the Private Client team changed some workflows.

        if 'TFSA' in txn.PortfolioTypeCode:
            if txn.TransactionCode == 'dp' and txn.Symbol2 == 'client' and txn.Comment01 == 'CONTRIBUTION':
                txn.TfsaContribAmt = txn.TradeAmount

    # 16. if transaction is a withdrawal to an 'TFSA' type portfolio then amount is considered an TFSA withdrawal for reporting purposes
            # EXCEPT if the transaction is pre 14Aug2015 and has a comment with the string 'EXCLUDE'. This is/was a hack to support backwards compatibility when the Private Client team changed some workflows.

            elif txn.TransactionCode == 'wd' and txn.Symbol1 == 'client' and txn.Comment01 == 'CONTRIBUTION':
                txn.TfsaContribAmt = txn.TradeAmount   

    def add_fields(self, txn: Transaction):
    # APXTxns.pm line 1259-1317: Add fields (just putting here to replicate ordering in APXTxns.pm)

        txn.PortfolioName = txn.ReportHeading1
        txn.AsOfDate = txn.TradeDate
        txn.SecurityID = txn.SecurityID1
        txn.LWID = txn.ProprietarySymbol1
        txn.Symbol = txn.Symbol1
        # TODO: do we need OrderNo?
        txn.PricePerUnit = txn.UnitPrice
        txn.PricePerUnitLocal = txn.UnitPriceLocal
        if not hasattr(txn, 'FxRate'):
            txn.FxRate = txn.TradeDateFX
        if hasattr(txn, 'ISOCode'):  # TODO: will need to populate ISOCode, even for non-FX txns
            txn.TradeCcy = txn.ISOCode
            txn.SecCcy = txn.ISOCode
        txn.RptCcy = txn.ReportingCurrencyCode
        # TODO: Need IncomeCcy? Convert PrincipalCurrencyCode1 to ISO?
        if hasattr(txn, 'RptCostBasis'):
            txn.CostBasis = txn.RptCostBasis 
        if hasattr(txn, 'RptCostPerUnit'):
            txn.CostPerUnit = txn.RptCostPerUnit 
        if hasattr(txn, 'LocalCostBasis'):
            txn.CostBasisLocal = txn.LocalCostBasis 
        if hasattr(txn, 'LocalCostPerUnit'):
            txn.CostPerUnitLocal = txn.LocalCostPerUnit 
        if hasattr(txn, 'RealizedGainLoss'):
            txn.RealizedGain = txn.RealizedGainLoss 
        txn.BrokerName = txn.BrokerFirmName
        txn.BrokerID = txn.BrokerFirmSymbol
        if not hasattr(txn, 'LocalTranKeySuffix'):
            txn.LocalTranKeySuffix = '_A'
        txn.LocalTranKey = f"{txn.PortfolioCode}_{txn.TradeDate.strftime('%Y%m%d')}_{txn.SettleDate.strftime('%Y%m%d')}_{txn.Symbol}_{txn.PortfolioTransactionID}_{txn.TranID}_{txn.LotNumber}{txn.LocalTranKeySuffix}"
        txn.SecTypeCode1 = f'{txn.SecTypeBaseCode1}{txn.PrincipalCurrencyCode1}'
        txn.SecTypeCode2 = f'{txn.SecTypeBaseCode2}{txn.PrincipalCurrencyCode2}'
        if hasattr(txn, 'FedTaxWithheld'):
            txn.WhFedTaxAmt = txn.FedTaxWithheld 
        if hasattr(txn, 'FgnTaxPaid'):
            txn.WhNrTaxAmt = txn.FgnTaxPaid 
        
    def massage_fi_maturities(self, txn: Transaction):
        if txn.TransactionCode == 'sl':
            if txn.SecTypeBaseCode1 == 'st':
                # APXTxns.pm line 1321-1373 sells of STs: use the prev day appraisal 
                if hasattr(txn, 'RptCostBasis'):
                    # APXTxns.pm line 1329: Create the new "interest" transaction
                    new_txn = Transaction(**(txn.__dict__))

                    # APXTxns.pm line 1332-1359: update new txn
                    income_local = new_txn.TradeAmountLocal - new_txn.LocalCostBasis  # TODO_EH: what if there's no LocalCostBasis?
                    fx_rate = 1.0
                    if txn.TradeAmountLocal:
                        fx_rate = txn.TradeAmount / txn.TradeAmountLocal
                    income = fx_rate * income_local
                    new_txn.TradeDateFX = fx_rate
                    # TODO: do we need OrderNo?
                    new_txn.TransactionCode = 'in'
                    new_txn.TradeAmount = income
                    new_txn.TradeAmountLocal = income_local
                    new_txn.RealizedGain = 0.0
                    new_txn.Commission = 0.0
                    for attr in ['PricePerUnit', 'PricePerUnitLocal', 'CostPerUnit', 'CostPerUnitLocal', 'Quantity']:
                        if hasattr(new_txn, attr):
                            delattr(new_txn, attr)

                    new_txn.NetInterest = income
                    new_txn.NetDividend = 0.0
                    new_txn.NetEligDividend = 0.0
                    new_txn.NetNonEligDividend = 0.0
                    new_txn.NetFgnIncome = 0.0
                    new_txn.CapGainsDistrib = 0.0
                    new_txn.TotalIncome = income
                    new_txn.LocalTranKeySuffix = '_A_B'
                    new_txn.LocalTranKey = f"{new_txn.PortfolioCode}_{new_txn.TradeDate.strftime('%Y%m%d')}_{new_txn.SettleDate.strftime('%Y%m%d')}_{new_txn.Symbol1}_{new_txn.PortfolioTransactionID}_{new_txn.TranID}_{new_txn.LotNumber}{new_txn.LocalTranKeySuffix}"

                    # APXTxns.pm line 1362-1372: clean up the parent txn for maturities
                    txn.RealizedGain = txn.TradeAmount - txn.RptCostBasis - income
                    if txn.TradeDate >= txn.MaturityDate1:
                        txn.PricePerUnit = 100.0
                        txn.TradeAmount = txn.RptCostBasis
                        txn.TransactionCode = 'mt'
                        txn.RealizedGain = 0.0
                    else:
                        txn.TradeAmount = txn.TradeAmount - income
                        txn.TradeAmountLocal = txn.TradeAmount - income_local

                    raise TransactionShouldBeAddedException(new_txn)
                else:
                    pass  # TODO_EH: exception? 
            else:
                if txn.MaturityDate1:
                    if txn.TradeDate >= txn.MaturityDate1:
                        txn.TransactionCode = 'mt'  # maturity

    def massage_names_for_cash(self, txn: Transaction):
        # if the APX transaction is a long-out of a holding in a cash security then change it to a 'Cash Transfer Withdrawal'

        # APXTxns.pm line 1389-1396
        if txn.TransactionCode == 'lo' and txn.Symbol1 == 'cash':
            if not txn.Name4Stmt: 
                txn.Name4Stmt = 'Cash Transfer Withdrawal'
            if not txn.Name4Trading: 
                txn.Name4Trading = 'Cash Transfer Withdrawal'

        # if the APX transaction is a long-in of a holding in a cash security then change it to a 'Cash Transfer Deposit'
        
        # APXTxns.pm line 1381-1388
        if txn.TransactionCode == 'li' and txn.Symbol1 == 'cash':
            if not txn.Name4Stmt: 
                txn.Name4Stmt = 'Cash Transfer Deposit'
            if not txn.Name4Trading: 
                txn.Name4Trading = 'Cash Transfer Deposit'

        # if the APX transaction is an interest payment of cash then change it to 'Interest Received'

        # APXTxns.pm line 1381-1388
        if txn.TransactionCode == 'in' and txn.Symbol1 == 'cash':
            if not txn.Name4Stmt1: 
                txn.Name4Stmt1 = 'Interest Received'
            if not txn.Name4Trading1:
                txn.Name4Trading1 = 'Interest Received'

    def remove_price_and_quantity(self, txn: Transaction):  
        # APXTxns.pm line 1485-1491
        #           
        # 102a. remove (blank) price per unit in local and reporting currency for a wide range of cash, fee, income, etc. types of transactions (dp, wd, ex, ep, wt, pa, sa, in, pd, rc)
        if hasattr(txn, 'PricePerUnit'):
            delattr(txn, 'PricePerUnit')
        if hasattr(txn, 'PricePerUnitLocal'):
            delattr(txn, 'PricePerUnitLocal')

        # 102b. remove (blank) quantity for a wide range of cash, fee, income, etc. types of transactions (dp, wd, ex, ep, wt, pa, sa, in, pd, rc)
        if hasattr(txn, 'Quantity'):
            delattr(txn, 'Quantity')

    def zero_registered_contributions(self, txn: Transaction):
        # 102c. hack to verify that there are no withdrawals from RSP/TFSA types of account
        # APXTxns.pm line 1505-1511
        if 'TFSA' in txn.PortfolioTypeCode:
            txn.TfsaContribAmt = 0.0
        if 'RRSP' in txn.PortfolioTypeCode:
            txn.RspContribAmt = 0.0

    def assign_sec_columns(self, txn: Transaction):
        # Populate sec columns from Security1
        for col in ['FullName', 'Name4Stmt', 'Name4Trading']:
            if hasattr(txn, f'{col}1'):
                setattr(txn, col, getattr(txn, f'{col}1'))

    def assign_transaction_name(self, txn: Transaction):
        # APXTxns.pm::get_transaction_name
        if txn.TransactionCode == 'dv':
            if txn.SecTypeBaseCode1 == 'lw':
                # TODO: better way of identifying equity/balanced/FI funds? At least move this to config?
                if txn.Symbol1 in ('AVBF', 'DPA', 'DPB', 'IAFA', 'IAFB', 'IPFA', 'UDPA', 'UDPB', 'BFA', 'BFB', 'BFF', 'IAFF'):
                    txn.TransactionName = 'Distribution'
                elif txn.Symbol1 in ('CFIA', 'TRFA', 'TRFB', 'CPFIA', 'CPFIB', 'FIA', 'FIB', 'LTFA', 'MMF', 'TRLA', 
                                                'CPPA', 'CPPB', 'HYA', 'HYAH', 'HYB', 'HYBH', 'CPBFA', 'CPFIF', 'HYF', 'HYFH', 
                                                'MMA', 'USSMA', 'USSMB', 'USSMF', 'IBHA', 'IBHB', 'MCA', 'MCB', 'MCF', 
                                                'STIFA', 'STIFB', 'STIFF', 'STIFI1', 'UMMA', 'UMMB', 'UMMF', 'TRFI1'):
                    txn.TransactionName = 'Interest Received'
                else:
                    txn.TransactionName = 'Dividend'
            else:
                txn.TransactionName = 'Dividend'

        elif txn.TransactionCode in ('by', 'bc'):
            txn.TransactionName = 'Purchase'
        
        elif txn.TransactionCode in ('sl', 'ss'):
            txn.TransactionName = 'Sale'
        
        elif txn.TransactionCode in ('rc'):
            txn.TransactionName = 'Return Of Capital'
        
        elif txn.TransactionCode in ('pd'):
            txn.TransactionName = 'Paydown'
        
        elif txn.TransactionCode in ('mt'):
            txn.TransactionName = 'Maturity'
        
        elif txn.TransactionCode in ('wd', 'lo'):
            txn.TransactionName = 'Withdrawal'
        
        elif txn.TransactionCode in ('dp', 'li'):
            txn.TransactionName = 'Contribution'
        
        elif txn.TransactionCode in ('in', 'sa'):
            txn.TransactionName = 'Interest Received'
        
        elif txn.TransactionCode in ('pa'):
            txn.TransactionName = 'Interest Paid'
        
        elif txn.TransactionCode in ('ex', 'ep'):
            txn.TransactionName = 'Expense'
        
        elif txn.TransactionCode in ('wt'):
            txn.TransactionName = 'Withholding Tax'
        
        elif txn.TransactionCode in ('ac'):
            txn.TransactionName = 'Cost Adjustment'
        
        else:
            txn.TransactionName = 'Unknown'  # pass  # TODO_EH: exception?

    def assign_section_and_stmt_tran(self, txn: Transaction):
        # apx2txnrpts.pl line 1414-1421
        if txn.TransactionCode in ('by'):
            txn.SectionDesc = 'Buys'
            txn.StmtTranDesc = 'Buy'
        elif txn.TransactionCode in ('sl'):
            txn.SectionDesc = 'Sells'
            txn.StmtTranDesc = 'Sell'
        elif txn.TransactionCode in ('pd'):
            txn.SectionDesc = 'Repayment'
            txn.StmtTranDesc = 'Repayment'
        elif txn.TransactionCode in ('mt'):
            txn.SectionDesc = 'Maturity'
            txn.StmtTranDesc = 'Maturity'
        elif txn.TransactionCode in ('dp', 'li'):
            txn.SectionDesc = 'Deposits'
            txn.StmtTranDesc = 'Deposit'
        elif txn.TransactionCode in ('wd', 'lo'):
            txn.SectionDesc = 'Withdrawals'
            txn.StmtTranDesc = 'Withdrawal'
        elif txn.TransactionCode in ('ex', 'ep'):
            txn.SectionDesc = 'Fees'
            txn.StmtTranDesc = 'Fee'
        elif txn.TransactionCode in ('dv'):
            txn.SectionDesc = 'Dividend'
            txn.StmtTranDesc = 'Dividend'
        elif txn.TransactionCode in ('dr'):
            txn.SectionDesc = 'Dividend Reclaim'
            txn.StmtTranDesc = 'Dividend Reclaim'
        elif txn.TransactionCode in ('in'):
            txn.SectionDesc = 'Interest'
            txn.StmtTranDesc = 'Interest'
        elif txn.TransactionCode in ('pa'):
            txn.SectionDesc = 'Accrued Interest Bought'
            txn.StmtTranDesc = 'Accrued Interest Bought'
        elif txn.TransactionCode in ('sa'):
            txn.SectionDesc = 'Accrued Interest Sold'
            txn.StmtTranDesc = 'Accrued Interest Sold'
        elif txn.TransactionCode in ('rc'):
            txn.SectionDesc = 'Return of Capital'
            txn.StmtTranDesc = 'Return of Capital'
        elif txn.TransactionCode in ('ti'):
            txn.SectionDesc = 'Transfer In'
            txn.StmtTranDesc = 'Transfer In'
        elif txn.TransactionCode in ('to'):
            txn.SectionDesc = 'Transfer Out'
            txn.StmtTranDesc = 'Transfer Out'
        elif txn.TransactionCode in ('ss'):
            txn.SectionDesc = 'Sell Short'
            txn.StmtTranDesc = 'Sell Short'
        elif txn.TransactionCode in ('cs'):
            txn.SectionDesc = 'Cover Short'
            txn.StmtTranDesc = 'Cover Short'
        elif txn.TransactionCode in ('si'):
            txn.SectionDesc = 'Deposit Security (Short)'
            txn.StmtTranDesc = 'Deposit Security (Short)'
        elif txn.TransactionCode in ('ac'):
            txn.SectionDesc = 'Adjust Cost'
            txn.StmtTranDesc = 'Adjust Cost'
        else:
            pass  # TODO_EH: exception?

    def null_fields_for_dv(self, txn: Transaction):
        # apx2txnrpts.pl line 1423-1426
        if txn.TransactionCode in ('dv'):
            txn.Quantity = None
            txn.PricePerUnit = None
            txn.CostPerUnit = None
            txn.CostPerUnitLocal = None
            txn.CostBasis = None
            txn.CostBasisLocal = None

    def reverse_amount_signs(self, txn: Transaction):
        # apx2txnrpts.pl line 1427-1435 + 1449-1458
        if txn.TransactionCode in ('lo', 'wd', 'ex', 'ep', 'wt'):
            txn.TradeAmount = -1.0 * txn.TradeAmount
            txn.TradeAmountLocal = -1.0 * txn.TradeAmountLocal

    def assign_name4stmt_for_client_wd_dp(self, txn: Transaction):
        # apx2txnrpts.pl line 1436-1448
        if txn.Symbol1 == 'client':
            if txn.TransactionCode == 'wd':
                txn.Name4Stmt = 'CASH WITHDRAWAL'
            elif txn.TransactionCode == 'dp':
                txn.Name4Stmt = 'CASH DEPOSIT'

    def unassign_gains_proceeds_quantity_if_zero(self, txn: Transaction):
        # apx2txnrpts.pl line 1459-1464
        for attr in ('RealizedGain', 'Proceeds', 'Quantity'):
            if not getattr(txn, attr, None):
                setattr(txn, attr, None)  
                # TODO: Using setattr rather than delattr to make it traceable... 
                # but perhaps delattr is more readable? And aligned better to the perl equivalent?

    def assign_cash_flow(self, txn: Transaction):
        # apx2txnrpts.pl line 1467-1478
        if txn.TransactionCode in ('by'):
            txn.CashFlow = -1.0 * txn.TradeAmount
        else:
            txn.CashFlow = txn.TradeAmount

    def assign_standard_attributes(self, txn: Transaction, queue_item: TransactionProcessingQueueItem):
        # Populate the portfolio_code, modified_by, trade_date
        txn.portfolio_code = queue_item.portfolio_code
        txn.trade_date_original = queue_item.trade_date
        txn.modified_by = f"{os.environ.get('APP_NAME')}_{str(self)}"


    def process(self, queue_item: TransactionProcessingQueueItem) -> List[Transaction]:
        logging.info(f'{self.cn} processing {queue_item}')
        
        source_transactions = self.source_txn_repo.get(portfolio_code=queue_item.portfolio_code, trade_date=queue_item.trade_date)
        transactions = source_transactions.copy()
        logging.info(f'{self.cn} found {len(transactions)} transactions from {self.source_txn_repo.cn}')

        source_dividends = self.dividends_repo.get(portfolio_code=queue_item.portfolio_code, trade_date=queue_item.trade_date)
        dividends = source_dividends.copy()
        logging.info(f'{self.cn} found {len(dividends)} dividends from {self.dividends_repo.cn}')

        # Remove dividends from generic transactions, then append dividends
        transactions = [t for t in transactions if t.TransactionCode != 'dv']
        transactions.extend(dividends)

        # First, supplement with "pre-processing" supplementary repos, to get additional fields
        self.preprocessing_supplement(transactions)
        
        # Track indices of items to be removed
        indices_to_remove = []
        # Track new txns separately. Reason is if we append them as we go, they may get re-processed, which we don't want.
        new_txns = []

        for i, txn in enumerate(transactions):

            try:
                # 0a. Assign FX rate
                self.assign_fx_rate(txn)

                # 0b. Pull prev bday cost info, if needed
                if (txn.SecTypeBaseCode1 == 'st' and txn.TransactionCode == 'sl') or txn.TransactionCode == 'lo':
                    # This should cover part of APXTxns.pm line 850-859
                    self.prev_bday_cost_repo.supplement(txn)
            
            # TODO_CLEANUP: remove this block once not needed, since dividends will be provided by the dividends_repo
            # 1. Dividends: combine up to 3 transactions in APX into a single transaction:
                    # If the dividend settles (i.e. 'pay date') in the date range requested then find the associated 'dv' and merge it into the transaction.
                    # If the dividend paid in a foreign currency then find the associated FX trade and merge it into the transaction.
            # for txn in transactions:
            #     if (txn.TransactionCode == 'dv'):
            #         pass
                    # APXTxns.pm line 344-524
                    # txn.TradeDate = txn.SettleDate
                    # 1a. find the matching dp/wd/sl
                    

                # Remove dividends, since they will be provided by the dividends_repo
                # if txn.TransactionCode == 'dv':
                #     indices_to_remove.append(i)
                #     continue

                # APXTxns.pm line 759-827
                if txn.TransactionCode in ('dp', 'wd'):
                    self.massage_deposits_withwrawals(txn)
                    
            # 5. Ignore/erase gains on dividends in foreign currency relative to reporting currency
                    # Background: LW configures APX to do gains/losses based on average cost. This creates realized gains/losses on holdings in foreign dividend accruals when they settle and are exchanged into the portfolio's 'Base Currency'. Client facing teams do not want to see these values.
            
                # APXTxns.pm line 830
                if txn.Symbol1 == 'cash' and txn.Symbol2 == 'cash' and txn.TransactionCode in ('sl', 'by'):
                    raise TransactionShouldBeRemovedException(txn)

                # APXTxns.pm line 850-859
                # TODO_CLEANUP: remove when not needed - this is now provided by LWDBAPXAppraisalPrevBdayRepository supplement method
                # if txn.TransactionCode == 'lo':
                #     if txn.LocalCostPerUnit:
                #         txn.LocalCostBasis = txn.LocalCostPerUnit * txn.Quantity
                #     if txn.RptlCostPerUnit:
                #         txn.LocalCostBasis = txn.RptCostPerUnit * txn.Quantity

                
                if txn.TransactionCode == 'li':
                    self.assign_cost_basis(txn)

                # APXTxns.pm line 873-890
                # TODO_CLEANUP: remove when not needed - this is now provided by LWDBAPXAppraisalPrevBdayRepository supplement method
                # if txn.SecTypeCode1 == 'st' and txn.TransactionCode == 'sl':
                #     if txn.LocalCostPerUnit:
                #         txn.LocalCostBasis = txn.LocalCostPerUnit * txn.Quantity
                #     if txn.RptlCostPerUnit:
                #         txn.LocalCostBasis = txn.RptCostPerUnit * txn.Quantity



            # 6. Massage sales of ST securities to recover cost and treat gains as accrued interest (using cost information from above)
            
                # N/A (this will have already been done via supplementing the txn with supplementary repo(s))
            
            # 7. Paydowns and Interest on MBS/CMBS holdings:
                    # combine multiple transactions in APX through wash 'delaypr' and 'delaynin' securities into single transactions

                # Not sure where in APXTxns.pm this described logic is ... but 893 is a guess due to ordering of documentation notes ...

                # APXTxns.pm line 893: vm/cm switch settle date from weekend to weekday??? 
                if txn.SecTypeBaseCode1 in ('cm', 'vm') and txn.TransactionCode in ('pd', 'in') and txn.TradeDate == txn.SettleDate:
                    if txn.CouponDelayDays1 > 0 and not getattr(txn, 'UseSecTypeForCouponDelayDays1', False):
                        new_settle_date = txn.TradeDate + datetime.timedelta(days=txn.CouponDelayDays1)
                        txn.SettleDate = new_settle_date

            # 8. Ignore dividend reclaims

            # Combined with #9 below

            # 9. Ignore transactions to erase (e.g. write off) divdidend amounts

                # APXTxns.pm line 957: ignore small st sell-all
                if txn.SecTypeBaseCode1 == 'st' and txn.TransactionCode == 'sa' and abs(txn.TradeAmount) < 0.000001:
                    raise TransactionShouldBeRemovedException(txn)

                # APXTxns.pm line 963
                if txn.TransactionCode in ('dr', 'dv') and txn.SecTypeBaseCode2 == 'aw' and txn.SecurityID2 is None:
                    raise TransactionShouldBeRemovedException(txn)

            # 10. Add supplementary information regarding broker, custodian, portfolio type, portfolio name, portfolio report heading

                # N/A (this will have already been done via supplementing the txn with supplementary repo(s))

            # 11. If it is an income transaction on an LW fund holding then look for distribution breakdown information from UMP {
                    # fuzzy match on distribution month-end (include link to explanation about distribution report not having a trade date)
                    # fuzzy logic on gains for transactions washing through 'dvshrt'
                    # If distribution breakdown information is found from UMP then {
                        # use the UMP information on a percentage basis
                        # scale the amount from APX by the percentages and add attribution information to the transaction
                        # NOTE: there is a patch around a shortcoming from UMP for MMF and management fee rebates
                    # } else {
                        # attribute the amount from APX based on transaction type (interest vs dividend) and currency (local vs foreign)
                    # }
            # }
                self.attribute_distribution(txn)

            # 12. if transaction is a buy funded through LW.MFR security then add comment 'Management Fee Rebate'
            
                # APXTxns.pm line 1164
                if txn.TransactionCode == 'by' and txn.Symbol1 == 'LW.MFR' and (not txn.Comment01):
                    txn.Comment01 = 'Management Fee Rebate'

            # 13-16. assign RSP/TFSA contribution amounts
                self.assign_contribution_amount(txn)
            
            # Add fields (just putting here to replicate ordering in APXTxns.pm)
                self.add_fields(txn)

            # 17. if transaction is a sale of a fixed income security on maturity date then rename it a 'Maturity' transaction
                self.massage_fi_maturities(txn)

            # 18. Massage the security name for some cash transactions:
                self.massage_names_for_cash(txn)

            except TransactionShouldBeAddedException as e:
                new_txns.append(e.txn)
            except TransactionShouldBeRemovedException as e:
                indices_to_remove.append(i)

        # 100a. Remove transactions which were identified to remove
        for i in reversed(indices_to_remove):
            del transactions[i]

        # 100b. Append new transactions
        transactions.extend(new_txns)

        # 101. loop through resulting transactions to consolidate contributions/withdrawals by Trade Date and Portfolio.
                # Determine if the result is a net withdrawal or contribution and label it appropriately.

        # APXTxns.pm line 1410 and APXTxns.pm::build_txn_grouping_for_report
        # TODO: figure out whether implementing this is needed...

        # 102. Final "cleanups":
        # Use a separate for loop than above, because we do want new txns to be included
        for txn in transactions:            
            
            if txn.TransactionCode in ('dp', 'wd', 'ex', 'ep', 'wt', 'pa', 'sa', 'in', 'pd', 'rc'):
                self.remove_price_and_quantity(txn)

            if txn.TransactionCode == 'wd':
                self.zero_registered_contributions(txn)
                
            self.assign_sec_columns(txn)

            self.assign_transaction_name(txn)

            self.assign_section_and_stmt_tran(txn)

            self.null_fields_for_dv(txn)

            self.reverse_amount_signs(txn)

            self.assign_name4stmt_for_client_wd_dp(txn)

            self.unassign_gains_proceeds_quantity_if_zero(txn)

            self.assign_cash_flow(txn)

            self.assign_standard_attributes(txn, queue_item)

        return transactions

    def __str__(self):
        return f'LW-Transaction-Summary-Engine'





