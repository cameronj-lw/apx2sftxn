
import argparse
import datetime
import logging
import os
import sys


# Append to pythonpath
src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(src_dir)


# from application.engines import StraightThruTransactionProcessingEngine, LWTransactionSummaryEngine
from domain.models import TransactionProcessingQueueItem
from domain.python_tools import get_current_callable

# native
from application.engines import StraightThruTransactionProcessingEngine, LWTransactionSummaryEngine, LWAPX2SFTransactionEngine
from application.event_handlers import TransactionEventHandler
from application.repositories import (
    TransactionNameRepository, TransactionSectionAndStmtTranRepository, 
    TransactionCashflowRepository, TransactionOtherPostSupplementRepository,
)
from infrastructure.message_subscribers import KafkaAPXTransactionMessageConsumer
from infrastructure.in_memory_repositories import (
    APXDBvSecurityInMemoryRepository, APXRepDBSecurityHashInMemoryRepository,
    APXDBvPortfolioInMemoryRepository, APXDBvPortfolioSettingExInMemoryRepository, 
    APXDBvPortfolioBaseInMemoryRepository, APXDBvPortfolioBaseCustomInMemoryRepository, APXDBvPortfolioBaseSettingExInMemoryRepository,
    APXDBvCurrencyInMemoryRepository, APXDBvCustodianInMemoryRepository,
    APXDBvFXRateInMemoryRepository,
    APXRepDBvPortfolioAndStmtGroupCurrencyInMemoryRepository, CoreDBSFPortfolioLatestInMemoryRepository,
)
from infrastructure.sql_repositories import (
    MGMTDBHeartbeatRepository, 
    CoreDBRealizedGainLossQueueRepository, CoreDBRealizedGainLossTransactionRepository, APXDBRealizedGainLossRepository,
    CoreDBTransactionActivityQueueRepository, CoreDBTransactionActivityRepository, APXDBTransactionActivityRepository,    
    CoreDBLWTransactionSummaryQueueRepository, CoreDBLWTransactionSummaryRepository, LWDBAPXAppraisalPrevBdayRepository,
    CoreDBRealizedGainLossSupplementaryRepository,
    APXRepDBLWTxnSummaryRepository, COREDBLWTxnSummaryRepository,
    APXDBDividendRepository,
    COREDBAPX2SFTxnQueueRepository, COREDBSFTransactionRepository
)
from infrastructure.util.config import AppConfig
from infrastructure.util.logging import setup_logging


from infrastructure.sql_tables import (
    COREDBLWTxnSummaryTable, APXRepDBLWTxnSummaryTable,
    COREDBSFTransactionTable, LWDBSFTransactionTable,
)
from infrastructure.util.dataframe import compare_dataframes



def main():
    parser = argparse.ArgumentParser(description='Generate & save txn data')
    parser.add_argument('--log_level', '-l', type=str.upper, choices=['DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL'], help='Log level')
    parser.add_argument('--from_date', '-fd', type=lambda d: datetime.datetime.strptime(d, '%Y-%m-%d').date())
    parser.add_argument('--to_date', '-td', type=lambda d: datetime.datetime.strptime(d, '%Y-%m-%d').date())
    parser.add_argument('--portfolio_code', '-pc', nargs='+', default=[])
    parser.add_argument('--gen_apx_realized_gain_loss', '-gargl', action='store_true', default=False)
    parser.add_argument('--gen_apx_txn_activity', '-gata', action='store_true', default=False)

    args = parser.parse_args()

    base_dir = AppConfig().get("logging", "base_dir")

    if args.gen_apx_realized_gain_loss:
        os.environ['APP_NAME'] = AppConfig().get("app_name", "apx2sftxn_populate_gain_loss")
        setup_logging(base_dir=base_dir, log_level_override=args.log_level)

        portfolios_supplement = APXDBvPortfolioInMemoryRepository()
        portfolios_supplement.relevant_columns.append('PortfolioCode')

        source_txn_repo = APXDBRealizedGainLossRepository()
        target_txn_repo = CoreDBRealizedGainLossTransactionRepository()

        logging.info(f'Reading from {source_txn_repo.cn}...')
        res_transactions = source_txn_repo.get(portfolio_code=args.portfolio_code[0], trade_date=(args.from_date, args.to_date))
        
        # Populate the portfolio_code, modified_by, trade_date, lineage
        for txn in res_transactions:
            portfolios_supplement.supplement(txn)
            txn.portfolio_code = txn.PortfolioCode  # None  # TODO: need to populate this?  # queue_item.portfolio_code
            txn.trade_date = txn.CloseDate
            txn.modified_by = f"{os.environ.get('APP_NAME')}_gen_apx_procs"
            txn.add_lineage(f"{str(source_txn_repo)}"
                                , source_callable=get_current_callable())

        # Save to target repo
        logging.info(f'Saving {len(res_transactions)} to {target_txn_repo.cn}')
        target_txn_repo.create(transactions=res_transactions)

        logging.info(f'Saved {len(res_transactions)} to {target_txn_repo.cn}')

    if args.gen_apx_txn_activity:
        os.environ['APP_NAME'] = AppConfig().get("app_name", "apx2sftxn_populate_txn_activity")
        setup_logging(base_dir=base_dir, log_level_override=args.log_level)

        portfolios_supplement = APXDBvPortfolioInMemoryRepository()
        portfolios_supplement.relevant_columns.append('PortfolioCode')

        source_txn_repo = APXDBTransactionActivityRepository()
        target_txn_repo = CoreDBTransactionActivityRepository()

        logging.info(f'{datetime.datetime.now()}: Reading from {source_txn_repo.cn}...')
        # res_transactions = source_txn_repo.get(portfolio_code=args.portfolio_code[0], trade_date=(args.from_date, args.to_date))
        res_transactions = source_txn_repo.get_raw(portfolio_code=args.portfolio_code[0], trade_date=(args.from_date, args.to_date))
        
        # Populate the portfolio_code, modified_by, trade_date, lineage
        for txn in res_transactions:
            portfolios_supplement.supplement(txn)
            txn.portfolio_code = txn.PortfolioCode  # TODO: need to populate this?  # queue_item.portfolio_code
            txn.trade_date = txn.TradeDate
            txn.modified_by = f"{os.environ.get('APP_NAME')}"
            txn.add_lineage(f"{str(source_txn_repo)}"
                                , source_callable=get_current_callable())

        # Save to target repo
        logging.info(f'Saving {len(res_transactions)} to {target_txn_repo.cn}')
        target_txn_repo.create(transactions=res_transactions)

        logging.info(f'Saved {len(res_transactions)} to {target_txn_repo.cn}')

if __name__ == "__main__":
    main()


