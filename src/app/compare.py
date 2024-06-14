
import argparse
import datetime
import os
import sys


# Append to pythonpath
src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(src_dir)


# from application.engines import StraightThruTransactionProcessingEngine, LWTransactionSummaryEngine
from domain.models import TransactionProcessingQueueItem

# native
from application.engines import StraightThruTransactionProcessingEngine, LWTransactionSummaryEngine
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
)
from infrastructure.sql_repositories import (
    MGMTDBHeartbeatRepository, 
    CoreDBRealizedGainLossQueueRepository, CoreDBRealizedGainLossTransactionRepository, APXDBRealizedGainLossRepository,
    CoreDBTransactionActivityQueueRepository, CoreDBTransactionActivityRepository, APXDBTransactionActivityRepository,    
    CoreDBLWTransactionSummaryQueueRepository, CoreDBLWTransactionSummaryRepository, LWDBAPXAppraisalPrevBdayRepository,
    CoreDBRealizedGainLossSupplementaryRepository,
    APXRepDBLWTxnSummaryRepository, COREDBLWTxnSummaryRepository,
    APXDBDividendRepository,
)
from infrastructure.util.config import AppConfig
from infrastructure.util.logging import setup_logging


from infrastructure.sql_tables import COREDBLWTxnSummaryTable, APXRepDBLWTxnSummaryTable
from infrastructure.util.dataframe import compare_dataframes



def main():
    parser = argparse.ArgumentParser(description='Compare old txn summary results vs new engine results')
    parser.add_argument('--from_date', '-fd', type=lambda d: datetime.datetime.strptime(d, '%Y-%m-%d').date())
    parser.add_argument('--to_date', '-td', type=lambda d: datetime.datetime.strptime(d, '%Y-%m-%d').date())
    parser.add_argument('--portfolio_code', '-pc', nargs='+', default=[])
    parser.add_argument('--run', '-run', action='store_true', default=[])
    
    args = parser.parse_args()

    # df1 = COREDBLWTxnSummaryTable().read(portfolio_code=args.portfolio_code, from_date=args.data_date)
    # if args.data_date == datetime.date(2024, 4, 8):
    #     df2 = APXRepDBLWTxnSummaryTable().read(scenario='BASE', data_handle='8328CB5AC1CB40E083F0DA4EE0DAC2BD', portfolio_code=args.portfolio_code, from_date=args.data_date)
    # else:
    #     df2 = APXRepDBLWTxnSummaryTable().read(scenario='BASE', data_handle='37020804B005458B874D74434DBCD0A0', portfolio_code=args.portfolio_code, from_date=args.data_date)

    # Define columns for matching and exclusion
    match_columns = ['portfolio_code', 'trade_date', 'name4stmt', 'quantity']
    match_columns = ['local_tran_key']
    exclude_columns = ['record_id', 'scenario', 'data_handle', 'asofdate', 'asofuser', 'scenariodate', 'computer', 'trade_date_original', 'fx_rate']
    
    if not args.portfolio_code:
        df1 = COREDBLWTxnSummaryTable().read(from_date=args.from_date, to_date=args.to_date)
        df2 = APXRepDBLWTxnSummaryTable().read(scenario='BASE', data_handle='37020804B005458B874D74434DBCD0A0', from_date=args.from_date, to_date=args.to_date)

        # Call the function to compare dataframes
        compare_dataframes(df1, df2, match_columns, exclude_columns)

    for pc in args.portfolio_code:
        if args.run:
            engines = [
                StraightThruTransactionProcessingEngine(
                    source_queue_repo = None,
                    target_txn_repos = [CoreDBRealizedGainLossTransactionRepository()],
                    target_queue_repos = [],
                    source_txn_repo = APXDBRealizedGainLossRepository(),
                ),
                StraightThruTransactionProcessingEngine(
                    source_queue_repo = None,
                    target_txn_repos = [CoreDBTransactionActivityRepository()],
                    target_queue_repos = [],
                    source_txn_repo = APXDBTransactionActivityRepository(),
                ),
                LWTransactionSummaryEngine(
                    source_queue_repo = None,
                    target_txn_repos = [
                        # CoreDBLWTransactionSummaryRepository(),
                        COREDBLWTxnSummaryRepository(),
                    ],
                    target_queue_repos = [],
                    source_txn_repo = CoreDBTransactionActivityRepository(),
                    dividends_repo = APXDBDividendRepository(),
                    preprocessing_supplementary_repos = [
                        APXDBvPortfolioInMemoryRepository(),
                        APXDBvPortfolioBaseInMemoryRepository(),
                        APXDBvPortfolioBaseCustomInMemoryRepository(),
                        APXDBvPortfolioSettingExInMemoryRepository(),
                        APXDBvPortfolioBaseSettingExInMemoryRepository(),
                        APXDBvSecurityInMemoryRepository(), 
                        APXRepDBSecurityHashInMemoryRepository(),
                        APXDBvCurrencyInMemoryRepository(),
                        APXDBvCustodianInMemoryRepository(),
                        CoreDBRealizedGainLossSupplementaryRepository(),
                        # APXDBPastDividendRepository(),
                    ],
                    prev_bday_cost_repo = LWDBAPXAppraisalPrevBdayRepository(),
                    postprocessing_supplementary_repos = [
                        # TODO_LAYERS: should these logics just be part of the engine? 
                        TransactionNameRepository(),
                        TransactionSectionAndStmtTranRepository(),
                        TransactionOtherPostSupplementRepository(),
                        TransactionCashflowRepository(),
                    ]
                ),
            ]

            for engine in engines:
                print(f'Processing {pc} for {engine}...')
                result = engine.process(TransactionProcessingQueueItem(portfolio_code=pc, trade_date=args.from_date))
                print(f'Got {len(result)} transaction from {engine}...')
                # Now we have the results. Save them:
                for repo in engine.target_txn_repos:
                    print(f'Creating transactions in {repo.cn}...')
                    create_res = repo.create(transactions=result)

        df1 = COREDBLWTxnSummaryTable().read(portfolio_code=pc, from_date=args.from_date, to_date=args.to_date)
        if args.from_date == datetime.date(2024, 4, 8):
            df2 = APXRepDBLWTxnSummaryTable().read(scenario='BASE', data_handle='8328CB5AC1CB40E083F0DA4EE0DAC2BD', portfolio_code=pc, from_date=args.from_date, to_date=args.to_date)
        else:
            df2 = APXRepDBLWTxnSummaryTable().read(scenario='BASE', data_handle='37020804B005458B874D74434DBCD0A0', portfolio_code=pc, from_date=args.from_date, to_date=args.to_date)
    
        print(f"\n\n\n===== {pc} =====\n")

        # Call the function to compare dataframes
        compare_dataframes(df1, df2, match_columns, exclude_columns)

if __name__ == "__main__":
    main()


