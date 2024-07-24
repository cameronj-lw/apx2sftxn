
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
    parser = argparse.ArgumentParser(description='Compare old txn summary results vs new engine results')
    parser.add_argument('--log_level', '-l', type=str.upper, choices=['DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL'], help='Log level')
    parser.add_argument('--from_date', '-fd', type=lambda d: datetime.datetime.strptime(d, '%Y-%m-%d').date())
    parser.add_argument('--to_date', '-td', type=lambda d: datetime.datetime.strptime(d, '%Y-%m-%d').date())
    parser.add_argument('--portfolio_code', '-pc', nargs='+', default=[])
    parser.add_argument('--gen_apx_realized_gain_loss', '-gargl', action='store_true', default=False)
    parser.add_argument('--gen_apx_txn_activity', '-gata', action='store_true', default=False)
    parser.add_argument('--run', '-run', action='store_true', default=False)
    parser.add_argument('--compare', '-diff', action='store_true', default=False)
    parser.add_argument('--no_log', '-nl', action='store_true', default=False)
    
    args = parser.parse_args()

    base_dir = AppConfig().get("logging", "base_dir")
    os.environ['APP_NAME'] = AppConfig().get("app_name", "apx2sftxn_compare")
    if not args.no_log:
        setup_logging(base_dir=base_dir, log_level_override=args.log_level)

    # df1 = COREDBLWTxnSummaryTable().read(portfolio_code=args.portfolio_code, from_date=args.data_date)
    # if args.data_date == datetime.date(2024, 4, 8):
    #     df2 = APXRepDBLWTxnSummaryTable().read(scenario='BASE', data_handle='8328CB5AC1CB40E083F0DA4EE0DAC2BD', portfolio_code=args.portfolio_code, from_date=args.data_date)
    # else:
    #     df2 = APXRepDBLWTxnSummaryTable().read(scenario='BASE', data_handle='37020804B005458B874D74434DBCD0A0', portfolio_code=args.portfolio_code, from_date=args.data_date)

    # Define columns for matching and exclusion
    match_columns = ['portfolio_code', 'trade_date', 'name4stmt', 'quantity']
    exclude_columns = ['record_id', 'scenario', 'data_handle', 'asofdate', 'asofuser', 'scenariodate', 'computer'
        , 'gendate', 'moddate', 'genuser', 'moduser'
        , 'lw_lineage'
        , 'lw_id', 'trade_date_original', 'portfolio_id'  # cols DNE in old LW Txn Summary or APX2SFTXN
        , 'sf_statement_group'  # thinking this is not needed for inclusion in new APX2SFTxn
        , 'security_id1', 'security_id2', 'price_per_unit_local'  # existing LW Transaction Summary doesn't save these to DB
        # , 'Portfolio__c'  # with MC running in test, there may be an incomplete SF dataset in most recent snap
    ]
    tolerances = {
        'fx_rate': 0.000051,  # Perl is inconsistent in number of rounding places (sometimes 4, sometimes 9? Not sure)
        'commission': 0.005,  # Perl is inconsistent in number of rounding places (2 places only for intl equities? Not sure)
        'trade_amt_firm__c': 0.011,  # Penny diffs in non-CAD portfolios - not sure why... TODO_ROUNDING: figure this out, ideally?
        'cash_flow_firm__c': 0.011,  # Penny diffs in non-CAD portfolios - not sure why... TODO_ROUNDING: figure this out, ideally?
        'quantity__c': 0.011,  # Penny diffs if quantity is x.xx5 ... could be from inconsistent perl rounding: https://stackoverflow.com/a/41318772 TODO_ROUNDING: figure this out?
    }
    
    if not args.portfolio_code:
        # df1 = COREDBLWTxnSummaryTable().read(from_date=args.from_date, to_date=args.to_date)
        # df2 = APXRepDBLWTxnSummaryTable().read(scenario='BASE', data_handle='37020804B005458B874D74434DBCD0A0', from_date=args.from_date, to_date=args.to_date)

        # df1 = COREDBLWTxnSummaryTable().read(from_date=args.from_date, to_date=args.to_date)
        # df2 = APXRepDBLWTxnSummaryTable().read(scenario='BASE', data_handle='37020804B005458B874D74434DBCD0A0', from_date=args.from_date, to_date=args.to_date)

        # # Call the function to compare dataframes
        # compare_dataframes(df1, df2, match_columns, exclude_columns)
        
        # target_df = LWDBSFTransactionTable().read(portfolio_code=None, from_date=args.from_date, to_date=args.to_date
        #                                         , data_handle='CJTEST_CR1504' # 'CJTEST20240620_v1'
        #                                     )
        target_df = LWDBSFTransactionTable().read(portfolio_code=None, from_date=args.from_date, to_date=args.to_date
                                                # , data_handle='CJTEST_CR1504' # 'CJTEST20240620_v1'
                                            )
        args.portfolio_code = target_df['portfolio_code'].unique().tolist()
    
    if args.run:
        engines = [
            # StraightThruTransactionProcessingEngine(
            #     source_queue_repo = None,
            #     target_txn_repos = [CoreDBRealizedGainLossTransactionRepository()],
            #     target_queue_repos = [],
            #     source_txn_repo = APXDBRealizedGainLossRepository(),
            # ),
            # StraightThruTransactionProcessingEngine(
            #     source_queue_repo = None,
            #     target_txn_repos = [CoreDBTransactionActivityRepository()],
            #     target_queue_repos = [],
            #     source_txn_repo = APXDBTransactionActivityRepository(),
            # ),
            LWTransactionSummaryEngine(
                source_queue_repo = CoreDBLWTransactionSummaryQueueRepository(),
                target_txn_repos = [
                    # CoreDBLWTransactionSummaryRepository(),
                    COREDBLWTxnSummaryRepository(),
                ],
                target_queue_repos = [COREDBAPX2SFTxnQueueRepository()],
                source_txn_repo = CoreDBTransactionActivityRepository(),
                # dividends_repo = APXDBDividendRepository(),
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
            ),
            LWAPX2SFTransactionEngine(
                source_queue_repo = COREDBAPX2SFTxnQueueRepository(),
                target_txn_repos = [
                    COREDBSFTransactionRepository(),
                ],
                target_queue_repos = [],
                source_txn_repo = COREDBLWTxnSummaryRepository(),
                preprocessing_supplementary_repos = [
                    APXDBvPortfolioInMemoryRepository(),
                    APXDBvPortfolioBaseSettingExInMemoryRepository(),
                    APXRepDBvPortfolioAndStmtGroupCurrencyInMemoryRepository(),
                    CoreDBSFPortfolioLatestInMemoryRepository(),
                    APXDBvSecurityInMemoryRepository(),
                    APXDBvCurrencyInMemoryRepository(),
                ],
                fx_rate_repo = APXDBvFXRateInMemoryRepository(),
            ),
        ]
    if args.gen_apx_realized_gain_loss:
        portfolios_supplement = APXDBvPortfolioInMemoryRepository()
        portfolios_supplement.relevant_columns.append('PortfolioCode')

        source_txn_repo = APXDBRealizedGainLossRepository()
        target_txn_repo = CoreDBRealizedGainLossTransactionRepository()

        print(f'{datetime.datetime.now()}: Reading from {source_txn_repo.cn}...')
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
        print(f'{datetime.datetime.now()}: Saving {len(res_transactions)} to {target_txn_repo.cn}')
        target_txn_repo.create(transactions=res_transactions)

        print(f'{datetime.datetime.now()}: Saved {len(res_transactions)} to {target_txn_repo.cn}')

    if args.gen_apx_txn_activity:
        source_txn_repo = APXDBTransactionActivityRepository()
        target_txn_repo = CoreDBTransactionActivityRepository()

        print(f'{datetime.datetime.now()}: Reading from {source_txn_repo.cn}...')
        res_transactions = source_txn_repo.get(portfolio_code=args.portfolio_code[0], trade_date=(args.from_date, args.to_date))
        
        # Populate the portfolio_code, modified_by, trade_date, lineage
        for txn in res_transactions:
            portfolios_supplement.supplement(txn)
            txn.portfolio_code = txn.PortfolioCode  # TODO: need to populate this?  # queue_item.portfolio_code
            txn.trade_date = txn.TradeDate
            txn.modified_by = f"{os.environ.get('APP_NAME')}_gen_apx_procs"
            txn.add_lineage(f"{str(source_txn_repo)}"
                                , source_callable=get_current_callable())

        # Save to target repo
        print(f'{datetime.datetime.now()}: Saving {len(res_transactions)} to {target_txn_repo.cn}')
        target_txn_repo.create(transactions=res_transactions)

        print(f'{datetime.datetime.now()}: Saved {len(res_transactions)} to {target_txn_repo.cn}')

    # else:
    #     engines = []

    for pc in args.portfolio_code:
        result = None
        for engine in engines:
            logging.info(f'{datetime.datetime.now()}: Processing {pc} for {engine}...')
            if not result:
                result = engine.process(queue_item=TransactionProcessingQueueItem(portfolio_code=pc, trade_date=args.from_date))
            else:
                result = engine.process(queue_item=TransactionProcessingQueueItem(portfolio_code=pc, trade_date=args.from_date)
                                            # , starting_transactions=result
                                        )
            logging.info(f'{datetime.datetime.now()}: Got {len(result)} transaction from {engine}...')
            # Now we have the results. Save them:
            for repo in engine.target_txn_repos:
                logging.info(f'{datetime.datetime.now()}: Creating transactions in {repo.cn}...')
                create_res = repo.create(transactions=result)

        match_columns = ['local_tran_key']  # for testing LW txn summary
        df1 = COREDBLWTxnSummaryTable().read(portfolio_code=pc, from_date=args.from_date, to_date=args.to_date)
        if args.from_date == datetime.date(2024, 4, 8):
            df2 = APXRepDBLWTxnSummaryTable().read(scenario='BASE', data_handle='8328CB5AC1CB40E083F0DA4EE0DAC2BD', portfolio_code=pc, from_date=args.from_date, to_date=args.to_date)
        elif pc == '8937' and args.from_date == datetime.date(2024, 3, 28):
            df2 = APXRepDBLWTxnSummaryTable().read(scenario='BASE', data_handle='DC746A299B4747AE896EDB806DBA7973', portfolio_code=pc, from_date=args.from_date, to_date=args.to_date)
        else:
            df2 = APXRepDBLWTxnSummaryTable().read(scenario='BASE', data_handle='37020804B005458B874D74434DBCD0A0', portfolio_code=pc, from_date=args.from_date, to_date=args.to_date)
    
        match_columns = ['lw_tran_id__c']  # for testing APX2SFTxn
        df1 = COREDBSFTransactionTable().read(portfolio_code=pc, from_date=args.from_date, to_date=args.to_date)
        df2 = LWDBSFTransactionTable().read(portfolio_code=pc, from_date=args.from_date, to_date=args.to_date
                                                , data_handle='CJTEST_CR1504' # 'CJTEST20240620_v1'
                                            )

        logging.info(f"\n\n\n{datetime.datetime.now()}: ===== {pc} =====\n")

        # Call the function to compare dataframes
        compare_dataframes(df1, df2, match_columns, exclude_columns, tolerances, ignore_zeros_vs_none=False)

        logging.info(f"\n{datetime.datetime.now()}: ==========\n\n")

if __name__ == "__main__":
    main()


