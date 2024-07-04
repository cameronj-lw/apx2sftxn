
# core python
import argparse
import logging
import os
import sys
import threading
import time

# Append to pythonpath
src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(src_dir)

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




def main():
    parser = argparse.ArgumentParser(description='Kafka Consumer')
    parser.add_argument('--reset_offset', '-ro', action='store_true', default=False, help='Reset consumer offset to beginning')
    parser.add_argument('--log_level', '-l', type=str.upper, choices=['DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL'], help='Log level')
    
    args = parser.parse_args()

    base_dir = AppConfig().get("logging", "base_dir")
    os.environ['APP_NAME'] = AppConfig().get("app_name", "apx2sftxn")
    setup_logging(base_dir=base_dir, log_level_override=args.log_level)

    engines = [
        StraightThruTransactionProcessingEngine(
            source_queue_repo = CoreDBRealizedGainLossQueueRepository(),
            target_txn_repos = [CoreDBRealizedGainLossTransactionRepository()],
            target_queue_repos = [CoreDBTransactionActivityQueueRepository()],
            source_txn_repo = APXDBRealizedGainLossRepository(),
        ),
        StraightThruTransactionProcessingEngine(
            source_queue_repo = CoreDBTransactionActivityQueueRepository(),
            target_txn_repos = [CoreDBTransactionActivityRepository()],
            target_queue_repos = [CoreDBLWTransactionSummaryQueueRepository()],
            source_txn_repo = APXDBTransactionActivityRepository(),
        ),
        LWTransactionSummaryEngine(
            source_queue_repo = CoreDBLWTransactionSummaryQueueRepository(),
            target_txn_repos = [
                # CoreDBLWTransactionSummaryRepository(),
                COREDBLWTxnSummaryRepository(),
            ],
            target_queue_repos = [COREDBAPX2SFTxnQueueRepository()],
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
        ),
        LWAPX2SFTransactionEngine(
            source_queue_repo = COREDBAPX2SFTxnQueueRepository(),
            target_txn_repos = [
                COREDBSFTransactionRepository(),
            ],
            target_queue_repos = [],
            source_txn_repo = COREDBLWTxnSummaryRepository(),
                preprocessing_supplementary_repos = [
                    APXDBvPortfolioBaseSettingExInMemoryRepository(),
                ],
            fx_rate_repo = APXDBvFXRateInMemoryRepository(),
        ),
    ]

    kafka_consumer = KafkaAPXTransactionMessageConsumer(
        event_handler = TransactionEventHandler(
            target_queue_repos = [CoreDBTransactionActivityQueueRepository()],
            supplementary_repos = [APXDBvPortfolioInMemoryRepository()],
        )
        , heartbeat_repo = MGMTDBHeartbeatRepository()
    )

    threads = [threading.Thread(target=engine.run) for engine in engines]
    kafka_consumer_thread = threading.Thread(target=kafka_consumer.consume, kwargs={'reset_offset': args.reset_offset})

    timers = [threading.Timer(interval=AppConfig().get('apx_transaction_engine', 'wait_sec', fallback=10), function=engine.run) for engine in engines]
    
    # # Start engine threads
    # for thread in threads:
    #     logging.info(f'Starting {thread}...')
    #     thread.start()

    # # Start engine timers
    # for timer in timers:
    #     logging.info(f'Starting {timer}...')
    #     timer.start()

    # Single-threaded? for testing
    while True:
        for engine in engines:
            logging.debug(f'Starting {engine}...')
            engine.run()
        time.sleep(5)

    # just kafka consumer, for testing
    logging.info(f'Consuming transactions...')
    kafka_consumer.consume(reset_offset=args.reset_offset)

    # # Start kafka thread
    # logging.info(f'Starting kafka consumer....')
    # kafka_consumer_thread.start()
    
    # # Await threads
    # for thread in threads:
    #     logging.info(f'Starting {thread}...')
    #     thread.join()

    kafka_consumer_thread.join()
    


if __name__ == '__main__':
    main()
