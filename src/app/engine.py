
# core python
import argparse
import logging
import os
import sys
import time

# Append to pythonpath
src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(src_dir)

# native
from application.engines import StraightThruTransactionProcessingEngine
from infrastructure.sql_repositories import (
    CoreDBRealizedGainLossQueueRepository, CoreDBRealizedGainLossTransactionRepository, APXDBRealizedGainLossRepository,
    CoreDBTransactionActivityQueueRepository, CoreDBTransactionActivityRepository, APXDBTransactionActivityRepository,
    MGMTDBHeartbeatRepository,
)
from infrastructure.util.config import AppConfig
from infrastructure.util.logging import setup_logging




def main():
    parser = argparse.ArgumentParser(description='LW Transaction Engine - engine component')
    parser.add_argument('--log_level', '-l', type=str.upper, choices=['DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL'], help='Log level')
    parser.add_argument('--sleep_secs', '-ss', type=int, help='How many seconds to wait between running engines. If not provided, default to config.ini')
    
    args = parser.parse_args()

    base_dir = AppConfig().get("logging", "base_dir")
    os.environ['APP_NAME'] = AppConfig().get("app_name", "lw_txn_engine_engine")
    setup_logging(base_dir=base_dir, log_level_override=args.log_level)

    engines = [
        StraightThruTransactionProcessingEngine(
            source_queue_repo = CoreDBRealizedGainLossQueueRepository(),
            target_txn_repos = [CoreDBRealizedGainLossTransactionRepository()],
            source_txn_repo = APXDBRealizedGainLossRepository(),
            target_queue_repos = [],
        ),
        StraightThruTransactionProcessingEngine(
            source_queue_repo = CoreDBTransactionActivityQueueRepository(),
            target_txn_repos = [CoreDBTransactionActivityRepository()],
            source_txn_repo = APXDBTransactionActivityRepository(),
            target_queue_repos = [],
        ),
    ]

    sleep_secs = args.sleep_secs or int(AppConfig().get('engine', 'sleep_secs', fallback=60))

    # Create and save heartbeat
    heartbeat_repo = MGMTDBHeartbeatRepository()
    heartbeat = heartbeat_repo.heartbeat_class(group='LW-Transaction-Engine', name=os.environ.get('APP_NAME'))
    heartbeat_repo.create(heartbeat)

    # Start the engines
    for engine in engines: 
        engine.start()

    # Loop endlessly processing
    while True:
        for engine in engines:
            # Create and save heartbeat
            # Doing so inside this for loop ensures saving the heartbeat in between processing for multiple engines
            heartbeat = heartbeat_repo.heartbeat_class(group='LW-Transaction-Engine', name=os.environ.get('APP_NAME'))
            heartbeat_repo.create(heartbeat)

            # Now run the engine
            logging.debug(f'Starting {engine}...')
            engine.run()

        logging.debug(f'Sleeping for {sleep_secs} seconds...')
        time.sleep(sleep_secs)


if __name__ == '__main__':
    main()
