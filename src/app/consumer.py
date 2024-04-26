
# core python
import argparse
import logging
import os
import sys

# Append to pythonpath
src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(src_dir)

# native
from application.event_handlers import TransactionEventHandler
from infrastructure.message_subscribers import KafkaAPXTransactionMessageConsumer
from infrastructure.sql_repositories import MGMTDBHeartbeatRepository, COREDBSFTransactionRepository
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
    
    kafka_consumer = KafkaAPXTransactionMessageConsumer(
        event_handler = TransactionEventHandler(
            target_txn_repo = COREDBSFTransactionRepository()
        )
        , heartbeat_repo = MGMTDBHeartbeatRepository()
    )
    logging.info(f'Consuming transactions...')
    kafka_consumer.consume(reset_offset=args.reset_offset)



if __name__ == '__main__':
    main()
