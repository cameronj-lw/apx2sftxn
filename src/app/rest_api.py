
# core python
import argparse
from configparser import ConfigParser
import logging
import os
import socket
import sys
import threading

# pypi
from flask import Flask
from waitress import serve

# Append to pythonpath
src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(src_dir)

# native
from application.query_handlers import (
    LWTransactionSummaryQueryHandler, LWAPX2SFTransactionQueryHandler
)
from infrastructure.in_memory_repositories import (
    APXDBvSecurityInMemoryRepository, APXRepDBSecurityHashInMemoryRepository,
    APXDBvPortfolioInMemoryRepository, APXDBvPortfolioSettingExInMemoryRepository, 
    APXDBvPortfolioBaseInMemoryRepository, APXDBvPortfolioBaseCustomInMemoryRepository, APXDBvPortfolioBaseSettingExInMemoryRepository,
    APXDBvCurrencyInMemoryRepository, APXDBvCustodianInMemoryRepository,
    APXDBvFXRateInMemoryRepository,
    APXRepDBvPortfolioAndStmtGroupCurrencyInMemoryRepository, CoreDBSFPortfolioLatestInMemoryRepository,
    CoreDBRealizedGainLossInMemoryRepository, APXDBRealizedGainLossInMemoryRepository
)
from infrastructure.message_subscribers import KafkaAPXTransactionMessageConsumer
from infrastructure.sql_repositories import (
    CoreDBTransactionActivityRepository,
    CoreDBRealizedGainLossSupplementaryRepository,
    LWDBAPXAppraisalPrevBdayRepository,
    MGMTDBHeartbeatRepository,
    APXDBTransactionActivityRepository, APXDBRealizedGainLossSupplementaryRepository
)
from infrastructure.util.config import AppConfig
from infrastructure.util.logging import setup_logging
from interface.formatters import CustomJSONEncoder
from interface.routes import blueprint  # import routes

# Initialize the Flask app and register blueprint
app = Flask(__name__)
EXITING = False

@app.route("/api/shutdown", methods=['POST'])
def exit_app():
    global EXITING
    EXITING = True
    return "Done"

@app.teardown_request
def teardown(exception):
    if EXITING:
        logging.info(f'Received shutdown request. Exiting...')
        os._exit(0)


if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser(description='Flask REST API using waitress for WSGI server')
        parser.add_argument('--reset_offset', '-ro', action='store_true', default=False, help='Reset consumer offset to beginning')
        parser.add_argument('--log_level', '-l', type=str.upper, choices=['DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL'], help='Log level')
        parser.add_argument('--kafka_consumer', '-kc', action='store_true', default=False, help='Activate kafka consumer to keep in-memory repos updated')
        # parser.add_argument('--use_coredb_sources', '-ucs', action='store_true', default=False, help='Use CoreDB versions of APX procs/funcs, asot querying APX procs/funcs directly')
        args = parser.parse_args()

        base_dir = AppConfig().get("logging", "base_dir")
        os.environ['APP_NAME'] = AppConfig().get("app_name", "lw_txn_engine_rest_api")
        setup_logging(base_dir=base_dir, log_level_override=args.log_level)

        # Get configs and run flask app
        host = socket.gethostbyname(socket.gethostname())
        port = AppConfig().parser.get("rest_api", "port")
        num_threads = AppConfig().get("rest_api", "num_threads", fallback=1)

        # Initialize command handlers and query handlers
        lw_transaction_summary_query_handler = LWTransactionSummaryQueryHandler(
            # source_txn_repo = CoreDBTransactionActivityRepository() if args.use_coredb_sources else APXDBTransactionActivityRepository(),
            source_txn_repo = CoreDBTransactionActivityRepository(),
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
                # CoreDBRealizedGainLossSupplementaryRepository(),
                # CoreDBRealizedGainLossInMemoryRepository() if args.use_coredb_sources else APXDBRealizedGainLossInMemoryRepository(),
                CoreDBRealizedGainLossInMemoryRepository(),
            ],
            prev_bday_cost_repo = LWDBAPXAppraisalPrevBdayRepository(),
        )
        lw_apx2sftxn_query_handler = LWAPX2SFTransactionQueryHandler(
            # source_txn_repo = CoreDBTransactionActivityRepository() if args.use_coredb_sources else APXDBTransactionActivityRepository(),
            source_txn_repo = CoreDBTransactionActivityRepository(),
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
                # CoreDBRealizedGainLossSupplementaryRepository(),
                # CoreDBRealizedGainLossInMemoryRepository() if args.use_coredb_sources else APXDBRealizedGainLossInMemoryRepository(),
                CoreDBRealizedGainLossInMemoryRepository(),
                APXRepDBvPortfolioAndStmtGroupCurrencyInMemoryRepository(),
                CoreDBSFPortfolioLatestInMemoryRepository(),
                APXDBvFXRateInMemoryRepository(),
            ],
            prev_bday_cost_repo = LWDBAPXAppraisalPrevBdayRepository(),
            # fx_rate_repo = APXDBvFXRateInMemoryRepository(),
        )

        # Inject dependencies into the Flask app context
        app.config['lw_transaction_summary_query_handler'] = lw_transaction_summary_query_handler
        app.config['lw_apx2sftxn_query_handler'] = lw_apx2sftxn_query_handler

        # Register the blueprint with the app, passing the app's config
        app.register_blueprint(blueprint, config=app.config)

        # Specify the app's encoder
        app.json_encoder = CustomJSONEncoder

        # Start using waitress
        # app.run(host=host, port=port, debug=True)
        logging.info(f'Starting REST API on {host} port {port} with {num_threads} threads...')
        serve(app, host=host, port=port, threads=num_threads)

        if args.kafka_consumer:
            kafka_consumer = KafkaAPXTransactionMessageConsumer(
                event_handler = None,
                heartbeat_repo = MGMTDBHeartbeatRepository(),
            )

            kafka_consumer_thread = threading.Thread(target=kafka_consumer.consume, kwargs={'reset_offset': args.reset_offset})

            # Start kafka thread
            logging.info(f'Starting kafka consumer....')
            kafka_consumer_thread.start()

        if args.kafka_consumer:
            kafka_consumer_thread.join()
        
    except Exception as e:
        logging.exception(f"{type(e).__name__}: {e}")
        sys.exit(1)
    sys.exit(0)


