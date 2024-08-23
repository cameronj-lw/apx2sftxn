
# core python
import argparse
from configparser import ConfigParser
import datetime
import json
import logging
import os
import requests
import socket
import sys

# Append to pythonpath
src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(src_dir)

# native
from domain.models import Transaction
from infrastructure.sql_repositories import (
    COREDBSFTransactionRepository
)
from infrastructure.util.config import AppConfig
from infrastructure.util.logging import setup_logging


if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser(description='Stage transaction data for uploading to Salesforce')
        parser.add_argument('--log_level', '-l', type=str.upper, choices=['DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL'], help='Log level')
        parser.add_argument('--from_date', '-fd', type=lambda d: datetime.datetime.strptime(d, '%Y-%m-%d').date())
        parser.add_argument('--to_date', '-td', type=lambda d: datetime.datetime.strptime(d, '%Y-%m-%d').date())
        parser.add_argument('--portfolio_code', '-pc', type=str)
        parser.add_argument('--calendar_days_back', type=int, help='If from_date is not provided, assign it as this many days before to_date')
        args = parser.parse_args()

        # Derive from_date if not provided, based on calendar_days_back
        if not args.from_date and args.calendar_days_back:
            args.from_date = args.to_date - datetime.timedelta(days=args.calendar_days_back)

        base_dir = AppConfig().get("logging", "base_dir")
        os.environ['APP_NAME'] = AppConfig().get("app_name", "apx2sftxn")
        setup_logging(base_dir=base_dir, log_level_override=args.log_level)

        # Get configs
        host = socket.gethostbyname(socket.gethostname())
        host = AppConfig().parser.get("rest_api", "host")
        port = AppConfig().parser.get("rest_api", "port")

        # Get transactions from REST API
        params = {k:(v.isoformat() if isinstance(v, (datetime.datetime, datetime.date)) else v) for k, v in args.__dict__.items()}
        # url = f"http://{host}:{port}/api/lw-apx2sftxn?portfolio_code={args.portfolio_code}&from_date={args.from_date.isoformat()}&to_date={args.to_date.isoformat()}"
        url = f"http://{host}:{port}/api/lw-apx2sftxn"
        logging.info(f'Params: {params}')
        logging.info(f'Getting transaction from URL... {url}')
        response = requests.get(url, params=params)

        # Check if the request was successful
        if response.status_code == 200:
            # Step 2: Convert the JSON result to a list of Transaction instances
            json_data = response.json()
            txn_dicts = [{k: (datetime.date.fromisoformat(v) if 'date' in k.lower() and isinstance(v, str) else v) for k, v in d.items()} for d in json_data['data']]
            transactions = [Transaction(**d) for d in txn_dicts]

            # Step 3: Save the Transactions to a SQL table using TransactionRepository
            try:
                repo = COREDBSFTransactionRepository()
                row_cnt = repo.create(transactions)
                logging.info(f'Successfully saved {row_cnt} rows to {repo.cn}.')
            except Exception as e:
                logging.error(f'Exception while saving to {repo.cn}: {e}')
        else:
            logging.info(f"Failed to get data from API. Status code: {response.status_code}")
        

    except Exception as e:
        logging.exception(f"{type(e).__name__}: {e}")
        sys.exit(1)
    sys.exit(0)


