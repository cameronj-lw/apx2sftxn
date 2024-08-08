
# core python
import datetime
import logging
import os

# pypi
from flask import Blueprint, current_app, request
from flask_cors import CORS
from flask_restx import Api, Resource, reqparse

# native
from interface.formatters import DefaultRESTFormatter

blueprint = Blueprint('blueprint', __name__)
api = Api(blueprint)
CORS(blueprint)
EXITING = False

# Request parser
parser = reqparse.RequestParser()
parser.add_argument('portfolio_code', type=str, help='Portfolio code')
parser.add_argument('from_date', type=str, help='Start date in YYYY-MM-DD format')
parser.add_argument('to_date', type=str, help='End date in YYYY-MM-DD format')


@api.route('/api/lw-transaction-summary')
class LWTransactionSummary(Resource):
    formatter = DefaultRESTFormatter()

    def get(self):
        try:
            # Parse the arguments
            args = parser.parse_args()
            portfolio_code = args['portfolio_code']
            from_date = datetime.date.fromisoformat(args['from_date'])
            to_date = datetime.date.fromisoformat(args['to_date'])

            # Get query handler, based on app config
            query_handler = current_app.config['lw_transaction_summary_query_handler']

            # Handle the query
            logging.info(f'Querying using {query_handler}...')  # TODO_CLEANUP: performance logging
            transactions = query_handler.handle(portfolio_code=portfolio_code, trade_date=(from_date, to_date))
            logging.info(f'Got {len(transactions)} from {query_handler}')  # TODO_CLEANUP: performance logging

            # Return standard format
            transactions_list = [t.to_dict() for t in transactions]
            logging.info(f'Formatted to list of dicts')  # TODO_CLEANUP: performance logging
            res = self.formatter.success_get(transactions_list)
            logging.info(f'Formatted to REST response format')  # TODO_CLEANUP: performance logging
            return res

        except Exception as e:
            logging.exception(f'Error handling request: {e}')
            return self.formatter.exception(e)


@api.route('/api/lw-apx2sftxn')
class LWTransactionSummary(Resource):
    formatter = DefaultRESTFormatter()

    def get(self):
        try:
            # Parse the arguments
            args = parser.parse_args()
            portfolio_code = args['portfolio_code']
            from_date = datetime.date.fromisoformat(args['from_date'])
            to_date = datetime.date.fromisoformat(args['to_date'])

            # Get query handler, based on app config
            query_handler = current_app.config['lw_apx2sftxn_query_handler']

            # Handle the query
            logging.info(f'Querying using {query_handler}...')  # TODO_CLEANUP: performance logging
            transactions = query_handler.handle(portfolio_code=portfolio_code, trade_date=(from_date, to_date))
            logging.info(f'Got {len(transactions)} from {query_handler}')  # TODO_CLEANUP: performance logging

            # Return standard format
            transactions_list = [t.to_dict() for t in transactions]
            logging.info(f'Formatted to list of dicts')  # TODO_CLEANUP: performance logging
            res = self.formatter.success_get(transactions_list)
            logging.info(f'Formatted to REST response format')  # TODO_CLEANUP: performance logging
            return res

        except Exception as e:
            logging.exception(f'Error handling request: {e}')
            return self.formatter.exception(e)

