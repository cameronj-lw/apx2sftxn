
# core python
import datetime
import logging
import os
import traceback

# pypi
from flask import Blueprint, current_app, request
from flask_cors import CORS
from flask_restx import Api, Resource, reqparse

# native
from interface.formatters import DefaultRESTFormatter, DefaultXMLFormatter

blueprint = Blueprint('blueprint', __name__)
api = Api(blueprint)
CORS(blueprint)
EXITING = False

# Request parser
parser = reqparse.RequestParser()
parser.add_argument('portfolio_code', type=str, help='Portfolio code')
parser.add_argument('from_date', type=str, help='Start date in YYYY-MM-DD format')
parser.add_argument('to_date', type=str, help='End date in YYYY-MM-DD format')
parser.add_argument('output_format', type=str, help='Specify XML for XML or defaults to JSON')


@api.route('/api/lw-transaction-summary')
class LWTransactionSummaryEndpoint(Resource):
    formatter = DefaultRESTFormatter()

    def get(self):
        try:
            # Parse the arguments
            args = parser.parse_args()
            portfolio_code = args['portfolio_code']
            from_date = datetime.date.fromisoformat(args['from_date'])
            to_date = datetime.date.fromisoformat(args['to_date'])
            output_format = args.get('output_format')
            if output_format:
                output_format = output_format.upper()

            # Update the formatter based on output format
            if output_format == 'XML':
                self.formatter = DefaultXMLFormatter()
            else:
                self.formatter = DefaultRESTFormatter()

            # Get query handler, based on app config
            query_handler = current_app.config['lw_transaction_summary_query_handler']

            # Handle the query
            logging.debug(f'[Performance] Querying using {query_handler}...')
            transactions = query_handler.handle(portfolio_code=portfolio_code, trade_date=(from_date, to_date))
            logging.debug(f'[Performance] Got {len(transactions)} from {query_handler}')

            # Return standard format
            transactions_list = [t.to_dict() for t in transactions]
            logging.debug(f'[Performance] Formatted to list of dicts')
            res = self.formatter.success_get(transactions_list)
            logging.debug(f'[Performance] Formatted to REST response format')
            return res

        except Exception as e:
            logging.exception(f'Error handling request: {e}')
            logging.exception(traceback.format_exc())
            return self.formatter.exception(e)


@api.route('/api/lw-apx2sftxn')
class LWAPX2SFTransactionSummaryEndpoint(Resource):
    formatter = DefaultRESTFormatter()

    def get(self):
        try:
            # Parse the arguments
            args = parser.parse_args()
            logging.info(f'{self.cn} handling GET request with the following args: {args}')
            portfolio_code = args['portfolio_code']
            from_date = datetime.date.fromisoformat(args['from_date'])
            to_date = datetime.date.fromisoformat(args['to_date'])
            output_format = args.get('output_format')
            if output_format:
                output_format = output_format.upper()

            # Update the formatter based on output format
            if output_format == 'XML':
                self.formatter = DefaultXMLFormatter()
            else:
                self.formatter = DefaultRESTFormatter()

            # Get query handler, based on app config
            query_handler = current_app.config['lw_apx2sftxn_query_handler']

            # Handle the query
            logging.debug(f'[Performance] Querying using {query_handler}...')
            transactions = query_handler.handle(portfolio_code=portfolio_code, trade_date=(from_date, to_date))
            logging.debug(f'[Performance] Got {len(transactions)} from {query_handler}')

            # Return standard format
            transactions_list = [t.to_dict() for t in transactions]
            logging.debug(f'[Performance] Formatted to list of dicts')
            res = self.formatter.success_get(transactions_list)
            logging.debug(f'[Performance] Formatted to REST response format')
            logging.info(f'{self.cn} providing response for GET request with the following args: {args}')
            return res

        except Exception as e:
            logging.exception(f'Error handling request: {e}')
            logging.exception(traceback.format_exc())
            return self.formatter.exception(e)

    @property
    def cn(self):  # Class name. Avoids having to print/log type(self).__name__.
        return type(self).__name__    

