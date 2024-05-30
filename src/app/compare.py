
import argparse
import datetime
import os
import sys


# Append to pythonpath
src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(src_dir)


from infrastructure.sql_tables import COREDBLWTxnSummaryTable, APXRepDBLWTxnSummaryTable
from infrastructure.util.dataframe import compare_dataframes



def main():
    parser = argparse.ArgumentParser(description='Compare old txn summary results vs new engine results')
    parser.add_argument('--from_date', '-fd', type=lambda d: datetime.datetime.strptime(d, '%Y-%m-%d'))
    parser.add_argument('--to_date', '-td', type=lambda d: datetime.datetime.strptime(d, '%Y-%m-%d'))
    parser.add_argument('--portfolio_code', '-pc', nargs='+')
    
    args = parser.parse_args()

    # df1 = COREDBLWTxnSummaryTable().read(portfolio_code=args.portfolio_code, from_date=args.data_date)
    # if args.data_date == datetime.date(2024, 4, 8):
    #     df2 = APXRepDBLWTxnSummaryTable().read(scenario='BASE', data_handle='8328CB5AC1CB40E083F0DA4EE0DAC2BD', portfolio_code=args.portfolio_code, from_date=args.data_date)
    # else:
    #     df2 = APXRepDBLWTxnSummaryTable().read(scenario='BASE', data_handle='37020804B005458B874D74434DBCD0A0', portfolio_code=args.portfolio_code, from_date=args.data_date)
    
    for pc in args.portfolio_code:
        df1 = COREDBLWTxnSummaryTable().read(portfolio_code=pc, from_date=args.from_date, to_date=args.to_date)
        if args.from_date == datetime.date(2024, 4, 8):
            df2 = APXRepDBLWTxnSummaryTable().read(scenario='BASE', data_handle='8328CB5AC1CB40E083F0DA4EE0DAC2BD', portfolio_code=pc, from_date=args.from_date, to_date=args.to_date)
        else:
            df2 = APXRepDBLWTxnSummaryTable().read(scenario='BASE', data_handle='37020804B005458B874D74434DBCD0A0', portfolio_code=pc, from_date=args.from_date, to_date=args.to_date)
    
        # Define columns for matching and exclusion
        match_columns = ['portfolio_code', 'trade_date', 'name4stmt', 'quantity']
        match_columns = ['local_tran_key']
        exclude_columns = ['record_id', 'scenario', 'data_handle', 'asofdate', 'asofuser', 'scenariodate', 'computer', 'trade_date_original', 'fx_rate']
        
        print(f"\n\n\n===== {pc} =====\n")

        # Call the function to compare dataframes
        compare_dataframes(df1, df2, match_columns, exclude_columns)

if __name__ == "__main__":
    main()


