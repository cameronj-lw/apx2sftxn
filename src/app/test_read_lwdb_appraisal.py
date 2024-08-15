
import argparse
import datetime
import logging
import os
import sys


# Append to pythonpath
src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(src_dir)


# native
from infrastructure.sql_tables import (
    LWDBAPXAppraisalTable
)
from infrastructure.util.config import AppConfig
from infrastructure.util.logging import setup_logging




def main():
    table = LWDBAPXAppraisalTable()

    res = table.read(data_dt=datetime.date(2024, 3, 25), PortfolioCode='0525', SecurityID=269342)
    print(res)

if __name__ == "__main__":
    main()


