"""
to run:
    - cd to directory of this file
    - <path_to_python_exe>python.exe -m unittest test_0501_sells_cost_per_unit.sells_0501_test_cost_per_unit

"""


# core python
import datetime
import os
import sys
import unittest
from unittest import mock
from typing import List, Union

# Append to path
src_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(src_dir)

# native
from domain.models import Transaction
from infrastructure.in_memory_repositories import CoreDBRealizedGainLossInMemoryRepository


class sells_0501_test_cost_per_unit(unittest.TestCase):

    def test_cost_per_unit(self):

        # Arrange
        txn_dict = {
            'PortfolioTransactionID': 13358799,
            'TranID': 39356,
            'LotNumber': 1,
            'Quantity': 123.456
        }
        txn = Transaction(**txn_dict)
        
        # Act
        CoreDBRealizedGainLossInMemoryRepository().supplement(txn)

        # Assert
        assert txn.Quantity == 123.456
        assert hasattr(txn, 'LocalCostPerUnit')

