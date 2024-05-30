
"""
to run:
    - cd to directory of this file
    - <path_to_python_exe>python.exe -m unittest test_supplement.SupplementTest

"""


import os
import sys
import unittest
from unittest import mock
from typing import Any, Dict

# Append to path
src_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(src_dir)


from domain.models import Transaction
from domain.repositories import SupplementaryRepository
from infrastructure.in_memory_repositories import APXRepDBSecurityHashInMemoryRepository


class SimpleSupplRepo(SupplementaryRepository):
    
    def create(self, data: Dict[str, Any]) -> int:
        pass 

    def get(self, pk_column_values: Dict[str, Any]) -> dict:
        if pk_column_values['SecurityID'] == 1:
            return {
                'SecurityID': 1,
                'Name4Stmt': 'name for stmt 1',
                'Name4Trading': 'name for trading 1',
            }
        if pk_column_values['SecurityID'] == 2:
            return {
                'SecurityID': 2,
                'Name4Stmt': 'name for stmt 2',
                'Name4Trading': 'name for trading 2',
            }

    def supplement(self, transaction: Transaction):
        for suffix in ('1', '2'):                
            # Get PK column values
            pk_column_values = {'SecurityID': getattr(transaction, f'SecurityID{suffix}')}

            # Now we have a dict containing all desired filtering criteria. Get with that criteria:
            supplemental_data = self.get(pk_column_values=pk_column_values)

            # Update the transaction
            for key, value in supplemental_data.items():
                setattr(transaction, f'{key}{suffix}', value)


class SupplementTest(unittest.TestCase):

    def test_supplementing_transaction(self):

        # Arrange
        txn = Transaction(**{'SecurityID1': 1, 'SecurityID2': 2})

        # Act
        SimpleSupplRepo().supplement(txn)

        # Assert
        print(txn)
        assert hasattr(txn, 'Name4Stmt1')

    def test_supplementing_from_hash(self):

        # Arrange
        txn = Transaction(**{'SecurityID1': 264450, 'SecurityID2': 6})

        # Act
        APXRepDBSecurityHashInMemoryRepository().supplement(txn)

        # Assert
        print(txn)
        assert hasattr(txn, 'Name4Stmt1')


