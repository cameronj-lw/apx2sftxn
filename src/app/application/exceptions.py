
from dataclasses import dataclass

from domain.models import Transaction


@dataclass
class TransactionShouldBeRemovedException(Exception):
    txn: Transaction


@dataclass
class TransactionShouldBeAddedException(Exception):
    txn: Transaction

