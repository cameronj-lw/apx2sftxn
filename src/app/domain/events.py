
# core python
from abc import ABC
from dataclasses import dataclass

# native
from domain.models import Transaction, TransactionComment


class Event(ABC):
    """ Base class for domain events """
    @property
    def cn(self):  # Class name. Avoids having to print/log type(self).__name__.
        return type(self).__name__


@dataclass
class TransactionCreatedEvent(Event):
    transaction: Transaction


@dataclass
class TransactionUpdatedEvent(Event):
    transaction_before: Transaction
    transaction_after: Transaction


@dataclass
class TransactionDeletedEvent(Event):
    transaction: Transaction


@dataclass
class TransactionCommentCreatedEvent(Event):
    comment: TransactionComment


@dataclass
class TransactionCommentUpdatedEvent(Event):
    comment_before: TransactionComment
    comment_after: TransactionComment


@dataclass
class TransactionCommentDeletedEvent(Event):
    comment: TransactionComment

