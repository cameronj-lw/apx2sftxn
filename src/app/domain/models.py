
# core python
from dataclasses import dataclass, field
import datetime
from enum import Enum
import inspect
import os
from types import SimpleNamespace
from typing import Callable, Optional, Union


class Transaction(SimpleNamespace):
    """ Facilitates object instance creation from dict """
    @property
    def cn(self):  # Class name. Avoids having to print/log type(self).__name__.
        return type(self).__name__

    def __post_init__(self):
        # Post-assign these interchangeable attributes:
        if hasattr(self, 'PortfolioID'):
            self.PortfolioBaseID = self.PortfolioID
        elif hasattr(self, 'PortfolioBaseID'):
            self.PortfolioID = self.PortfolioBaseID

    def __str__(self):
        try:
            return f"{self.TransactionCode} of {self.Quantity} units of {self.SecurityID1} in {self.PortfolioID} on {self.TradeDate}"
        except Exception as e:
            return super().__str__()

    def get_lineage_msg_prefix(self, source_callable: Optional[Callable]=None) -> str:
        """ Get desired prefix to add context to lineage """
        lineage_msg = None        
        source_class_name = (source_callable.__self__.__class__.__name__ if inspect.ismethod(source_callable) else None)
        source_callable_name = (source_callable.__name__ if source_callable else None)
        app_name = os.environ.get('APP_NAME')
        if app_name:
            if source_callable_name:
                if source_class_name:
                    return f'{app_name}: {source_class_name}::{source_callable_name}: '
                else:
                    return f'{app_name}: {source_callable_name}: '
            else:
                return f'{app_name}: '
        else:
            return ''

    def add_lineage(self, msg: str, source_callable: Optional[Callable]=None):
        """ Append a new note on lineage """
        prefix = self.get_lineage_msg_prefix(source_callable)

        if not hasattr(self, 'lw_lineage'):
            self.lw_lineage = f'{prefix}{msg}'
        else:
            self.lw_lineage += f';\n{prefix}{msg}'
        

class TransactionComment(SimpleNamespace):
    """ Facilitates object instance creation from dict """
    @property
    def cn(self):  # Class name. Avoids having to print/log type(self).__name__.
        return type(self).__name__

    def __str__(self):
        return f"Comment in {self.PortfolioID} on {self.TradeDate}: {self.Comment}"


@dataclass
class Heartbeat:
    group: str
    name: str
    data_date: datetime.date = field(default_factory=datetime.date.today)
    modified_at: datetime.datetime = field(default_factory=datetime.datetime.now)

    @property
    def cn(self):  # Class name. Avoids having to print/log type(self).__name__.
        return type(self).__name__

    def to_dict(self):
        """ Export an instance to dict format """
        return {
            'group': self.group
            , 'name': self.name
            , 'data_date': self.data_date.isoformat() if self.data_date else self.data_date
            , 'modified_at': self.modified_at.isoformat() if self.modified_at else self.modified_at
        }

    @classmethod
    def from_dict(cls, data: dict):
        """ Create an instance from dict """
        try:
            # Validate required fields
            group = data['group']
            name = data['name']

            # Optional fields
            data_date = data.get('data_date', datetime.date.today())
            if isinstance(data_date, str):
                data_date = datetime.date.fromisoformat(data_date)

            modified_at = data.get('modified_at', datetime.datetime.now())
            if isinstance(modified_at, str):
                modified_at = datetime.datetime.fromisoformat(modified_at)

            return cls(group=group, name=name, data_date=data_date, modified_at=modified_at)
        except KeyError as e:
            raise InvalidDictError(f"Missing required field: {e}")


@dataclass
class PKColumnMapping:
    transaction_column_name: str
    supplementary_column_name: Union[str,None]=None

    def __post_init__(self):
        if not self.supplementary_column_name:
            self.supplementary_column_name = self.transaction_column_name


QueueStatus = Enum('QueueStatus', 'PENDING IN_PROGRESS SUCCESS FAIL UNKNOWN')

# Class method to create instances by value
def from_value(cls, value):
    for status in cls:
        if status.name == value.upper():
            return status
    raise ValueError(f"No enum member with value '{value}'")

QueueStatus.from_value = classmethod(from_value)

@dataclass
class TransactionProcessingQueueItem:
    portfolio_code: str
    trade_date: datetime.date
    queue_status: QueueStatus = QueueStatus.UNKNOWN

