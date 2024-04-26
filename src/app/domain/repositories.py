
# core python
from abc import ABC, abstractmethod
import datetime
from typing import List, Union

# native
from domain.models import Heartbeat, Transaction


class HeartbeatRepository(ABC):
    
    @abstractmethod
    def create(self, heartbeat: Heartbeat) -> int:
        pass

    @abstractmethod
    def get(self, data_date: Union[datetime.date,None]=None, group: Union[str,None]=None, name: Union[str,None]=None) -> List[Heartbeat]:
        pass

    @property
    def cn(self):  # Class name. Avoids having to print/log type(self).__name__.
        return type(self).__name__


class TransactionRepository(ABC):

    @abstractmethod
    def create(self, transaction: Transaction) -> int:
        pass

    @abstractmethod
    def get(self, data_date: Union[datetime.date,None]=None) -> List[Transaction]:
        pass

    @property
    def cn(self):  # Class name. Avoids having to print/log type(self).__name__.
        return type(self).__name__

