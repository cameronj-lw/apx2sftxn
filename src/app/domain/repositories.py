
# core python
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import datetime
import logging
from typing import Any, Dict, List, Tuple, Union

# native
from domain.models import Heartbeat, Transaction, PKColumnMapping, TransactionProcessingQueueItem, QueueStatus


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
    def create(self, transactions: Union[List[Transaction],Transaction]) -> int:
        pass

    @abstractmethod
    def get(self, portfolio_code: Union[str,None]=None, trade_date: Union[datetime.date, Tuple[datetime.date, datetime.date], None]=None) -> List[Transaction]:
        pass

    @property
    def cn(self):  # Class name. Avoids having to print/log type(self).__name__.
        return type(self).__name__


@dataclass
class SupplementaryRepository(ABC):
    """ For non-transaction data which may be used to enrich transactions """
    pk_columns: List[PKColumnMapping] = field(default_factory=list)

    @abstractmethod
    def create(self, data: Dict[str, Any]) -> int:
        pass 

    @abstractmethod
    def get(self, pk_column_values: Dict[str, Any]) -> dict:
        pass

    def supplement(self, transaction: Transaction) -> Union[Dict, None]:
        """ Default behaviour to supplement a transaction. Subclasses should override for other desired behaviour """
        
        supplemental_data = self._get_supplemental_data(transaction)

        # Update the transaction
        if isinstance(supplemental_data, dict):
            for key, value in supplemental_data.items():
                setattr(transaction, key, value)
        else:
            logging.debug(f'{self.cn} has no supplemental data for {transaction}')

        # Return supplemental data. 
        # The caller may benefit from being provided the supplemental data for other purpose (e.g. efficiency gain)
        return supplemental_data

    def _get_supplemental_data(self, transaction: Transaction) -> Union[Dict, None]:

        # Get PK column values
        pk_column_values = {}
        for col in self.pk_columns:
            pk_column_values.update({col.supplementary_column_name: getattr(transaction, col.transaction_column_name)})

        # Now we have a dict containing all desired filtering criteria. Get with that criteria and return result:
        supplemental_data = self.get(pk_column_values=pk_column_values)
        return supplemental_data

    @property
    def cn(self):  # Class name. Avoids having to print/log type(self).__name__.
        return type(self).__name__


@dataclass
class TransactionProcessingQueueRepository(ABC):

    @abstractmethod
    def create(self, queue_item: TransactionProcessingQueueItem) -> int:
        pass

    @abstractmethod
    def update_queue_status(self, queue_item: TransactionProcessingQueueItem, old_queue_status: Union[QueueStatus,None]=None) -> int:
        pass

    @abstractmethod
    def get(self, portfolio_code: Union[str,None]=None, trade_date: Union[datetime.date,None]=None
                , queue_status: Union[QueueStatus,None]=None) -> List[TransactionProcessingQueueItem]:
        pass

    @property
    def cn(self):  # Class name. Avoids having to print/log type(self).__name__.
        return type(self).__name__


