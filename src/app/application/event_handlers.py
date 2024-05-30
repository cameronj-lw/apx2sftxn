
# core python
from dataclasses import dataclass, field
import json
import logging
from typing import Dict, List, Union

# native
from domain.event_handlers import EventHandler
from domain.events import (Event, TransactionCreatedEvent, TransactionUpdatedEvent, TransactionDeletedEvent
    , TransactionCommentCreatedEvent, TransactionCommentUpdatedEvent, TransactionCommentDeletedEvent
)
from domain.models import Transaction, QueueStatus, TransactionProcessingQueueItem
from domain.repositories import TransactionRepository, SupplementaryRepository, TransactionProcessingQueueRepository



@dataclass
class TransactionEventHandler(EventHandler):
    target_queue_repos: List[TransactionProcessingQueueRepository]
    supplementary_repos: List[SupplementaryRepository] = field(default_factory=list)

    def handle(self, event: Union[TransactionCreatedEvent, TransactionUpdatedEvent, TransactionDeletedEvent
                , TransactionCommentCreatedEvent, TransactionCommentUpdatedEvent, TransactionCommentDeletedEvent]
                , stream_data: Union[Dict,None]=None):
        try:
            if isinstance(event, TransactionCreatedEvent) or isinstance(event, TransactionUpdatedEvent) or isinstance(event, TransactionDeletedEvent):
                transaction = event.transaction_after if isinstance(event, TransactionUpdatedEvent) else event.transaction
                                
                # Supplement from each specified repo
                for r in self.supplementary_repos:
                    r.supplement(transaction)

                # Confirm it now has a portfolio_code
                if not hasattr(transaction, 'portfolio_code'):
                    logging.error(f'Transaction does not have a portfolio_code! {transaction.__dict__}')

                # Create with PENDING status
                for repo in self.target_queue_repos:
                    if not repo.create(TransactionProcessingQueueItem(portfolio_code=transaction.portfolio_code, trade_date=transaction.trade_date, queue_status=QueueStatus.PENDING)):
                        return False
                
                # If we made it here, we successfully saved to all queue repos:
                return True
                
            else:
                logging.info(f'{self.cn} ignoring {event.cn}')
                return True

        except Exception as e:
            logging.exception(e)
        return True  # commit offset

    def __str__(self):
        return f"{self.cn}, saving results to {', '.join([str(r) for r in self.target_queue_repos])}"


