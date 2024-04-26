
# core python
from dataclasses import dataclass
import json
import logging
from typing import Dict, Union

# native
from domain.event_handlers import EventHandler
from domain.events import (Event, TransactionCreatedEvent, TransactionUpdatedEvent, TransactionDeletedEvent
    , TransactionCommentCreatedEvent, TransactionCommentUpdatedEvent, TransactionCommentDeletedEvent
)
from domain.repositories import TransactionRepository



@dataclass
class TransactionEventHandler(EventHandler):
    target_txn_repo: TransactionRepository

    def handle(self, event: Union[TransactionCreatedEvent, TransactionUpdatedEvent, TransactionDeletedEvent
                , TransactionCommentCreatedEvent, TransactionCommentUpdatedEvent, TransactionCommentDeletedEvent]
                , stream_data: Union[Dict,None]=None):
        try:
            if isinstance(event, TransactionCreatedEvent):
                logging.info(f'{self.cn} consuming transaction created event:')
                logging.info(f'{event.transaction}')
                return True
            elif isinstance(event, TransactionUpdatedEvent):
                logging.info(f'{self.cn} consuming transaction updated event:')
                logging.info(f'BEFORE: {event.transaction_before}')
                logging.info(f' AFTER: {event.transaction_after}')
                return True
            elif isinstance(event, TransactionDeletedEvent):
                logging.info(f'{self.cn} consuming transaction deleted event:')
                logging.info(f'{event.transaction}')
                return True
            else:
                logging.info(f'{self.cn} ignoring {event.cn}')
                return True

        except Exception as e:
            logging.exception(ex)
        return True  # commit offset

    def __str__(self):
        return f"{self.cn}, saving results to {self.target_txn_repo}"


