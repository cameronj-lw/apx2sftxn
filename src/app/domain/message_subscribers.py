
# core python
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Type

# native
from domain.event_handlers import EventHandler
from domain.message_brokers import MessageBroker


@dataclass
class MessageSubscriber(ABC):
    message_broker: Type[MessageBroker]
    topics: List[str]
    event_handler: Type[EventHandler]

    @property
    def cn(self):  # Class name. Avoids having to print/log type(self).__name__.
        return type(self).__name__

