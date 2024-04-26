
# core python
from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class MessageBroker(ABC):
    """ Base class for message queues/brokers """
    config: dict

    @property
    def cn(self):  # Class name. Avoids having to print/log type(self).__name__.
        return type(self).__name__

