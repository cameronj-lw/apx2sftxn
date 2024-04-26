
# core python
from abc import ABC, abstractmethod
from typing import List

# native
from domain.events import Event


class EventHandler(ABC):
    @property
    def cn(self):  # Class name. Avoids having to print/log type(self).__name__.
        return type(self).__name__
    
    @abstractmethod
    def handle(self, event: Event):
        """ Event handlers must handle a Event """


