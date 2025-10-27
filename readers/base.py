from abc import ABC, abstractmethod
from typing import List, Any


class DataReader(ABC):
    @abstractmethod
    def read(self) -> List[Any]:
        pass

