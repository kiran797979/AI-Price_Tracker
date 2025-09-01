from abc import ABC, abstractmethod
from typing import Generic, TypeVar, List, Optional

T = TypeVar("T")


class BaseRepository(ABC, Generic[T]):
    """Base repository interface"""

    @abstractmethod
    def add(self, entity: T) -> T:
        """Add a new entity"""
        pass

    @abstractmethod
    def get(self, id: str) -> Optional[T]:
        """Get an entity by id"""
        pass

    @abstractmethod
    def get_all(self) -> List[T]:
        """Get all entities"""
        pass

    @abstractmethod
    def delete(self, id: str) -> None:
        """Delete an entity"""
        pass
