"""Database connector base class and implementations."""

from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, List, Optional
import logging

logger = logging.getLogger(__name__)


class DatabaseConnector(ABC):
    """Abstract base class for database connectors.
    
    Provides a unified interface for connecting to and querying
    different database systems.
    """
    
    def __init__(self, connection_string: str, **kwargs):
        self.connection_string = connection_string
        self._options = kwargs
        self._connection = None
    
    @abstractmethod
    def connect(self) -> None:
        """Establish database connection."""
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Close database connection."""
        pass
    
    @abstractmethod
    def get_tables(self) -> List[str]:
        """Get list of all tables in database."""
        pass
    
    @abstractmethod
    def get_columns(self, table: str) -> List[Dict[str, Any]]:
        """Get column information for a table.
        
        Returns list of dicts with: name, type, nullable, default
        """
        pass
    
    @abstractmethod
    def read_table(self, table: str, batch_size: int = 1000) -> Iterator[List[Dict]]:
        """Read table data in batches.
        
        Yields:
            List of row dicts
        """
        pass
    
    @abstractmethod
    def write_table(self, table: str, data: List[Dict]) -> int:
        """Write data to table.
        
        Returns:
            Number of rows written
        """
        pass
    
    @abstractmethod
    def execute(self, query: str, params: Optional[tuple] = None) -> Any:
        """Execute raw SQL query."""
        pass
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
