"""File format handlers for various data formats."""

import csv
import json
from abc import ABC, abstractmethod
from typing import Dict, Iterator, List


class FormatHandler(ABC):
    """Abstract base for file format handlers."""

    @abstractmethod
    def read(self, filepath: str) -> Iterator[Dict]:
        """Read file and yield records."""
        pass

    @abstractmethod
    def write(self, filepath: str, data: List[Dict]) -> None:
        """Write data to file."""
        pass


class JSONHandler(FormatHandler):
    """Handler for JSON and JSONL files."""

    def read(self, filepath: str) -> Iterator[Dict]:
        """Read JSON or JSONL file."""
        with open(filepath) as f:
            if filepath.endswith(".jsonl"):
                for line in f:
                    if line.strip():
                        yield json.loads(line)
            else:
                data = json.load(f)
                if isinstance(data, list):
                    yield from data
                else:
                    yield data

    def write(self, filepath: str, data: List[Dict]) -> None:
        """Write JSON or JSONL file."""
        with open(filepath, "w") as f:
            if filepath.endswith(".jsonl"):
                for record in data:
                    f.write(json.dumps(record) + "\n")
            else:
                json.dump(data, f, indent=2)


class CSVHandler(FormatHandler):
    """Handler for CSV and TSV files."""

    def __init__(self, delimiter: str = ",", encoding: str = "utf-8"):
        self.delimiter = delimiter
        self.encoding = encoding

    def read(self, filepath: str) -> Iterator[Dict]:
        """Read CSV/TSV file."""
        with open(filepath, encoding=self.encoding, newline="") as f:
            reader = csv.DictReader(f, delimiter=self.delimiter)
            yield from reader

    def write(self, filepath: str, data: List[Dict]) -> None:
        """Write CSV/TSV file."""
        if not data:
            return

        with open(filepath, "w", encoding=self.encoding, newline="") as f:
            writer = csv.DictWriter(
                f, fieldnames=data[0].keys(), delimiter=self.delimiter
            )
            writer.writeheader()
            writer.writerows(data)


class ParquetHandler(FormatHandler):
    """Handler for Parquet files."""

    def read(self, filepath: str) -> Iterator[Dict]:
        """Read Parquet file."""
        try:
            import pandas as pd

            df = pd.read_parquet(filepath)
            for record in df.to_dict("records"):
                yield record
        except ImportError:
            raise ImportError("pandas required for Parquet support")

    def write(self, filepath: str, data: List[Dict]) -> None:
        """Write Parquet file."""
        try:
            import pandas as pd

            df = pd.DataFrame(data)
            df.to_parquet(filepath, index=False)
        except ImportError:
            raise ImportError("pandas and pyarrow required for Parquet support")


class ExcelHandler(FormatHandler):
    """Handler for Excel files."""

    def read(self, filepath: str, sheet_name: str = None) -> Iterator[Dict]:
        """Read Excel file."""
        try:
            import pandas as pd

            df = pd.read_excel(filepath, sheet_name=sheet_name)
            for record in df.to_dict("records"):
                yield record
        except ImportError:
            raise ImportError("pandas and openpyxl required for Excel support")

    def write(
        self, filepath: str, data: List[Dict], sheet_name: str = "Sheet1"
    ) -> None:
        """Write Excel file."""
        try:
            import pandas as pd

            df = pd.DataFrame(data)
            df.to_excel(filepath, sheet_name=sheet_name, index=False)
        except ImportError:
            raise ImportError("pandas and openpyxl required for Excel support")
