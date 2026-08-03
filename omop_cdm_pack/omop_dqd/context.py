"""Resolution of CDM table names to Polars LazyFrames."""

from typing import Dict, List, Set

import polars as pl

VOCABULARY_TABLES = frozenset({"CONCEPT", "CONCEPT_ANCESTOR"})


class CdmContext:
    """Lazy access to the CDM tables materialised as parquet.

    Table names are normalised to upper case, column names to lower case,
    so callers never have to worry about the casing a given source used.
    """

    def __init__(self, table_paths: Dict[str, List[str]]):
        self._paths = {
            name.upper(): paths for name, paths in table_paths.items()
        }
        self._schema_cache: Dict[str, Dict[str, pl.DataType]] = {}

    @classmethod
    def from_paths(cls, table_paths: Dict[str, List[str]]) -> "CdmContext":
        return cls(table_paths)

    @property
    def available_tables(self) -> Set[str]:
        return set(self._paths)

    @property
    def has_vocabulary(self) -> bool:
        return VOCABULARY_TABLES.issubset(self.available_tables)

    def has_table(self, name: str) -> bool:
        return name.upper() in self._paths

    def table(self, name: str) -> pl.LazyFrame:
        key = name.upper()
        if key not in self._paths:
            raise KeyError(f"CDM table {key} is not available")
        frame = pl.scan_parquet(self._paths[key])
        return frame.rename(
            {c: c.lower() for c in frame.collect_schema().names()}
        )

    def _schema(self, name: str) -> Dict[str, pl.DataType]:
        key = name.upper()
        if key not in self._schema_cache:
            schema = self.table(key).collect_schema()
            self._schema_cache[key] = dict(schema)
        return self._schema_cache[key]

    def columns(self, name: str) -> List[str]:
        return list(self._schema(name))

    def dtypes(self, name: str) -> Dict[str, pl.DataType]:
        return dict(self._schema(name))

    def has_column(self, table: str, column: str) -> bool:
        if not self.has_table(table):
            return False
        return column.lower() in self._schema(table)
