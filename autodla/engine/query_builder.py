import polars as pl
from typing import Callable, List, Optional
from autodla.engine.data_conversion import DataTransformer

class QueryBuilder:
    def __init__(self, data_transformer = DataTransformer):
        self._data_transformer = data_transformer

    def select(self, from_table: str, columns: List[str], where: str = None, limit: int = 10, order_by: str = None, group_by: list[str] = None) -> pl.DataFrame:
        pass

    def insert(self, into_table: str, values: List[dict]) -> None:
        pass

    def update(self, table: str, values: dict, where: str) -> None:
        pass

    def delete(self, table: str, where: str) -> None:
        pass

    def create_table(self, table_name: str, schema: dict, if_exists = False) -> None:
        pass

    def drop_table(self, table_name: str, if_exists = False) -> None:
        pass