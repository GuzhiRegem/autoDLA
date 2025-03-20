import polars as pl
from typing import Dict, List, Tuple, Any, Optional, Union, Callable, ClassVar
from engine.data_conversion import DataTransformer
from pypika import Query

class DB_Connection:
    __data_transformer : DataTransformer
    __query : Query

    def __init__(self, data_transformer, query):
        self.__data_transformer = data_transformer
        self.__query = query

    @property
    def query(self):
        return self.__query

    @property
    def data_transformer(self):
        return self.__data_transformer

    def execute(self, query: str) -> pl.DataFrame:
        print("---------------")
        print(query)
        print("---------------")
        pass

    def ensure_table(self, table_name, schema):
        schema = self.data_transformer.convert_data_schema(schema)
        self.execute(self.query.drop_table(table_name).if_exists())
        self.execute(self.query.create_table(table_name).columns(*[f"{k} {v}" for k, v in schema.items()]))

if __name__ == "__main__":
    from autoDLA.pyton2sql.dbs.postgresdb import PostgresDataTransformer
    from pypika import PostgreSQLQuery
    db = DB_Connection(PostgresDataTransformer(), PostgreSQLQuery)
    schema = {
        "name": str,
        "age": int,
        "mass": float
    }
    db.ensure_table('user', schema)