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
    
    def attach(self, objects):
        for obj in objects:
            obj.set_db(self)

    def execute(self, query: str) -> pl.DataFrame:
        print("---------------")
        print(query)
        print("---------------")
        pass

    def normalize_statment(self, statement: str) -> str:
        if hasattr(statement, "QUOTE_CHAR"):
            statement.QUOTE_CHAR = ""
        if not isinstance(statement, str):
            statement = str(statement)
        statement = statement.lstrip().rstrip()
        if statement[-1] != ";":
            statement += ";"
        return statement

    def ensure_table(self, table_name, schema):
        data_schema = {k: v["type"] for k, v in schema.items()}
        schema = self.data_transformer.convert_data_schema(data_schema)
        self.execute(self.query.drop_table(table_name).if_exists())
        qry = self.query.create_table(table_name).columns(*[f"{k} {v}" for k, v in schema.items()])
        self.execute(qry)