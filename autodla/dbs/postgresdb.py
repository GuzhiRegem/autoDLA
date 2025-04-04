import psycopg2
import polars as pl
from autodla.engine.data_conversion import DataTransformer, DataConversion
from autodla.engine.db import DB_Connection
from autodla.engine.object import primary_key
from autodla.engine.query_builder import QueryBuilder
from datetime import date, datetime
from typing import List, Optional
from uuid import UUID
import os

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
if "DATETIME_FORMAT" in os.environ:
    DATETIME_FORMAT = os.environ.get("DATETIME_FORMAT")
POSTGRES_USER = 'postgres'
if "AUTODLA_POSTGRES_USER" in os.environ:
    POSTGRES_USER = os.environ.get("AUTODLA_POSTGRES_USER")
POSTGRES_PASSWORD = 'password'
if "AUTODLA_POSTGRES_PASSWORD" in os.environ:
    POSTGRES_PASSWORD = os.environ.get("AUTODLA_POSTGRES_PASSWORD")
POSTGRES_URL = 'localhost'
if "AUTODLA_POSTGRES_HOST" in os.environ:
    POSTGRES_URL = os.environ.get("AUTODLA_POSTGRES_HOST")
POSTGRES_DB = 'my_db'
if "AUTODLA_POSTGRES_DB" in os.environ:
    POSTGRES_DB = os.environ.get("AUTODLA_POSTGRES_DB")
VERBOSE = False
if "AUTODLA_SQL_VERBOSE" in os.environ:
    VERBOSE = os.environ.get("AUTODLA_SQL_VERBOSE")

CONNECTION_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_URL}/{POSTGRES_DB}"

class PostgresQueryBuilder(QueryBuilder):
    def select(self, from_table: str, columns: List[str], where: str = None, limit: int = 10, order_by: str = None, group_by: list[str] = None) -> pl.DataFrame:
        qry = "SELECT " + ", ".join(columns) + " FROM " + from_table
        if where:
            qry += " WHERE " + where
        if order_by:
            qry += " ORDER BY " + order_by
        if limit:
            qry += " LIMIT " + str(limit)
        return qry

    def insert(self, into_table: str, values: List[dict]) -> None:
        qry = "INSERT INTO " + into_table + " (" + ", ".join(values[0].keys()) + ") VALUES "
        qry += ", ".join([f"({', '.join([self._data_transformer.convert_data(v) for v in d.values()])})" for d in values])
        return qry

    def update(self, table: str, values: dict, where: str) -> None:
        qry = f"UPDATE {table} SET {', '.join([f'{k.upper()} = {self._data_transformer.convert_data(v)}' for k, v in values.items()])} WHERE {where}"
        return qry

    def delete(self, table: str, where: str) -> None:
        qry = f"DELETE FROM {table} WHERE {where}"
        return qry

    def create_table(self, table_name: str, schema: dict, if_exists = False) -> None:
        if_exists_st = "IF EXISTS" if if_exists else ""
        items = [f'{k} {v}' for k, v in schema.items()]
        qry = f"CREATE TABLE {if_exists_st} {table_name} ({', '.join(items)});"
        return qry

    def drop_table(self, table_name: str, if_exists = False) -> None:
        if_exists_st = "IF EXISTS" if if_exists else ""
        qry = f"DROP TABLE {if_exists_st} {table_name};"
        return qry

class PostgresDataTransformer(DataTransformer):
    TYPE_DICT= {
        UUID: DataConversion("UUID", lambda x: f"'{x}'"),
        primary_key: DataConversion("UUID", lambda x: f"'{x}'"),
        type(None): DataConversion('', lambda x: "NULL"),
        int: DataConversion('INTEGER'),
        float: DataConversion("REAL"),
        str: DataConversion("TEXT", lambda x: f"'{x}'"),
        bool: DataConversion("BOOL", lambda x: {True: "TRUE", False: "FALSE"}[x]),
        date: DataConversion("DATE", lambda x: f"'{x.year}-{x.month}-{x.day}'"),
        datetime: DataConversion("TIMESTAMP", lambda x: f"'{x.strftime(DATETIME_FORMAT)}'"),
    }
    OPERATOR_DICT = {
        "numeric": {
            'Eq': "=",
            'NotEq': "<>",
            'Lt': "<",
            'LtE': "<=",
            'Gt': ">",
            'GtE': ">=",
            'In': "IN",
            'NotIn': "NOT IN",
            'Is': "IS",
            'IsNot': "IS NOT"
        },
        "binary": {
            "Add": lambda x, y: f'{x} + {y}',
            "Sub": lambda x, y: f'{x} - {y}',
            "Mult": lambda x, y: f'{x} * {y}',
            "Div": lambda x, y: f'{x} / {y}',
            "FloorDiv": lambda x, y: f'FLOOR({x} / {y})',
            "Mod": lambda x, y: f'{x} % {y}',
            "Pow": lambda x, y: f'POWER({x},{y})'
        },
        "boolean": {
            "And": 'AND',
            "Or": 'OR',
        },
        "unary": {
            "Not": 'NOT'
        }
    }
    NODE_COMPATIBILITY = {
        primary_key: UUID,
        UUID: primary_key
    }

class PostgresDB(DB_Connection):

    def __init__(self, connection_url=CONNECTION_URL):
        self.__db_connection = psycopg2.connect(connection_url)
        dt = PostgresDataTransformer()
        super().__init__(dt, PostgresQueryBuilder(dt))
    
    def get_table_definition(self, table_name) -> dict[str, type]:
        if "." in table_name:
            table_name = table_name.split(".")[-1]
        res = self.execute(self.query.select(
            from_table='INFORMATION_SCHEMA.COLUMNS',
            columns=["column_name", "data_type"],
            limit=None,
            where=f"table_name = '{table_name}'"
        )).to_dicts()
        conversion_dict = {
            "boolean": "bool",
            "timestamp without time zone": "timestamp"
        }
        out = {}
        for row in res:
            if row['data_type'] in conversion_dict:
                row['data_type'] = conversion_dict[row['data_type']]
            out[row['column_name'].upper()] = self.data_transformer.get_type_from_sql_type(row["data_type"])
        return out
                
    def execute(self, statement, commit=True):
        statement = self.normalize_statment(statement)
        with self.__db_connection.cursor() as cursor:
            if VERBOSE:
                print()
                print("$$$$$$ SQL STATEMENT $$$$$$")
                print(statement)
            cursor.execute(statement)
            try:
                rows = cursor.fetchall()
                schema = [desc[0] for desc in cursor.description]
                out = pl.DataFrame(rows, schema=schema, orient='row')
                if VERBOSE:
                    print()
                    print(out)
                return out
            except:
                return None
            finally:
                if commit:
                    self.__db_connection.commit()
                if VERBOSE:
                    print("$$$$$$$$$$$$$")
                    print()
