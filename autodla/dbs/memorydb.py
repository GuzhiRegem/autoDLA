import sqlite3
import polars as pl
from autodla.engine.data_conversion import DataTransformer, DataConversion
from autodla.engine.db import DB_Connection, TableName
from autodla.engine.query_builder import QueryBuilder
from autodla.engine.object import primary_key
from datetime import date, datetime
from uuid import UUID
import os

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
if "DATETIME_FORMAT" in os.environ:
    DATETIME_FORMAT = os.environ.get("DATETIME_FORMAT")
VERBOSE = False
if "AUTODLA_SQL_VERBOSE" in os.environ:
    VERBOSE = os.environ.get("AUTODLA_SQL_VERBOSE")

class MemoryQueryBuilder(QueryBuilder):
    def select(self, from_table: str, columns: list[str], where: str = None, limit: int = 10, order_by: str = None, group_by: list[str] = None, offset: int = None) -> str:
        qry = "SELECT " + ", ".join(columns) + " FROM " + from_table
        if where:
            qry += " WHERE " + where
        if order_by:
            qry += " ORDER BY " + order_by
        if limit:
            qry += " LIMIT " + str(limit)
        if offset:
            qry += " OFFSET " + str(offset)
        return qry

    def insert(self, into_table: str, values: list[dict]) -> str:
        qry = "INSERT INTO " + into_table + " (" + ", ".join(values[0].keys()) + ") VALUES "
        qry += ", ".join([f"({', '.join([self._data_transformer.convert_data(v) for v in d.values()])})" for d in values])
        return qry

    def update(self, table: str, values: dict, where: str) -> str:
        qry = f"UPDATE {table} SET {', '.join([f'{k.upper()} = {self._data_transformer.convert_data(v)}' for k, v in values.items()])} WHERE {where}"
        return qry

    def delete(self, table: str, where: str) -> str:
        qry = f"DELETE FROM {table} WHERE {where}"
        return qry

    def create_table(self, table_name: str, schema: dict, if_exists: bool = False) -> str:
        if_exists_st = "IF NOT EXISTS" if if_exists else ""
        items = [f'{k} {v}' for k, v in schema.items()]
        qry = f"CREATE TABLE {if_exists_st} {table_name} ({', '.join(items)});"
        return qry

    def drop_table(self, table_name: str, if_exists: bool = False) -> str:
        if_exists_st = "IF EXISTS" if if_exists else ""
        qry = f"DROP TABLE {if_exists_st} {table_name};"
        return qry

class MemoryDataTransformer(DataTransformer):
    TYPE_DICT = {
        UUID: DataConversion("TEXT", lambda x: f"'{x}'"),
        primary_key: DataConversion("TEXT", lambda x: f"'{x}'"),
        type(None): DataConversion('', lambda x: "NULL"),
        int: DataConversion('INTEGER'),
        float: DataConversion('REAL'),
        str: DataConversion('TEXT', lambda x: f"'{x}'"),
        bool: DataConversion('INTEGER', lambda x: "1" if x else "0"),
        date: DataConversion('TEXT', lambda x: f"'{x.year}-{x.month}-{x.day}'"),
        datetime: DataConversion('TEXT', lambda x: f"'{x.strftime(DATETIME_FORMAT)}'")
    }
    OPERATOR_DICT = {
        "numeric": {
            'Eq': "=",
            'NotEq': "!=",
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
            "FloorDiv": lambda x, y: f'({x} / {y})',
            "Mod": lambda x, y: f'{x} % {y}',
            "Pow": lambda x, y: f'POWER({x},{y})'
        },
        "boolean": {
            "And": 'AND',
            "Or": 'OR'
        },
        "unary": {
            "Not": 'NOT'
        }
    }
    NODE_COMPATIBILITY = {
        primary_key: UUID,
        UUID: primary_key
    }

class MemoryDB(DB_Connection):
    def __init__(self):
        self.__db_connection = sqlite3.connect(":memory:")
        dt = MemoryDataTransformer()
        self.tables = {}
        super().__init__(dt, MemoryQueryBuilder(dt))

    def get_table_name(self, table_name: str) -> TableName:
        return TableName(name=f'"{table_name.upper()}"', alias=f'"{table_name.lower()}"')

    def get_table_definition(self, table_name) -> dict[str, type]:
        cursor = self.__db_connection.cursor()
        cursor.execute(f"PRAGMA table_info('{table_name.split('.')[-1]}')")
        rows = cursor.fetchall()
        out = {}
        for row in rows:
            col_name = row[1]
            col_type = row[2]
            if not col_type:
                continue
            out[col_name.upper()] = self.data_transformer.get_type_from_sql_type(col_type)
        return out

    def execute(self, statement, commit=True):
        statement = self.normalize_statment(statement)
        cursor = self.__db_connection.cursor()
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
        except Exception:
            return None
        finally:
            if commit:
                self.__db_connection.commit()
            if VERBOSE:
                print("$$$$$$$$$$$$$")
                print()
    
    def snapshot_tables(self):
        out = {}
        for table, schema in self.__table_schemas.items():
            qry = self.query.select(from_table=self.get_table_name(table), columns=list(schema.keys()), limit=None)
            out[table] = self.execute(qry)
        return out
