import sqlite3
import polars as pl
from autodla.engine.data_conversion import DataTransformer, DataConversion
from autodla.engine.db import DB_Connection, TableName
from autodla.engine.query_builder import QueryBuilder
from autodla.engine.object import primary_key
from autodla.utils.logger import logger
from datetime import date, datetime
from uuid import UUID, uuid4
import os
import time

import threading

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
if "DATETIME_FORMAT" in os.environ:
    DATETIME_FORMAT = os.environ.get("DATETIME_FORMAT")

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

import atexit

class MemoryDB(DB_Connection):
    def __init__(self):
        self.query_queue = []
        self.result_queue = {}
        self.lock = threading.Lock()
        self.db_status = {"connected": False}
        threading.Thread(target=self.pooling_queue).start()
        while True:
            time.sleep(0.1)
            with self.lock:
                if self.db_status["connected"]:
                    break
        dt = MemoryDataTransformer()
        self.tables = {}
        super().__init__(dt, MemoryQueryBuilder(dt))
        self.__querys_executed_memory = 0
    
    @property
    def usage_metrics(self):
        """
        Returns the usage metrics of the MemoryDB connection.
        This is a placeholder method and should be implemented in subclasses.
        """
        return {"memorydb": self.__querys_executed_memory}
    
    def exit(self):
        with self.lock:
            if self.query_queue is not None:
                logger.debug("Closing MemoryDB connection...")
                self.query_queue = None

    
    def pooling_queue(self):
        self.__db_connection = sqlite3.connect(":memory:")
        with self.lock:
            self.db_status["connected"] = True
        while True:
            if self.query_queue is None:
                break
            if self.query_queue:
                with self.lock:
                    query = self.query_queue.pop(0)
                    self.result_queue[query["execution_id"]] = self._execute_memory(**{"statement_input": query["statement"], "commit": query["commit"]})
            else:
                time.sleep(0.1)

    def get_table_name(self, table_name: str) -> TableName:
        return TableName(name=f'"{table_name.upper()}"', alias=f'"{table_name.lower()}"')
    
    def attach(self, objects):
        logger.debug("Attaching objects to MemoryDB...\n")
        super().attach(objects)
        logger.debug("Attached objects to MemoryDB...\n")

    def get_table_definition(self, table_name) -> dict[str, type]:
        try:
            res = self.execute(f"PRAGMA table_info('{table_name.split('.')[-1]}')").to_dicts()
            out = {}
            for row in res:
                out[row['name'].upper()] = self.data_transformer.get_type_from_sql_type(row["type"])
            return out
        except Exception as e:
            return {}
        
    def execute(self, statement, commit=True):
        self.__querys_executed_memory += 1
        execution_id = str(uuid4())
        with self.lock:
            if self.query_queue is None:
                return
            self.query_queue.append({"statement": statement, "commit": commit, "execution_id": execution_id})
        while True:
            with self.lock:
                if execution_id in self.result_queue:
                    result = self.result_queue.pop(execution_id)
                    return result
            time.sleep(0.1)

    def _execute_memory(self, statement_input, commit=True):
        logger.debug('{"running": "MemoryDB.execute", "statement": "' + str(statement_input) + '"}')
        statements = statement_input if isinstance(statement_input, list) else [statement_input]
        cursor = self.__db_connection.cursor()
        try:
            out = None
            for statement in statements:
                statement = self.normalize_statment(statement)
                cursor.execute(statement)
                rows = cursor.fetchall()
                schema = [desc[0] for desc in cursor.description]
                out = pl.DataFrame(rows, schema=schema, orient='row')
                out.columns = [col.lower() for col in out.columns]
                logger.debug('{"running": "MemoryDB.execute", "result": "' + str(out) + '"}')
            return out
        except Exception:
            return None
        finally:
            if commit:
                self.__db_connection.commit()
    
    def snapshot_tables(self):
        out = {}
        for table, schema in self._table_schemas.items():
            qry = self.query.select(from_table=self.get_table_name(table).name, columns=list(schema.keys()), limit=None)
            out[table] = self.execute(qry)
        return out
