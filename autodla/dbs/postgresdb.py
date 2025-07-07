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
import time
import threading
from .memorydb import MemoryDB

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
    def select(self, from_table: str, columns: List[str], where: str = None, limit: int = 10, order_by: str = None, group_by: list[str] = None, offset: int = None) -> pl.DataFrame:
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

class PostgresDB(MemoryDB):

    def __init__(self, connection_url=CONNECTION_URL, sync_interval: int = 5):
        super().__init__()
        self.__pg_connection = psycopg2.connect(connection_url)
        self.__pg_dt = PostgresDataTransformer()
        self.__pg_query = PostgresQueryBuilder(self.__pg_dt)
        self.__last_snapshot = self._snapshot_tables()
        self.__last_sync = time.time()
        self.__sync_interval = sync_interval
        self.__sync_thread = threading.Thread(target=self.__sync_loop, daemon=True)
        self.__sync_thread.start()
    
    def get_table_definition(self, table_name) -> dict[str, type]:
        if "." in table_name:
            table_name = table_name.split(".")[-1]
        qry = self.__pg_query.select(
            from_table='INFORMATION_SCHEMA.COLUMNS',
            columns=["column_name", "data_type"],
            limit=None,
            where=f"table_name = '{table_name}'"
        )
        with self.__pg_connection.cursor() as cursor:
            cursor.execute(qry)
            res = cursor.fetchall()
            schema_cols = [d[0] for d in cursor.description]
            res = [dict(zip(schema_cols, r)) for r in res]
        conversion_dict = {
            "boolean": "bool",
            "timestamp without time zone": "timestamp"
        }
        out = {}
        for row in res:
            if row['data_type'] in conversion_dict:
                row['data_type'] = conversion_dict[row['data_type']]
            out[row['column_name'].upper()] = self.__pg_dt.get_type_from_sql_type(row["data_type"])
        return out
                
    def execute(self, statement, commit=True):
        if time.time() - self.__last_sync > self.__sync_interval:
            self.synchronize()
        return super().execute(statement, commit)

    def _snapshot_tables(self) -> dict:
        cursor = self._MemoryDB__db_connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [r[0] for r in cursor.fetchall()]
        snap = {}
        for t in tables:
            df = super().execute(f"SELECT * FROM {t}")
            snap[t] = df if df is not None else pl.DataFrame()
        return snap

    def _compute_delta(self, old: dict, new: dict) -> dict:
        delta = {}
        for t, df_new in new.items():
            df_old = old.get(t)
            if df_old is None or len(df_old) == 0:
                delta[t] = df_new
            else:
                diff = df_new.join(df_old, on=df_new.columns, how='anti')
                delta[t] = diff
        return delta

    def _sync_to_postgres(self, delta: dict):
        with self.__pg_connection.cursor() as cursor:
            for table, df in delta.items():
                if df is None or len(df) == 0:
                    continue
                for row in df.to_dicts():
                    obj_id = row.get("DLA_object_id")
                    if obj_id is not None:
                        check_q = self.__pg_query.select(
                            from_table=table,
                            columns=["DLA_object_id"],
                            where=f"DLA_object_id = '{obj_id}'",
                            limit=1,
                        )
                        cursor.execute(check_q)
                        if cursor.fetchone():
                            update_vals = {k: v for k, v in row.items() if k != "DLA_object_id"}
                            qry = self.__pg_query.update(
                                table,
                                values=update_vals,
                                where=f"DLA_object_id = '{obj_id}'",
                            )
                            cursor.execute(qry)
                            continue
                    qry = self.__pg_query.insert(table, [row])
                    cursor.execute(qry)
        self.__pg_connection.commit()

    def _reload_memory(self):
        with self.__pg_connection.cursor() as cursor:
            m_cursor = self._MemoryDB__db_connection.cursor()
            m_cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [r[0] for r in m_cursor.fetchall()]
            for t in tables:
                super().execute(f"DELETE FROM {t}")
                cursor.execute(f"SELECT * FROM {t}")
                rows = cursor.fetchall()
                if not rows:
                    continue
                columns = [d[0] for d in cursor.description]
                values = [dict(zip(columns, r)) for r in rows]
                qry = self.query.insert(t, values)
                super().execute(qry)

    def synchronize(self):
        new_snap = self._snapshot_tables()
        delta = self._compute_delta(self.__last_snapshot, new_snap)
        self._sync_to_postgres(delta)
        self._reload_memory()
        self.__last_snapshot = self._snapshot_tables()
        self.__last_sync = time.time()

    def __sync_loop(self):
        while True:
            time.sleep(self.__sync_interval)
            try:
                self.synchronize()
            except Exception:
                pass

