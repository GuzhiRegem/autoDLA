import psycopg2
import polars as pl
from autodla.engine.data_conversion import DataTransformer, DataConversion
from autodla.engine.db import DB_Connection, TableName
from autodla.engine.object import primary_key
from autodla.engine.query_builder import QueryBuilder
from datetime import date, datetime, timedelta
from typing import List, Literal, Optional
from autodla.utils.logger import logger
from uuid import UUID
import os
import asyncio
from autodla.utils.watchdog import Watchdog
from autodla.dbs.memorydb import MemoryDB
from autodla.utils.df_tools import df_comparator, ensure_dtype_equality

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
DB_FLUSH_TIME = 60
if "AUTODLA_DB_FLUSH_TIME" in os.environ:
    DB_FLUSH_TIME = int(os.environ.get("AUTODLA_DB_FLUSH_TIME"))

CONNECTION_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_URL}/{POSTGRES_DB}"

class PostgresQueryBuilder(QueryBuilder):
    def select(self, from_table: str, columns: List[str], where: str = None, limit: int = 10, order_by: str = None, group_by: list[str] = None, offset: int = None) -> pl.DataFrame:
        qry = "SELECT " + ", ".join(columns) + " FROM public." + from_table
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
        qry = "INSERT INTO public." + into_table + " (" + ", ".join(values[0].keys()) + ") VALUES "
        qry += ", ".join([f"({', '.join([self._data_transformer.convert_data(v) for v in d.values()])})" for d in values])
        return qry

    def update(self, table: str, values: dict, where: str) -> None:
        qry = f"UPDATE public.{table} SET {', '.join([f'{k.upper()} = {self._data_transformer.convert_data(v)}' for k, v in values.items()])} WHERE {where}"
        return qry

    def delete(self, table: str, where: str) -> None:
        qry = f"DELETE FROM public.{table} WHERE {where}"
        return qry

    def create_table(self, table_name: str, schema: dict, if_exists = False) -> None:
        if_exists_st = "IF EXISTS" if if_exists else ""
        items = [f'{k} {v}' for k, v in schema.items()]
        qry = f"CREATE TABLE {if_exists_st} public.{table_name} ({', '.join(items)});"
        return qry

    def drop_table(self, table_name: str, if_exists = False) -> None:
        if_exists_st = "IF EXISTS" if if_exists else ""
        qry = f"DROP TABLE {if_exists_st} public.{table_name};"
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

    def __init__(self, connection_url=CONNECTION_URL):
        self.__querys_executed_postgres = 0
        super().__init__()
        self.__connection_url = connection_url
        self.__pg_dt = PostgresDataTransformer()
        self.__pg_query = PostgresQueryBuilder(self.__pg_dt)
        self.__last_sync = datetime.now()
        self.watchdog = Watchdog(DB_FLUSH_TIME, self.sync)
        self.__atached = False
        self.__mid_sync = False
        self.connect()
    
    def connect(self):
        """
        Connects to the PostgreSQL database.
        """
        try:
            self.__pg_connection = psycopg2.connect(self.__connection_url)
            self._execute("update pg_cast set castcontext='a' where casttarget = 'boolean'::regtype;")
            return True
        except psycopg2.OperationalError as e:
            self.__pg_connection = None
            logger.error(f"Failed to connect to PostgreSQL: {e}")
        return False

    @property
    def usage_metrics(self):
        """
        Returns the usage metrics of the MemoryDB connection.
        This is a placeholder method and should be implemented in subclasses.
        """
        return {"postgres": self.__querys_executed_postgres, **super().usage_metrics}

    async def poll_watchdog(self):
        while True:
            await asyncio.sleep(1)
            self.watchdog.check()

    def execute(self, statement, commit=True):
        if self.__atached:
            self.watchdog.reset()
            if not self.__mid_sync:
                datetime_diff = (datetime.now() - self.__last_sync).total_seconds()
                if datetime_diff > DB_FLUSH_TIME:
                    self.sync()
        res = super().execute(statement, commit)
        return res
    
    def attach(self, objects):
        if self.__pg_connection is None:
            if not self.connect():
                logger.error("Failed to connect to PostgreSQL, cannot execute query.")
                return None
        logger.debug("Attaching objects to PostgreSQL...\n")
        super().attach(objects)
        for table in self._tables:
            self.ensure_table(table, self._table_schemas[table], current_data_schema=self._get_table_definition(table))
        self.__atached = True
        self.sync()
        logger.debug("Attached objects to PostgreSQL...\n")

    def sync(self):
        if self.__pg_connection is None:
            if not self.connect():
                logger.error("Failed to connect to PostgreSQL, cannot execute query.")
                return None
        if self.__mid_sync:
            logger.debug("Sync already in progress, skipping...")
            return
        logger.debug("Syncing to PostgreSQL...")
        self.__mid_sync = True
        self.__last_sync = datetime.now()

        external_snapshot: dict[str, pl.DataFrame] = self.snapshot_tables_external()
        internal_snapshot: dict[str, pl.DataFrame] = self.snapshot_tables()
        
        final_snapshot = {}
        for table_name in internal_snapshot:
            in_df = internal_snapshot[table_name]
            ex_df = external_snapshot[table_name]

            if in_df.is_empty() and ex_df.is_empty():
                continue
            if in_df.is_empty():
                final_snapshot[table_name] = ex_df
                continue
            if ex_df.is_empty():
                final_snapshot[table_name] = in_df
                continue
                
            in_df, ex_df = ensure_dtype_equality(in_df, ex_df)

            final_df = pl.concat([in_df, ex_df], how="vertical").sort("dla_modified_at", descending=True)
            final_df = final_df.group_by("dla_object_id", maintain_order=True).first()

            final_snapshot[table_name] = final_df
        
        if final_snapshot is None or len(final_snapshot) == 0:
            logger.debug("No changes to sync.")
            self.__mid_sync = False
            return

        internal_querys = []
        external_querys = []
        for table_name, df in final_snapshot.items():

            def get_change_querys(
                    snapshot: dict[str, pl.DataFrame],
                    query_executor
                ):
                out = []
                add_df = df_comparator(df, snapshot, method="insert")
                update_df = df_comparator(df, snapshot, method="update")
                logger.debug(f"add_df: {query_executor.__class__} {add_df}")
                logger.debug(f"update_df: {query_executor.__class__} {update_df}")
                if not add_df.is_empty():
                    out.append(query_executor.insert(into_table=table_name, values=add_df.to_dicts()))
                if not update_df.is_empty():
                    for row in update_df.to_dicts():
                        where_clause = f"dla_object_id = '{row['dla_object_id']}'"
                        out.append(query_executor.update(table=table_name, values=row, where=where_clause))
                return out
            
            internal_querys += get_change_querys(internal_snapshot[table_name], self.query)
            external_querys += get_change_querys(external_snapshot[table_name], self.__pg_query)

        if len(internal_querys) > 0:
            self.execute(internal_querys)
        if len(external_querys) > 0:
            self._execute(external_querys)
        logger.debug("Syncing to PostgreSQL completed.")
        self.__mid_sync = False

    def exit(self):
        self.sync()
        return super().exit()
        
    def snapshot_tables_external(self):
        out = {}
        for table, schema in self._table_schemas.items():
            qry = self.__pg_query.select(from_table=f"{table} t", columns=[f"t.{i}" for i in schema], limit=None)
            out[table] = self._execute([qry], commit=False)
        return out

    def _get_table_definition(self, table_name) -> dict[str, type]:
        if "." in table_name:
            table_name = table_name.split(".")[-1]
        res = self._execute([self.query.select(
            from_table='INFORMATION_SCHEMA.COLUMNS',
            columns=["column_name", "data_type"],
            limit=None,
            where=f"table_name = '{table_name}'"
        )], commit=False).to_dicts()
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
    
    def _execute(self, statements : list, commit=True):
        self.__querys_executed_postgres += 1
        with self.__pg_connection.cursor() as cursor:
            try:
                out = None
                for statement in statements:
                    statement = self.normalize_statment(statement)
                    logger.debug('{"running": "PostgresDB._execute", "statement": "' + statement + '"}')
                    cursor.execute(statement)
                    rows = cursor.fetchall()
                    schema = [desc[0] for desc in cursor.description]
                    out = pl.DataFrame(rows, schema=schema, orient='row')
                    out.columns = [col.lower() for col in out.columns]
                    logger.debug('{"running": "PostgresDB._execute", "result": "' + str(out) + '"}')
                return out
            except:
                return None
            finally:
                if commit:
                    self.__pg_connection.commit()