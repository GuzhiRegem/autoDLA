import psycopg2
import polars as pl
from engine.data_conversion import DataTransformer, DataConversion
from engine.db import DB_Connection
from datetime import date, datetime
from pypika import PostgreSQLQuery
from engine.object import primary_key

CONNECTION_URL = "postgresql://my_user:password@localhost/my_db"
CONNECTION_URL = "postgresql://postgres:password@localhost/my_db"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

class PostgresDataTransformer(DataTransformer):
    TYPE_DICT= {
        primary_key: DataConversion("UUID", lambda x: f"'{x}'"),
        type(None): DataConversion('', lambda x: "NULL"),
        int: DataConversion('INTEGER'),
        float: DataConversion("REAL"),
        str: DataConversion("TEXT", lambda x: f"'{x}'"),
        bool: DataConversion("BOOL", lambda x: {True: "TRUE", False: "FALSE"}[x]),
        date: DataConversion("DATE", lambda x: f"'{x.year}-{x.month}-{x.day}'"),
        datetime: DataConversion("TIMESTAMP", lambda x: f"'{x.strftime(DATETIME_FORMAT)}'")
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

class PostgresDB(DB_Connection):

    def __init__(self):
        self.__db_connection = psycopg2.connect(CONNECTION_URL)
        super().__init__(PostgresDataTransformer(), PostgreSQLQuery)
                
    def execute(self, statement):
        statement = self.normalize_statment(statement)
        super().execute(statement)
        with self.__db_connection.cursor() as cursor:
            cursor.execute(statement)
            self.__db_connection.commit()
            try:
                rows = cursor.fetchall()
                schema = [desc[0] for desc in cursor.description]
                return pl.DataFrame(rows, schema=schema, orient='row')
            except:
                return None