import psycopg2
import polars as pl
from data_conversion import DataTransformer, DataConversion
from datetime import date, datetime

CONNECTION_URL = "postgresql://my_user:password@localhost/my_db"
CONNECTION_URL = "postgresql://postgres:password@localhost/my_db"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

class PostgresDataTransformer(DataTransformer):
    TYPE_DICT= {
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

class PostgresDB:

    def __init__(self):
        self.__db_connection = psycopg2.connect(CONNECTION_URL)
        self.__data_transformer = PostgresDataTransformer()
    
    @property
    def data_transformer(self):
        return self.__data_transformer
                
    def execute_statement(self, statement, commit=False):
        if statement[-1] != ';':
            statement += ';'
        with self.__db_connection.cursor() as cursor:
            cursor.execute(statement)
            if commit:
                self.__db_connection.commit()
            try:
                rows = cursor.fetchall()
                schema = [desc[0] for desc in cursor.description]
                return pl.DataFrame(rows, schema=schema, orient='row')
            except:
                return None
    
    def ensure_table(self, table_name, schema):
        schema = self.data_transformer.convert_data_schema(schema)
        self.execute_statement(f"DROP TABLE IF EXISTS public.{table_name};", True)
        qry = f'CREATE TABLE public.{table_name} ('
        qry += ", ".join([f"{k} {v}" for k, v in schema.items()])
        qry += ");"
        self.execute_statement(qry, True)
    
    def insert_into_table(self, table_name, schema, data):
        if len(data) < 1:
            return
        self.data_transformer.validate_data_from_schema(schema, data[0])
        qry = f"INSERT INTO public.{table_name} ({', '.join(list(schema.keys()))}) values "
        rows = []
        for row in data:
            values = []
            for k, v in row.items():
                values.append(self.data_transformer.convert_data(v))
            rows.append(f'({", ".join(values)})')
        qry += ', '.join(rows)
        self.execute_statement(qry, True)
