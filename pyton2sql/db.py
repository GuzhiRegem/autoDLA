import psycopg2
import polars as pl
from data_conversion import DataTransformer, DataConversion
from datetime import date, datetime

CONNECTION_URL = "postgresql://postgres:password@localhost/my_db"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

class PostgresDataTransformer(DataTransformer):
    TYPE_DICT= {
        int: DataConversion('INTEGER'),
        float: DataConversion("REAL"),
        str: DataConversion("TEXT", lambda x: f"'{x}'"),
        bool: DataConversion("BOOL", lambda x: {True: "TRUE", False: "FALSE"}[x]),
        date: DataConversion("DATE", lambda x: f"'{x.year}-{x.month}-{x.day}'"),
        datetime: DataConversion("TIMESTAMP", lambda x: f"'{x.strftime(DATETIME_FORMAT)}'")
    }

class PostgresDB:

    def __init__(self):
        self.__db_connection = psycopg2.connect(CONNECTION_URL)
        self.__data_transformer = PostgresDataTransformer()
    
    @property
    def data_transformer(self):
        return self.__data_transformer
    
    def to_str(self, value):
        match type(value).__name__:
            case 'str':
                return f"'{value}'"
            case 'datetime':
                return f"'{value.strftime(DATETIME_FORMAT)}'"
            case _:
                return str(value)
                

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
