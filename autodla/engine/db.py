import polars as pl
from ..engine.data_conversion import DataTransformer
from ..engine.query_builder import QueryBuilder

class DB_Connection:
    __data_transformer : DataTransformer
    __query : QueryBuilder
    __classes = {}

    def __init__(self, data_transformer, query):
        self.__data_transformer = data_transformer
        self.__query = query

    @property
    def query(self):
        return self.__query

    @property
    def data_transformer(self):
        return self.__data_transformer
    
    def get_table_definition(self, table_name) -> dict[str, type]:
        pass
    
    def attach(self, objects):
        ordered_objects = []
        pending = objects[:]
        while True:
            if pending == []:
                break
            tmp = pending[:]
            for obj in tmp:
                schema = obj.get_types()
                class_dependencies = []
                for i in schema.values():
                    if 'depends' in i:
                        class_dependencies.append(i.get('depends'))
                if all([dep in ordered_objects for dep in class_dependencies]):
                    ordered_objects.append(obj)
                    pending.remove(obj)
        for obj in ordered_objects:
            self.__classes[obj.__name__] = obj
            obj.set_db(self)

    def execute(self, query: str) -> pl.DataFrame:
        pass

    def normalize_statment(self, statement: str) -> str:
        if not isinstance(statement, str):
            statement = str(statement)
        statement = statement.lstrip().rstrip()
        if statement[-1] != ";":
            statement += ";"
        return statement
    
    def ensure_table(self, table_name, schema):
        data_schema = {k.upper(): v["type"] for k, v in schema.items()}
        current_data_schema = self.get_table_definition(table_name)
        if all([self.data_transformer.check_type_compatibilty(data_schema.get(k), current_data_schema.get(k)) for k in list(set(data_schema.keys()).union(set(data_schema.keys())))]):
            return
        print(data_schema)
        print(current_data_schema)
        if data_schema == current_data_schema:
            return
        schema = self.data_transformer.convert_data_schema(data_schema)
        self.execute(self.query.drop_table(table_name, if_exists=True))
        qry = self.query.create_table(table_name, schema)
        self.execute(qry)