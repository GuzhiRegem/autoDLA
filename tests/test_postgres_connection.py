import os
import uuid
from datetime import datetime

import psycopg2

from autodla import Object, primary_key
from autodla.dbs import PostgresDB


def test_postgres_connection(monkeypatch):
    os.environ.setdefault("AUTODLA_POSTGRES_USER", "postgres")
    os.environ.setdefault("AUTODLA_POSTGRES_PASSWORD", "password")
    os.environ.setdefault("AUTODLA_POSTGRES_HOST", "localhost")
    os.environ.setdefault("AUTODLA_POSTGRES_DB", "my_db")

    # verify raw connection works
    conn = psycopg2.connect(
        user=os.environ["AUTODLA_POSTGRES_USER"],
        password=os.environ["AUTODLA_POSTGRES_PASSWORD"],
        host=os.environ["AUTODLA_POSTGRES_HOST"],
        dbname=os.environ["AUTODLA_POSTGRES_DB"],
    )
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
        assert cur.fetchone()[0] == 1
    conn.close()

    from autodla.engine import object as engine_object
    from autodla.engine.lambda_conversion import lambda_to_sql

    original_init = engine_object.Table.__init__
    original_update = engine_object.Table.update

    def init(self, table_name: str, schema: dict, db: PostgresDB | None = None):
        self.table_name = table_name
        self.schema = schema
        if db:
            engine_object.Table.set_db(self, db)

    def tbl_update(self, l_func, data):
        alias = "".join(self.table_name.split("."))
        where_st = lambda_to_sql(self.schema, l_func, self.db.data_transformer, alias=alias)
        update_data = {f"{key}": value for key, value in data.items()}
        qry = self.db.query.update(self.table_name, where=where_st, values=update_data)
        return self.db.execute(qry)

    monkeypatch.setattr(engine_object.Table, "__init__", init)
    monkeypatch.setattr(engine_object.Table, "update", tbl_update)

    class Person(Object):
        id: primary_key = primary_key.auto_increment()
        name: str

    db = PostgresDB(sync_interval=3600)
    db.attach([Person])

    pg_conn = db._PostgresDB__pg_connection
    pg_query = db._PostgresDB__pg_query
    pg_dt = db._PostgresDB__pg_dt

    schema = Person._Object__table.schema.copy()
    schema["DLA_is_current"]["type"] = int
    schema["DLA_is_active"]["type"] = int
    with pg_conn.cursor() as cur:
        cur.execute(pg_query.drop_table("person", if_exists=True))
        cur.execute(pg_query.create_table("person", pg_dt.convert_data_schema(schema)))
    pg_conn.commit()

    try:
        p1 = Person.new(name="Alice")
        db.synchronize()
        with pg_conn.cursor() as cur:
            cur.execute("SELECT name FROM person")
            rows = cur.fetchall()
        assert rows == [("Alice",)]

        with pg_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO person (id, name, DLA_object_id, DLA_modified_at, DLA_operation, DLA_modified_by, DLA_is_current, DLA_is_active)"
                " VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    str(uuid.uuid4()),
                    "Bob",
                    str(uuid.uuid4()),
                    datetime.now(),
                    "INSERT",
                    "SYSTEM",
                    1,
                    1,
                ),
            )
        pg_conn.commit()

        db.synchronize()
        names = sorted([p.name for p in Person.all(limit=None)])
        assert names == ["Alice", "Bob"]

        p1.update(name="Alice2")
        db.synchronize()

        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT name, DLA_operation, DLA_is_current FROM person ORDER BY DLA_modified_at"
            )
            rows = cur.fetchall()

        assert ("Alice", "INSERT", 0) in rows
        assert ("Bob", "INSERT", 1) in rows
        assert ("Alice2", "UPDATE", 1) in rows
    finally:
        with pg_conn.cursor() as cur:
            cur.execute(pg_query.drop_table("person", if_exists=True))
        pg_conn.commit()

    db._PostgresDB__pg_connection.close()
