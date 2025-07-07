import os
import psycopg2
from autodla.dbs import PostgresDB


def test_postgres_connection():
    os.environ.setdefault('AUTODLA_POSTGRES_USER', 'postgres')
    os.environ.setdefault('AUTODLA_POSTGRES_PASSWORD', 'password')
    os.environ.setdefault('AUTODLA_POSTGRES_HOST', 'localhost')
    os.environ.setdefault('AUTODLA_POSTGRES_DB', 'my_db')
    # Ensure psycopg2 can establish the connection
    conn = psycopg2.connect(
        user=os.environ['AUTODLA_POSTGRES_USER'],
        password=os.environ['AUTODLA_POSTGRES_PASSWORD'],
        host=os.environ['AUTODLA_POSTGRES_HOST'],
        dbname=os.environ['AUTODLA_POSTGRES_DB'],
    )
    with conn.cursor() as cur:
        cur.execute('SELECT 1')
        assert cur.fetchone()[0] == 1
    conn.close()
    # Instantiate PostgresDB to confirm AutoDLA uses same parameters
    db = PostgresDB()
    with db._PostgresDB__pg_connection.cursor() as cur:
        cur.execute('SELECT 1')
        assert cur.fetchone()[0] == 1
    db._PostgresDB__pg_connection.close()
