import psycopg2
import pytest
from psycopg2.extensions import connection, ISOLATION_LEVEL_AUTOCOMMIT
from typing import Iterator

PG_DATABASE = 'test'
PG_USER, PG_PWD, PG_HOST, PG_PORT = 'postgres', 'postgres', 'localhost', 5432
PG_URL = f'postgresql://{PG_USER}:{PG_PWD}@{PG_HOST}:{PG_PORT}'


@pytest.fixture(scope='session')
def engine() -> Iterator[connection]:
    conn = psycopg2.connect(f'{PG_URL}/{PG_DATABASE}')
    yield conn
    conn.close()


@pytest.fixture(scope='session', autouse=True)
def clean_pg_auto():
    from tracktolib.pg_sync import drop_db
    conn = psycopg2.connect(PG_URL)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    drop_db(conn, PG_DATABASE)

    with conn.cursor() as c:
        c.execute(f'CREATE DATABASE {PG_DATABASE}')

    yield

    conn.close()


_TABLES: list[str] = []


@pytest.fixture(scope='function', autouse=True)
def clean_pg(engine):
    global _TABLES
    from tracktolib.pg_sync import get_tables, clean_tables
    if not _TABLES:
        _TABLES = get_tables(engine, schemas=['foo'])
    clean_tables(engine, _TABLES)
    yield

    engine.commit()
    clean_tables(engine, _TABLES)


@pytest.fixture()
def setup_tables(engine, static_dir):
    from tracktolib.pg_sync import exec_req
    sql_file = static_dir / 'setup.sql'
    exec_req(engine, sql_file.read_text())


@pytest.mark.usefixtures('setup_tables')
def test_insert_fetch_many(engine):
    from tracktolib.pg_sync import insert_many, fetch_all

    data = [
        {'foo': 1, 'bar': 'baz'},
        {'foo': 2, 'bar': 'bazz'}
    ]
    insert_many(engine, 'foo.bar', data)
    db_data = fetch_all(engine, 'SELECT foo, bar FROM foo.bar ORDER BY foo')
    assert data == db_data


@pytest.fixture()
def insert_data(engine):
    from tracktolib.pg_sync import insert_many

    data = [
        {'foo': 1, 'bar': 'baz'},
        {'foo': 2, 'bar': 'bazz'}
    ]

    insert_many(engine, 'foo.bar', data)


@pytest.mark.usefixtures('setup_tables', 'insert_data')
def test_fetch_one(engine):
    from tracktolib.pg_sync import fetch_one

    db_data = fetch_one(engine, 'SELECT foo, bar FROM foo.bar ORDER BY foo')
    assert db_data == {'foo': 1, 'bar': 'baz'}


@pytest.mark.usefixtures('setup_tables', 'insert_data')
def test_fetch_count(engine):
    from tracktolib.pg_sync import fetch_count

    assert fetch_count(engine, 'foo.bar') == 2
    assert fetch_count(engine, 'foo.bar', where='foo = 1') == 1
