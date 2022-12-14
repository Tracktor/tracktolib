import asyncio
from typing import Iterator
from typing_extensions import LiteralString

import asyncpg
import psycopg
import pytest

PG_DATABASE = 'test'
PG_USER, PG_PWD, PG_HOST, PG_PORT = 'postgres', 'postgres', 'localhost', 5432
PG_URL = f'postgresql://{PG_USER}:{PG_PWD}@{PG_HOST}:{PG_PORT}'


@pytest.fixture(scope='session')
def pg_url():
    return f'{PG_URL}/{PG_DATABASE}'


@pytest.fixture(scope='session')
def engine(pg_url) -> Iterator[psycopg.Connection]:
    conn = psycopg.connect(pg_url)
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture(scope='session', autouse=True)
def clean_pg_auto():
    from tracktolib.pg_sync import drop_db
    conn = psycopg.connect(PG_URL, autocommit=True)
    drop_db(conn, PG_DATABASE)
    conn.execute(f'CREATE DATABASE {PG_DATABASE}')

    yield

    conn.close()


_TABLES: list[LiteralString] = []


@pytest.fixture(scope='function', autouse=True)
def clean_tables(engine):
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


@pytest.fixture(scope='session')
def aengine(loop, pg_url) -> asyncpg.Connection:
    conn = loop.run_until_complete(asyncpg.connect(pg_url))
    yield conn
    loop.run_until_complete(asyncio.wait_for(conn.close(), timeout=1))
