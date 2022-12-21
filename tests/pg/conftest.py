import asyncio
from typing import Iterator

import asyncpg
import psycopg
import pytest
from typing_extensions import LiteralString

PG_DATABASE = 'test'
PG_USER, PG_PWD, PG_HOST, PG_PORT = 'postgres', 'postgres', 'localhost', 5432
PG_URL = f'postgresql://{PG_USER}:{PG_PWD}@{PG_HOST}:{PG_PORT}'


@pytest.fixture(scope='session')
def pg_url():
    return f'{PG_URL}/{PG_DATABASE}'


@pytest.fixture(scope='session')
def engine(pg_url) -> Iterator[psycopg.Connection]:
    with psycopg.connect(pg_url) as conn:
        yield conn


@pytest.fixture(scope='session', autouse=True)
def clean_pg_auto():
    from tracktolib.pg_sync import drop_db
    with psycopg.connect(PG_URL, autocommit=True) as conn:
        drop_db(conn, PG_DATABASE)
        conn.execute(f'CREATE DATABASE {PG_DATABASE}')

        yield


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
    sql_file = static_dir / 'setup.sql'
    engine.execute(sql_file.read_text())
    engine.commit()


@pytest.fixture(scope='session')
def aengine(loop, pg_url) -> asyncpg.Connection:
    conn = loop.run_until_complete(asyncpg.connect(pg_url))
    yield conn
    loop.run_until_complete(asyncio.wait_for(conn.close(), timeout=1))


@pytest.fixture(scope='session')
def apool(loop, pg_url) -> Iterator[asyncpg.pool.Pool]:
    pool = loop.run_until_complete(asyncpg.create_pool(pg_url, loop=loop))
    yield pool
    loop.run_until_complete(asyncio.wait_for(pool.close(), timeout=1))
