import json
from collections.abc import Iterator
from typing import LiteralString

import asyncpg
import psycopg
import pytest

PG_DATABASE = "test"
PG_USER, PG_PWD, PG_HOST, PG_PORT = "postgres", "postgres", "localhost", 5432
PG_URL = f"postgresql://{PG_USER}:{PG_PWD}@{PG_HOST}:{PG_PORT}"


@pytest.fixture(scope="session")
def pg_url():
    return f"{PG_URL}/{PG_DATABASE}"


@pytest.fixture(scope="session")
def engine(pg_url) -> Iterator[psycopg.Connection]:
    with psycopg.connect(pg_url) as conn:
        yield conn


@pytest.fixture(scope="session", autouse=True)
def clean_pg_auto():
    from tracktolib.pg_sync import drop_db

    with psycopg.connect(PG_URL, autocommit=True) as conn:
        drop_db(conn, PG_DATABASE)
        conn.execute(f"CREATE DATABASE {PG_DATABASE}")

        yield


_TABLES: list[LiteralString] = []


@pytest.fixture(scope="function", autouse=True)
def clean_tables(engine):
    global _TABLES
    from tracktolib.pg_sync import clean_tables, get_tables

    if not _TABLES:
        _TABLES = get_tables(engine, schemas=["foo"])
    clean_tables(engine, _TABLES)
    yield

    engine.commit()
    clean_tables(engine, _TABLES)


@pytest.fixture()
def setup_tables(engine, static_dir):
    sql_file = static_dir / "setup.sql"
    engine.execute(sql_file.read_text())
    engine.commit()


async def init_connection(conn: asyncpg.Connection):
    await conn.set_type_codec("jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog")
    await conn.set_type_codec("json", encoder=json.dumps, decoder=json.loads, schema="pg_catalog")
    return conn


@pytest.fixture(scope="function")
async def aengine(pg_url) -> asyncpg.Connection:  # type: ignore
    conn = await asyncpg.connect(pg_url)
    await init_connection(conn)
    yield conn  # type: ignore
    await conn.close()


@pytest.fixture(scope="function")
async def apool(pg_url) -> asyncpg.pool.Pool:  # type: ignore
    pool = await asyncpg.create_pool(pg_url, init=init_connection)
    yield pool  # type: ignore
    await pool.close()
