from pathlib import Path
from typing import Iterable, Any, overload, Literal, cast

from typing_extensions import LiteralString

try:
    from psycopg import Connection, Cursor
    from psycopg.abc import Query
    from psycopg.errors import InvalidCatalogName
    from psycopg.rows import dict_row
    from psycopg.types.json import Json
except ImportError:
    raise ImportError('Please install psycopg or tracktolib with "pg-sync" to use this module')

from .pg_utils import get_tmp_table_query


def fetch_all(engine: Connection, query: LiteralString, *data) -> list[dict]:
    with engine.cursor(row_factory=dict_row) as cur:
        resp = (cur.execute(query) if not data else cur.execute(query, data)).fetchall()
    return resp


def fetch_count(engine: Connection, table: str, *args, where: str | None = None) -> int | None:
    query = f'SELECT count(*) from {table}'
    if where:
        query = f'{query} WHERE {where}'
    with engine.cursor() as cur:
        count = cur.execute(cast(LiteralString, query), params=args).fetchone()

    return count[0] if count else None


@overload
def fetch_one(engine: Connection, query: Query, *args,
              required: Literal[False]) -> dict | None: ...


@overload
def fetch_one(engine: Connection, query: Query, *args,
              required: Literal[True]) -> dict: ...


@overload
def fetch_one(engine: Connection, query: Query, *args) -> dict | None: ...


def fetch_one(engine: Connection, query: Query, *args,
              required: bool = False) -> dict | None:
    with engine.cursor(row_factory=dict_row) as cur:
        _data = cur.execute(query, args).fetchone()
    if required and not _data:
        raise ValueError('No value found for query')
    return _data


def _parse_value(v):
    if isinstance(v, dict):
        return Json(v)
    return v


def _get_insert_data(table: LiteralString, data: list[dict]) -> tuple[LiteralString, list[tuple[Any, ...]]]:
    keys = data[0].keys()
    _values = ','.join('%s' for _ in range(0, len(keys)))
    query = f"INSERT INTO {table} as t ({','.join(keys)}) VALUES ({_values})"
    return query, [tuple(_parse_value(_x) for _x in x.values()) for x in data]


def insert_many(engine: Connection,
                table: LiteralString,
                data: list[dict]):
    query, _data = _get_insert_data(table, data)
    with engine.cursor() as cur:
        _ = cur.executemany(query, _data)
    engine.commit()


def insert_one(engine: Connection,
               table: LiteralString,
               data: dict):
    query, _data = _get_insert_data(table, [data])
    with engine.cursor() as cur:
        _ = cur.execute(query, _data[0])
    engine.commit()


def drop_db(conn: Connection, db_name: LiteralString):
    try:
        conn.execute(f'DROP DATABASE {db_name}')
    except InvalidCatalogName:
        pass


def clean_tables(engine: Connection, tables: Iterable[LiteralString],
                 cascade: bool = True):
    if not tables:
        return

    _tables = ', '.join(set(tables))
    engine.execute(f'TRUNCATE {_tables} {"" if not cascade else "CASCADE"}')
    engine.commit()


def get_tables(engine: Connection,
               schemas: list[str],
               ignored_tables: Iterable[str] | None = None):
    table_query = """
    SELECT CONCAT_WS('.', schemaname, tablename) AS table
    FROM pg_catalog.pg_tables
    WHERE schemaname = ANY(%s)
    ORDER BY schemaname, tablename
    """
    resp = fetch_all(engine, table_query, schemas)

    # Foreign keys
    _ignored_tables = set(ignored_tables) if ignored_tables else []
    return [x['table'] for x in resp if x['table'] not in _ignored_tables]


def insert_csv(cur: Cursor,
               schema: LiteralString,
               table: LiteralString,
               csv_path: Path,
               query: Query | None = None,
               *,
               exclude_columns: Iterable[str] | None = None,
               delimiter: LiteralString = ',',
               block_size: int = 1000):
    _tmp_table, _tmp_query, _insert_query = get_tmp_table_query(schema, table)
    _columns = csv_path.open().readline()
    _query: Query = query or cast(LiteralString, f"""
    COPY {_tmp_table}({_columns})
    FROM STDIN
    DELIMITER {delimiter!r}
    CSV HEADER
    """)
    cur.execute(_tmp_query)
    if exclude_columns:
        _drop_columns = ','.join(f'DROP COLUMN {x}' for x in exclude_columns)
        _alter_query = f"""
        ALTER TABLE {_tmp_table}
        {_drop_columns}
        """
        cur.execute(cast(LiteralString, _alter_query))
    with csv_path.open() as f:
        with cur.copy(_query) as copy:
            while data := f.read(block_size):
                copy.write(data)
    cur.execute(_insert_query)


def set_seq_max(engine: Connection,
                seq_name: str,
                table_name: str):
    # To avoid potential conflicts
    with engine.cursor() as cursor:
        query = cast(LiteralString, f"SELECT SETVAL(%s, (SELECT MAX(id) + 1 FROM {table_name}))")
        cursor.execute(query, (seq_name,))
    engine.commit()
