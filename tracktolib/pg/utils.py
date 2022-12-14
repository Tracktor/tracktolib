import csv
import datetime as dt
import functools
import logging
from pathlib import Path
from typing import AsyncIterator
from typing_extensions import LiteralString

try:
    import asyncpg
    from rich.progress import Progress
except ImportError:
    raise ImportError('Please install tracktolib with "pg" to use this module')

from tracktolib.utils import get_chunks
from tracktolib.pg_utils import get_tmp_table_query

logger = logging.Logger('tracktolib-pg')


async def iterate_pg(conn: asyncpg.Connection,
                     query: str,
                     *args,
                     from_offset: int = 0,
                     chunk_size: int = 500,
                     timeout: int | None = None) -> AsyncIterator[list[asyncpg.Record]]:
    async with conn.transaction():
        cur: asyncpg.connection.cursor.Cursor = await conn.cursor(query, *args)
        if from_offset:
            await cur.forward(from_offset, timeout=timeout)
        while data := await cur.fetch(chunk_size, timeout=timeout):
            yield data


_GET_TABLE_INFOS_QUERY = """
SELECT column_name, data_type, character_maximum_length
FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2;
"""


def _str_to_date(value: str) -> dt.date | None:
    return dt.date.fromisoformat(value) if value else None


def _str_max_size(value: str, *, max_size: int | None = None) -> str:
    _value = value.strip()
    if max_size is not None and len(_value) > max_size:
        raise ValueError(f'Got size {len(_value)} but max size is {max_size} for {_value!r}')
    return _value


def _str_to_datetime(value: str) -> dt.datetime | None:
    return dt.datetime.fromisoformat(value) if value else None


def _get_type(data_type: str, char_max_length: int | None):
    match data_type:
        case 'integer':
            return int
        case 'timestamp without time zone':
            return _str_to_datetime
        case 'date':
            return _str_to_date
        case 'boolean':
            return bool

    return functools.partial(_str_max_size, max_size=char_max_length)


async def get_table_infos(conn: asyncpg.Connection, schema: str, table: str):
    infos = await conn.fetch(_GET_TABLE_INFOS_QUERY, schema, table)
    return {
        info['column_name']: _get_type(data_type=info['data_type'],
                                       char_max_length=info['character_maximum_length'])
        for info in infos
    }


def _fmt_record_tuple(record: dict, data_types: dict) -> tuple:
    return tuple(data_types[k.lower()](v) if v not in {'', '-'} and v is not None
                 else None
                 for k, v in record.items())


async def upsert_csv(conn: asyncpg.Connection,
                     csv_path: Path,
                     schema: LiteralString,
                     table: LiteralString,
                     *,
                     chunk_size: int = 5_000,
                     show_progress: bool = False,
                     nb_lines: int | None = None):
    infos = await get_table_infos(conn, schema, table)

    with csv_path.open('r') as f:
        reader = csv.DictReader(f)
        _columns = [x.lower() for x in (reader.fieldnames or [])]
        async with conn.transaction():
            _tmp_table, _tmp_query, _insert_query = get_tmp_table_query(schema, table)
            logger.info(f'Creating tmp table: {_tmp_table!r}')
            await conn.execute(_tmp_query)
            logger.info(f'Inserting data from {csv_path!r} to {_tmp_table!r}')

            with Progress(disable=not show_progress) as progress:
                task1 = progress.add_task("[red]Inserting csv chunks....", total=nb_lines)
                for records in get_chunks(reader, size=chunk_size, as_list=False):
                    _data = [_fmt_record_tuple(x, infos) for x in records]
                    await conn.copy_records_to_table(table_name=_tmp_table,
                                                     columns=_columns,
                                                     records=_data)
                    progress.update(task1, advance=chunk_size)
            logger.info(f'Inserting data from {_tmp_table} to "{schema}.{table}"')
            await conn.execute(_insert_query)
