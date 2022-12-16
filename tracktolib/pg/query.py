import typing
from dataclasses import dataclass, field
from typing import (
    TypeVar, Iterable, Callable, Generic, Iterator, TypeAlias,
    overload, Any)

try:
    import asyncpg
except ImportError:
    raise ImportError('Please install tracktolib with "pg" to use this module')

from tracktolib.utils import fill_dict

K = TypeVar('K', bound=str)
V = TypeVar('V')


def _get_insert_query(table: str, keys: Iterable[K], values: str) -> str:
    _keys = ', '.join(keys)
    return f"INSERT INTO {table} AS t ({_keys}) VALUES ( {values} )"


def _get_returning_query(query: str, returning: Iterable[K]) -> str:
    _returning = ', '.join(returning)
    return f'{query} RETURNING {_returning}'


def _get_on_conflict_query(query: str,
                           keys: Iterable[K],
                           update_keys: Iterable[K] | None,
                           ignore_keys: Iterable[K] | None,
                           constraint: str | None,
                           on_conflict: str | None) -> str:
    if on_conflict:
        return f'{query} {on_conflict}'

    if constraint:
        query = f'{query} ON CONFLICT ON CONSTRAINT {constraint}'
    elif update_keys:
        update_keys_str = ', '.join(sorted(update_keys))
        query = f'{query} ON CONFLICT ({update_keys_str})'
    else:
        raise NotImplementedError('update_keys or constraint must be set')

    _ignore_keys = [*(update_keys or []), *(ignore_keys or [])]
    fields = ', '.join(f'{x} = COALESCE(EXCLUDED.{x}, t.{x})'
                       for x in keys
                       if x not in _ignore_keys)
    if not fields:
        raise ValueError('No fields set')

    return f'{query} DO UPDATE SET {fields}'


ReturningFn = Callable[[Iterable[K] | None, K | None], None]
ConflictFn = Callable[[Iterable[K] | None, Iterable[K] | None, str | None], None]


@dataclass
class PGReturningQuery(Generic[K]):
    returning_ids: Iterable[K] | None = None
    query: str | None = None

    @classmethod
    def load(cls, *,
             keys: Iterable[K] | None = None,
             key: K | None = None,
             query: str | None = None):
        if not query and not keys and not key:
            raise ValueError('Please specify either key or keys')

        _keys = [key] if key else keys
        # self._check_keys(_keys)
        return cls(returning_ids=_keys, query=query)


@dataclass
class PGConflictQuery(Generic[K]):
    keys: Iterable[K] | None = None
    ignore_keys: Iterable[K] | None = None
    query: str | None = None
    constraint: str | None = None

    def __post_init__(self):
        _has_keys = 1 if (self.keys or self.ignore_keys) else 0
        _has_constraint = 1 if self.constraint else 0
        if self.query and sum([_has_keys, _has_constraint]) > 0:
            raise ValueError('Please choose either keys, ignore_keys, constraint OR query')


@dataclass
class PGInsertQuery(Generic[K, V]):
    table: str
    items: list[dict[K, V]]

    returning: PGReturningQuery | None = None
    on_conflict: PGConflictQuery | None = None

    fill: bool = field(kw_only=True, default=False)

    def __post_init__(self):
        if self.fill:
            self.items = fill_dict(self.items, default=None)

    @property
    def keys(self) -> list[K]:
        return sorted(self.items[0].keys())

    def iter_values(self) -> Iterator[tuple]:
        _keys = self.keys
        for _item in self.items:
            yield tuple(_item[k] for k in _keys)

    @property
    def has_conflict(self):
        return self.on_conflict is not None

    @property
    def is_returning(self):
        return self.returning is not None

    @property
    def query(self) -> str:
        _values = ', '.join(f'${i + 1}' for i, _ in enumerate(self.keys))
        _keys = self.keys

        query = _get_insert_query(self.table, _keys, _values)

        # Conflict
        if self.on_conflict:
            query = _get_on_conflict_query(query,
                                           _keys,
                                           self.on_conflict.keys,
                                           self.on_conflict.ignore_keys,
                                           self.on_conflict.constraint,
                                           self.on_conflict.query)

        # Returning
        if self.returning is not None:
            if self.returning.returning_ids is None:
                raise ValueError('No returning ids found')
            if len(self.items) == 1:
                query = _get_returning_query(query, self.returning.returning_ids)
            else:
                raise NotImplementedError('Cannot return value when inserting many.')

        return query

    def _check_keys(self, keys: list[K]):
        invalid_keys = set(keys) - set(self.keys)
        if invalid_keys:
            raise ValueError(f'Invalid keys found: {invalid_keys}')

    @property
    def values(self):
        return self._get_values()

    def _get_values(self):
        return next(self.iter_values()) if len(self.items) == 1 else list(self.iter_values())

    async def run(self, conn: asyncpg.Connection,
                  timeout: float | None = None):
        if len(self.items) == 1:
            await conn.execute(self.query, *self.values, timeout=timeout)
        else:
            await conn.executemany(self.query, self.values, timeout=timeout)

    async def fetch(self, conn: asyncpg.Connection,
                    timeout: float | None = None) -> list[asyncpg.Record]:
        return await conn.fetch(self.query,
                                self._get_values(),
                                timeout=timeout)

    async def fetchrow(self, conn: asyncpg.Connection,
                       timeout: float | None = None) -> asyncpg.Record | None:
        return await conn.fetchrow(self.query,
                                   *self._get_values(),
                                   timeout=timeout)

    async def fetchval(self, conn: asyncpg.Connection,
                       *,
                       column: int = 0,
                       timeout: float | None = None):
        return await conn.fetchval(self.query,
                                   *self._get_values(),
                                   timeout=timeout, column=column)

    async def exists(self, conn: asyncpg.Connection,
                     *,
                     timeout: float | None = None) -> bool:
        return await conn.fetchval(f'SELECT EXISTS({self.query})',
                                   *self._get_values(),
                                   timeout=timeout)


OnConflict: TypeAlias = PGConflictQuery | str


def insert_pg(
        table: str,
        items: list[dict],
        *,
        on_conflict: OnConflict | None = None,
        returning: Iterable[K] | None = None,
        fill: bool = False
) -> PGInsertQuery:
    _on_conflict = PGConflictQuery(query=on_conflict) if isinstance(on_conflict, str) else on_conflict
    _returning = PGReturningQuery.load(keys=returning) if returning else None
    return PGInsertQuery(table, items, fill=fill, on_conflict=_on_conflict,
                         returning=_returning)


async def insert_one(conn: asyncpg.Connection,
                     table: str, item: dict,
                     *,
                     on_conflict: OnConflict | None = None,
                     fill: bool = False):
    query = insert_pg(table=table, items=[item], on_conflict=on_conflict, fill=fill)
    await query.run(conn)


async def insert_many(conn: asyncpg.Connection,
                      table: str, items: list[dict],
                      *,
                      on_conflict: OnConflict | None = None,
                      fill: bool = False):
    query = insert_pg(table=table, items=items, on_conflict=on_conflict, fill=fill)
    await query.run(conn)


@overload
async def insert_returning(conn: asyncpg.Connection,
                           table: str,
                           item: dict,
                           returning: str,
                           on_conflict: OnConflict | None = None,
                           fill: bool = False) -> Any | None: ...


@overload
async def insert_returning(conn: asyncpg.Connection,
                           table: str,
                           item: dict,
                           returning: list[str],
                           on_conflict: OnConflict | None = None,
                           fill: bool = False) -> asyncpg.Record | None: ...


async def insert_returning(conn: asyncpg.Connection,
                           table: str,
                           item: dict,
                           returning: list[str] | str,
                           on_conflict: OnConflict | None = None,
                           fill: bool = False) -> asyncpg.Record | Any | None:
    returning_values = [returning] if isinstance(returning, str) else returning
    query = insert_pg(table=table, items=[item], on_conflict=on_conflict, fill=fill,
                      returning=returning_values)
    fn = conn.fetchval if len(returning_values) == 1 else conn.fetchrow

    return await fn(query.query, *query.values)


async def fetch_count(conn: asyncpg.Connection, query: str, *args) -> int:
    c = await conn.fetchval(f"SELECT COUNT(*) FROM ({query}) t", *args)
    return typing.cast(int, c)


def Conflict(
        keys: Iterable[K],
        ignore_keys: Iterable[K] | None = None,
) -> PGConflictQuery:
    return PGConflictQuery(keys=keys, ignore_keys=ignore_keys)
