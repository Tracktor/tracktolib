import typing
from dataclasses import dataclass, field
from typing import TypeVar, Iterable, Callable, Generic, Iterator, TypeAlias, overload, Any, Literal

from ..pg_utils import get_conflict_query

try:
    import asyncpg
except ImportError:
    raise ImportError('Please install tracktolib with "pg" to use this module')

from tracktolib.utils import fill_dict

K = TypeVar("K", bound=str)
V = TypeVar("V")


def _get_insert_query(table: str, columns: Iterable[K], values: str) -> str:
    _columns = ", ".join(columns)
    return f"INSERT INTO {table} AS t ({_columns}) VALUES ({values})"


def _get_returning_query(query: str, returning: Iterable[K]) -> str:
    _returning = ", ".join(returning)
    return f"{query} RETURNING {_returning}"


def _get_on_conflict_query(
    query: str,
    columns: Iterable[K],
    update_columns: Iterable[K] | None,
    ignore_columns: Iterable[K] | None,
    constraint: K | None,
    on_conflict: K | None,
    where: K | None,
    merge_columns: Iterable[K] | None,
) -> str:
    _on_conflict = get_conflict_query(
        columns=columns,
        update_columns=update_columns,
        ignore_columns=ignore_columns,
        constraint=constraint,
        on_conflict=on_conflict,
        where=where,
        merge_columns=merge_columns,
    )
    return f"{query} {_on_conflict}"


ReturningFn = Callable[[Iterable[K] | None, K | None], None]
ConflictFn = Callable[[Iterable[K] | None, Iterable[K] | None, str | None], None]

_Connection = asyncpg.Connection | asyncpg.pool.Pool


@dataclass
class PGReturningQuery(Generic[K]):
    returning_ids: Iterable[K] | None = None
    query: str | None = None

    @classmethod
    def load(cls, *, keys: Iterable[K] | None = None, key: K | None = None, query: str | None = None):
        if not query and not keys and not key:
            raise ValueError("Please specify either key or keys")

        _keys = [key] if key else keys
        # self._check_keys(_keys)
        return cls(returning_ids=_keys, query=query)


@dataclass
class PGConflictQuery(Generic[K]):
    keys: Iterable[K] | None = None
    ignore_keys: Iterable[K] | None = None
    query: str | None = None
    constraint: str | None = None
    where: str | None = None
    """JSONB keys to merge (like jsonb1 || newjsonb2)"""
    merge_keys: Iterable[K] | None = None

    def __post_init__(self):
        _has_keys = 1 if (self.keys or self.ignore_keys) else 0
        _has_constraint = 1 if self.constraint else 0
        if self.query and sum([_has_keys, _has_constraint]) > 0:
            raise ValueError("Please choose either keys, ignore_keys, constraint OR query")


@dataclass
class PGQuery(Generic[K, V]):
    table: str
    items: list[dict[K, V]]

    fill: bool = field(kw_only=True, default=False)
    quote_columns: bool = field(kw_only=True, default=False)

    def __post_init__(self):
        if self.fill:
            self.items = fill_dict(self.items, default=None)

    @property
    def keys(self) -> list[K]:
        return sorted(self.items[0].keys())

    @property
    def columns(self):
        return [f'"{x}"' for x in self.keys] if self.quote_columns else self.keys

    def iter_values(self) -> Iterator[tuple]:
        _keys = self.keys
        for _item in self.items:
            yield tuple(_item[k] for k in _keys)

    @property
    def query(self) -> str:
        raise NotImplementedError()

    def _check_keys(self, keys: list[K]):
        invalid_keys = set(keys) - set(self.keys)
        if invalid_keys:
            _invalid_keys = ", ".join(f"{x!r}" for x in invalid_keys)
            raise ValueError(f"Invalid key(s) found: {_invalid_keys}")

    @property
    def values(self):
        return self._get_values()

    def _get_values(self):
        return next(self.iter_values()) if len(self.items) == 1 else list(self.iter_values())

    async def run(self, conn: _Connection, timeout: float | None = None):
        if len(self.items) == 1:
            await conn.execute(self.query, *self.values, timeout=timeout)  # type: ignore
        else:
            await conn.executemany(self.query, self.values, timeout=timeout)  # type: ignore

    async def fetch(self, conn: _Connection, timeout: float | None = None) -> list[asyncpg.Record]:
        return await conn.fetch(self.query, self._get_values(), timeout=timeout)

    async def fetchrow(self, conn: _Connection, timeout: float | None = None) -> asyncpg.Record | None:
        return await conn.fetchrow(self.query, *self._get_values(), timeout=timeout)

    async def fetchval(self, conn: _Connection, *, column: int = 0, timeout: float | None = None):
        return await conn.fetchval(self.query, *self._get_values(), timeout=timeout, column=column)

    async def exists(self, conn: _Connection, *, timeout: float | None = None) -> bool:
        _exists = await conn.fetchval(f"SELECT EXISTS({self.query})", *self._get_values(), timeout=timeout)
        return _exists or False


@dataclass
class PGInsertQuery(PGQuery):
    returning: PGReturningQuery | None = None
    on_conflict: PGConflictQuery | None = None

    @property
    def has_conflict(self):
        return self.on_conflict is not None

    @property
    def is_returning(self):
        return self.returning is not None

    @property
    def query(self) -> str:
        _columns = self.columns
        _values = ", ".join(f"${i + 1}" for i, _ in enumerate(_columns))

        query = _get_insert_query(self.table, _columns, _values)

        # Conflict
        if self.on_conflict:
            query = _get_on_conflict_query(
                query,
                _columns,
                self.on_conflict.keys,
                self.on_conflict.ignore_keys,
                self.on_conflict.constraint,
                self.on_conflict.query,
                self.on_conflict.where,
                self.on_conflict.merge_keys,
            )

        # Returning
        if self.returning is not None:
            if self.returning.returning_ids is None:
                raise ValueError("No returning ids found")
            if len(self.items) == 1:
                query = _get_returning_query(query, self.returning.returning_ids)
            else:
                raise NotImplementedError("Cannot return value when inserting many.")

        return query


def get_update_fields(
    item: dict,
    keys: list[str],
    *,
    start_from: int = 0,
    ignore_keys: list[str] | None = None,
    quote_columns: bool = False,
    merge_keys: list[str] | None = None,
) -> tuple[str, list]:
    values, fields, where_values = [], [], []
    counter = 0
    _merge_keys = set(merge_keys or [])
    _ignore_keys = ignore_keys or []

    for k in keys:
        v = item[k]
        if k in _ignore_keys:
            where_values.append(v)
            continue
        values.append(v)
        _col = f'"{k}"' if quote_columns else k
        _counter = counter + start_from + 1
        fields.append(
            f"{_col} = ${_counter}"
            if k not in _merge_keys
            else f"{_col} = COALESCE(t.{_col}, JSONB_BUILD_OBJECT()) || " f"${_counter}"
        )
        counter += 1
    return ",\n".join(fields), values + where_values


@dataclass
class PGUpdateQuery(PGQuery):
    """Value to start the arguments from:
    For instance, with a value of 10, the first argument will be $11
    """

    start_from: int | None = None
    """Keys to use for the WHERE clause. Theses fields will not be updated"""
    where_keys: list[str] | None = None
    """Where condition for the update query"""
    where: str | None = None
    returning: str | list[str] | None = None
    """If True, the query will return all the updated fields"""
    return_keys: bool = False
    """Values to update using merge (like {}::jsonb || {}::jsonb)"""
    merge_keys: list[str] | None = None

    _update_fields: str | None = field(init=False, default=None)
    _values: list | None = field(init=False, default=None)

    def __post_init__(self):
        if self.where_keys:
            self._check_keys(self.where_keys)
            # Ordering the keys
            self.where_keys = [k for k in self.keys if k in self.where_keys]

        self._update_fields, self._values = get_update_fields(
            self.items[0],
            self.keys,
            start_from=self.start_from or 0,
            ignore_keys=self.where_keys,
            quote_columns=self.quote_columns,
            merge_keys=self.merge_keys,
        )
        if self.returning and self.return_keys:
            raise ValueError("Please choose either returning or return_keys")

    @property
    def values(self):
        if not self._values:
            raise ValueError("No values found")
        return self._values if len(self.items) == 1 else super().values

    def _get_where_query(self) -> str:
        if self.where:
            return self.where
        elif self.where_keys is not None:
            start_from = self.start_from if self.start_from is not None else len(self.values) - len(self.where_keys)

            return "WHERE " + " AND ".join(f"{k} = ${i + start_from + 1}" for i, k in enumerate(self.where_keys))
        return ""

    @property
    def query(self) -> str:
        if not self._update_fields:
            raise ValueError("No update fields found")

        query = f"""
        UPDATE {self.table} t
            SET {self._update_fields}
        {self._get_where_query()}
        """
        if self.returning or self.return_keys:
            returning = self.returning or [k for k in self.keys if k not in self.where_keys]
            query = _get_returning_query(query.strip(), returning)
        return query


OnConflict: TypeAlias = PGConflictQuery | str


def insert_pg(
    table: str,
    items: list[dict],
    *,
    on_conflict: OnConflict | None = None,
    returning: Iterable[K] | None = None,
    fill: bool = False,
    quote_columns: bool = False,
) -> PGInsertQuery:
    """
    Create a PGInsertQuery for inserting the given items into a table with optional conflict and returning behavior.
    
    Parameters:
        table (str): Target table name.
        items (list[dict]): List of row dictionaries to insert; each dict maps column names to values.
        on_conflict (OnConflict | None): Conflict resolution configuration. May be a PGConflictQuery or a constraint name string.
        returning (Iterable[K] | None): Keys (column name or iterable of column names) to include in a RETURNING clause.
        fill (bool): If True, fill missing keys in each item with None before building the query.
        quote_columns (bool): If True, quote column names in the generated SQL.
    
    Returns:
        PGInsertQuery: A configured insert query object ready for execution.
    """
    _on_conflict = PGConflictQuery(query=on_conflict) if isinstance(on_conflict, str) else on_conflict
    _returning = PGReturningQuery.load(keys=returning) if returning else None
    return PGInsertQuery(
        table, items, fill=fill, on_conflict=_on_conflict, returning=_returning, quote_columns=quote_columns
    )


Q = TypeVar("Q", bound=PGInsertQuery | PGUpdateQuery)
QueryCallback = Callable[[Q], None]


async def insert_one(
    conn: _Connection,
    table: str,
    item: dict,
    *,
    on_conflict: OnConflict | None = None,
    fill: bool = False,
    quote_columns: bool = False,
    query_callback: QueryCallback[PGInsertQuery] | None = None,
):
    """
    Insert a single row into the specified PostgreSQL table.
    
    Builds an INSERT query for the given item, optionally applying conflict-handling and column quoting, invokes an optional callback with the constructed query, and executes the query on the provided connection.
    
    Parameters:
        conn (_Connection): asyncpg connection or pool used to execute the query.
        table (str): Target table name.
        item (dict): Mapping of column names to values for the row to insert.
        on_conflict (OnConflict | None): Conflict specification or constraint to apply (e.g., keys or PGConflictQuery). If None, no ON CONFLICT clause is added.
        fill (bool): If True, fill missing keys in the item with None before building the query.
        quote_columns (bool): If True, quote column names in the generated SQL.
        query_callback (QueryCallback[PGInsertQuery] | None): Optional callable invoked with the constructed PGInsertQuery before execution.
    """
    query = insert_pg(table=table, items=[item], on_conflict=on_conflict, fill=fill, quote_columns=quote_columns)
    if query_callback is not None:
        query_callback(query)
    await query.run(conn)


async def insert_many(
    conn: _Connection,
    table: str,
    items: list[dict],
    *,
    on_conflict: OnConflict | None = None,
    fill: bool = False,
    quote_columns: bool = False,
    query_callback: QueryCallback[PGInsertQuery] | None = None,
):
    """
    Execute an INSERT for multiple rows into a PostgreSQL table.
    
    Parameters:
        table (str): Target table name.
        items (list[dict]): List of row dictionaries to insert; keys are column names.
        on_conflict (OnConflict | None): Conflict handling specification (columns, constraint, or PGConflictQuery) or None to omit `ON CONFLICT`.
        fill (bool): If True, fill missing keys in each item with None before building the query.
        quote_columns (bool): If True, quote column names in the generated SQL.
        query_callback (QueryCallback[PGInsertQuery] | None): Optional callback invoked with the constructed PGInsertQuery before execution.
    
    """
    query = insert_pg(table=table, items=items, on_conflict=on_conflict, fill=fill, quote_columns=quote_columns)
    if query_callback is not None:
        query_callback(query)
    await query.run(conn)


@overload
async def insert_returning(
    conn: _Connection,
    table: str,
    item: dict,
    returning: str,
    on_conflict: OnConflict | None = None,
    fill: bool = False,
    query_callback: QueryCallback[PGInsertQuery] | None = None,
) -> Any | None: """
    Insert a single row into the specified table and return the value of a given column.
    
    Parameters:
        table (str): Target table name.
        item (dict): Column-value mapping for the row to insert.
        returning (str): Column name to return from the inserted row.
        on_conflict (OnConflict | None): Conflict handling rule or constraint name to apply.
        fill (bool): If True, populate missing keys in `item` with None before insertion.
        query_callback (QueryCallback[PGInsertQuery] | None): Optional callback invoked with the constructed insert query before execution.
    
    Returns:
        Any | None: The value of the requested `returning` column from the inserted row, or `None` if no row is returned.
    """
    ...


@overload
async def insert_returning(
    conn: _Connection,
    table: str,
    item: dict,
    returning: list[str],
    on_conflict: OnConflict | None = None,
    fill: bool = False,
    query_callback: QueryCallback[PGInsertQuery] | None = None,
) -> asyncpg.Record | None: """
    Insert a single row into the given table and return the requested columns from the inserted (or upserted) row.
    
    Parameters:
        table (str): Target table name.
        item (dict): Column-value mapping for the row to insert.
        returning (list[str]): Column names to include in the returned record.
        on_conflict (OnConflict | None): Conflict resolution specification (columns, constraint, or PGConflictQuery); if provided, used to form an ON CONFLICT clause.
        fill (bool): If true, missing keys in `item` are filled with None before constructing the query.
        query_callback (QueryCallback[PGInsertQuery] | None): Optional callback invoked with the constructed PGInsertQuery before execution.
    
    Returns:
        asyncpg.Record | None: A record containing the requested `returning` columns if the statement produced a row, `None` otherwise.
    """
    ...


async def insert_returning(
    conn: _Connection,
    table: str,
    item: dict,
    returning: list[str] | str,
    on_conflict: OnConflict | None = None,
    fill: bool = False,
    query_callback: QueryCallback[PGInsertQuery] | None = None,
) -> asyncpg.Record | Any | None:
    """
    Insert a single row into the given table and return the requested column(s).
    
    Parameters:
        table (str): Target table name.
        item (dict): Mapping of column names to values for the row to insert.
        returning (list[str] | str): Column name or list of column names to return, or "*" to return all columns.
        on_conflict (OnConflict | None): Conflict resolution configuration or constraint name; if provided, used to build an ON CONFLICT clause.
        fill (bool): If True, fill missing keys in `item` with None before inserting.
        query_callback (QueryCallback[PGInsertQuery] | None): Optional callback invoked with the constructed PGInsertQuery before execution.
    
    Returns:
        asyncpg.Record | Any | None: If `returning` is a single column name (and not "*"), returns that column's value or `None` if no row was returned; otherwise returns an `asyncpg.Record` with the requested columns or `None` if no row was returned.
    """
    returning_values = [returning] if isinstance(returning, str) else returning
    query = insert_pg(table=table, items=[item], on_conflict=on_conflict, fill=fill, returning=returning_values)
    if query_callback is not None:
        query_callback(query)
    fn = conn.fetchval if len(returning_values) == 1 and returning != "*" else conn.fetchrow

    return await fn(query.query, *query.values)


async def fetch_count(conn: _Connection, query: str, *args) -> int:
    c = await conn.fetchval(f"SELECT COUNT(*) FROM ({query}) t", *args)
    return typing.cast(int, c)


def Conflict(
    keys: Iterable[K],
    ignore_keys: Iterable[K] | None = None,
) -> PGConflictQuery:
    return PGConflictQuery(keys=keys, ignore_keys=ignore_keys)


async def update_one(
    conn: _Connection,
    table: str,
    item: dict,
    *args,
    keys: list[str] | None = None,
    start_from: int | None = None,
    where: str | None = None,
    merge_keys: list[str] | None = None,
    query_callback: QueryCallback[PGUpdateQuery] | None = None,
):
    """
    Execute an UPDATE statement for a single item in the specified table.
    
    Parameters:
        conn: Database connection or pool used to execute the query.
        table (str): Target table name.
        item (dict): Mapping of column names to values for the single row to update.
        *args: Positional arguments to pass to conn.execute before the query's parameter values.
        keys (list[str] | None): Column names used to build the WHERE clause; if omitted, no generated WHERE is applied unless `where` is provided.
        start_from (int | None): Starting index for positional parameter placeholders (1-based); adjusts numbering of generated parameters.
        where (str | None): Explicit WHERE clause string; if provided, it overrides generated WHERE from `keys`.
        merge_keys (list[str] | None): Column names whose values should be merged as JSONB rather than replaced.
        query_callback (Callable[[PGUpdateQuery], None] | None): Optional callback invoked with the constructed PGUpdateQuery before execution.
    """
    query = PGUpdateQuery(
        table=table, items=[item], start_from=start_from, where_keys=keys, where=where, merge_keys=merge_keys
    )
    if query_callback is not None:
        query_callback(query)
    await conn.execute(query.query, *args, *query.values)


@overload
async def update_returning(
    conn: _Connection,
    table: str,
    item: dict,
    *args,
    returning: str,
    return_keys: Literal[False] = False,
    where: str | None = None,
    keys: list[str] | None = None,
    start_from: int | None = None,
    merge_keys: list[str] | None = None,
    query_callback: QueryCallback[PGUpdateQuery] | None = None,
) -> Any | None: """
    Update a row in the given table and return requested column value(s).
    
    Parameters:
        table (str): Target table name.
        item (dict): Column values to update for the row(s) identified by `where` or `keys`.
        *args: Positional arguments passed to the underlying query execution (e.g., additional values for WHERE).
        returning (str): Column name to return from the updated row.
        return_keys (Literal[False] | True): If `True`, return a record mapping of returned columns; if `False`, return the single column's value. Defaults to `False`.
        where (str | None): Optional SQL WHERE clause to select rows to update. If omitted, `keys` must be provided.
        keys (list[str] | None): Column names to use for generating a WHERE clause from `item` when `where` is not provided.
        start_from (int | None): Starting index for parameter placeholders in the generated query.
        merge_keys (list[str] | None): Keys that should be merged (e.g., JSONB merge) instead of overwritten.
        query_callback (QueryCallback[PGUpdateQuery] | None): Optional callback invoked with the constructed PGUpdateQuery before execution.
    
    Returns:
        The value of the requested `returning` column when `return_keys` is `False`, a record/dict of returned columns when `return_keys` is `True`, or `None` if no row was updated.
    """
    ...


@overload
async def update_returning(
    conn: _Connection,
    table: str,
    item: dict,
    *args,
    returning: list[str],
    return_keys: Literal[False] = False,
    where: str | None = None,
    keys: list[str] | None = None,
    start_from: int | None = None,
    merge_keys: list[str] | None = None,
    query_callback: QueryCallback[PGUpdateQuery] | None = None,
) -> asyncpg.Record | None: """
    Execute an UPDATE on `table` using values from `item` and return the requested columns.
    
    Performs an UPDATE that sets columns from `item`, applies a WHERE clause either from `where` or from `keys`, and returns the columns listed in `returning` for the matched row; returns `None` if no row matched. If `query_callback` is provided it will be called with the constructed PGUpdateQuery before execution.
    
    Parameters:
        item (dict): Column-value mapping to set on the target row.
        returning (list[str]): Column names to include in the `RETURNING` clause.
        return_keys (Literal[False]): Must be `False` for this overload; controls return-shape in other overloads.
        where (str | None): Explicit SQL WHERE clause to apply; if provided it overrides `keys`.
        keys (list[str] | None): Column names to use for an implicit WHERE clause matching values from `item` when `where` is not provided.
        start_from (int | None): Start index for SQL parameter placeholders (e.g., 1-based offset).
        merge_keys (list[str] | None): Column names whose values should be merged (JSONB merge semantics) instead of replaced.
        query_callback (QueryCallback[PGUpdateQuery] | None): Optional callback invoked with the constructed PGUpdateQuery before execution.
    
    Returns:
        asyncpg.Record | None: The returned row containing the requested `returning` columns, or `None` if no row matched.
    """
    ...


@overload
async def update_returning(
    conn: _Connection,
    table: str,
    item: dict,
    *args,
    returning: None = None,
    return_keys: Literal[True],
    where: str | None = None,
    keys: list[str] | None = None,
    start_from: int | None = None,
    merge_keys: list[str] | None = None,
    query_callback: QueryCallback[PGUpdateQuery] | None = None,
) -> asyncpg.Record | None: """
    Builds and executes an UPDATE for a single item and returns the requested returning values.
    
    Parameters:
        conn: Database connection or pool used to execute the query.
        table: Name of the table to update.
        item: Mapping of column names to new values for the update.
        *args: Positional values appended to the query parameters (e.g., values for a custom WHERE).
        returning: Column name or list of column names to include in the RETURNING clause; if omitted, no specific columns are requested.
        return_keys: When True, request the updated columns corresponding to the keys present in `item` (useful instead of specifying `returning`).
        where: Explicit WHERE clause to filter which row is updated; if omitted, `keys` is used to build the WHERE clause.
        keys: List of column names to use for the generated WHERE clause when `where` is not provided.
        start_from: Starting index for positional parameter placeholders (useful when composing queries with existing parameters).
        merge_keys: List of keys whose values should be merged using JSONB merge semantics instead of replaced.
        query_callback: Optional callback invoked with the constructed PGUpdateQuery before execution (can inspect or modify the query).
    
    Returns:
        An asyncpg.Record containing the requested returning columns, or `None` if no row matched the update.
    """
    ...


async def update_returning(
    conn: _Connection,
    table: str,
    item: dict,
    *args,
    returning: list[str] | str | None = None,
    return_keys: bool = False,
    where: str | None = None,
    keys: list[str] | None = None,
    start_from: int | None = None,
    merge_keys: list[str] | None = None,
    query_callback: QueryCallback[PGUpdateQuery] | None = None,
) -> Any | asyncpg.Record | None:
    """
    Update a single row in the given table and return specified columns from the updated row.
    
    Parameters:
        conn (_Connection): Database connection or pool used to execute the query.
        table (str): Table name to update.
        item (dict): Mapping of column names to values to set on the row.
        *args: Positional parameters to bind to the query placeholders (e.g., values used in WHERE).
        returning (list[str] | str | None): Column name or list of column names to return from the updated row. If None, no columns are requested.
        return_keys (bool): If True and `returning` is None, return the input `item` keys as part of a RETURNING clause.
        where (str | None): Explicit WHERE clause to restrict which row is updated. If omitted, `keys` may be used to build the WHERE clause.
        keys (list[str] | None): Column names from `item` to use as the WHERE clause (instead of updating them).
        start_from (int | None): Starting index for parameter placeholders (e.g., 1 for $1); adjusts positional parameter numbering.
        merge_keys (list[str] | None): Column names whose JSONB values should be merged instead of replaced.
        query_callback (QueryCallback[PGUpdateQuery] | None): Optional callback invoked with the constructed PGUpdateQuery before execution.
    
    Returns:
        Any | asyncpg.Record | None: If a single column name was requested via `returning`, returns that column's value; if multiple columns were requested, returns an asyncpg.Record containing those columns; returns `None` if no returning columns were requested or the update affected no rows.
    """
    if returning is not None:
        returning_values = [returning] if isinstance(returning, str) else returning
    else:
        returning_values = None
    query = PGUpdateQuery(
        table=table,
        items=[item],
        start_from=start_from,
        where=where,
        where_keys=keys,
        return_keys=return_keys,
        returning=returning_values,
        merge_keys=merge_keys,
    )
    if query_callback is not None:
        query_callback(query)
    fn = conn.fetchval if len(returning_values or []) == 1 else conn.fetchrow
    return await fn(query.query, *args, *query.values)


async def update_many(
    conn: _Connection,
    table: str,
    items: list[dict],
    keys: list[str] | None = None,
    start_from: int | None = None,
    where: str | None = None,
    merge_keys: list[str] | None = None,
    query_callback: QueryCallback[PGUpdateQuery] | None = None,
):
    """
    Update multiple rows in a table using the provided item mappings.
    
    Executes a batched UPDATE for each mapping in `items`, using `where` or `keys` to form the WHERE clause and applying JSONB merge for any `merge_keys`.
    
    Parameters:
        conn (_Connection): asyncpg Connection or Pool used to execute the query.
        table (str): Target table name.
        items (list[dict]): List of dictionaries where each dict maps column names to new values for a single row.
        keys (list[str] | None): Column names to use for the WHERE clause; if omitted, WHERE clause is generated from overlap between items and other configuration.
        start_from (int | None): Starting index for SQL parameter placeholders (1-based); used when combining with other parameterized fragments.
        where (str | None): Explicit WHERE clause string that overrides generated WHERE conditions when provided.
        merge_keys (list[str] | None): Columns to merge using JSONB concatenation instead of simple assignment.
        query_callback (QueryCallback[PGUpdateQuery] | None): Optional callback invoked with the constructed PGUpdateQuery before execution.
    """
    query = PGUpdateQuery(
        table=table, items=items, start_from=start_from, where_keys=keys, where=where, merge_keys=merge_keys
    )
    if query_callback is not None:
        query_callback(query)
    await conn.executemany(query.query, query.values)