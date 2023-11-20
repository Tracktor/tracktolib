from typing_extensions import LiteralString
from typing import Iterable
from typing import cast


def get_tmp_table_query(
    schema: LiteralString,
    table: LiteralString,
    columns: Iterable[LiteralString] | None = None,
    on_conflict: LiteralString = "ON CONFLICT DO NOTHING",
):
    tmp_table_name = f"{schema}_{table}_tmp"
    create_tmp_table_query = f"""
    CREATE TEMP TABLE {tmp_table_name}
    (LIKE {schema}.{table} INCLUDING DEFAULTS)
    ON COMMIT DROP;
    """

    if columns:
        _columns = ",".join(columns)
        insert_query = f"""
            INSERT INTO {schema}.{table} as t({_columns})
            SELECT {_columns}
            FROM {tmp_table_name}
            {on_conflict};
        """
    else:
        insert_query = f"""
            INSERT INTO {schema}.{table}
            SELECT *
            FROM {tmp_table_name}
            {on_conflict};
        """
    return tmp_table_name, create_tmp_table_query, insert_query


def get_conflict_query(
    columns: Iterable[str],
    update_columns: Iterable[str] | None = None,
    ignore_columns: Iterable[str] | None = None,
    constraint: str | None = None,
    on_conflict: str | None = None,
    where: str | None = None,
) -> LiteralString:
    if on_conflict:
        return cast(LiteralString, on_conflict)

    if constraint:
        query = f"ON CONFLICT ON CONSTRAINT {constraint}"
    elif update_columns:
        update_columns_str = ", ".join(sorted(update_columns))
        query = f"ON CONFLICT ({update_columns_str})"
        if where:
            query += f" WHERE {where}"
    else:
        raise NotImplementedError("update_keys or constraint must be set")

    _ignore_columns = [*(update_columns or []), *(ignore_columns or [])]
    fields = ", ".join(f"{x} = COALESCE(EXCLUDED.{x}, t.{x})" for x in columns if x not in _ignore_columns)
    if not fields:
        raise ValueError("No fields set")

    return cast(LiteralString, f"{query} DO UPDATE SET {fields}")
