from typing_extensions import LiteralString
from typing import Iterable
from typing import cast


def get_tmp_table_query(schema: LiteralString,
                        table: LiteralString,
                        columns: Iterable[LiteralString] | None = None,
                        on_conflict: LiteralString = 'ON CONFLICT DO NOTHING'):
    tmp_table_name = f'{schema}_{table}_tmp'
    create_tmp_table_query = f"""
    CREATE TEMP TABLE {tmp_table_name}
    (LIKE {schema}.{table} INCLUDING DEFAULTS)
    ON COMMIT DROP;
    """

    if columns:
        _columns = ','.join(columns)
        insert_query = f"""
            INSERT INTO {schema}.{table} as t({_columns})
            SELECT *
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


def get_conflict_query(keys: Iterable[str],
                       update_keys: Iterable[str] | None = None,
                       ignore_keys: Iterable[str] | None = None,
                       constraint: str | None = None,
                       on_conflict: str | None = None) -> LiteralString:
    if on_conflict:
        return cast(LiteralString, on_conflict)

    if constraint:
        query = f'ON CONFLICT ON CONSTRAINT {constraint}'
    elif update_keys:
        update_keys_str = ', '.join(sorted(update_keys))
        query = f'ON CONFLICT ({update_keys_str})'
    else:
        raise NotImplementedError('update_keys or constraint must be set')

    _ignore_keys = [*(update_keys or []), *(ignore_keys or [])]
    fields = ', '.join(f'{x} = COALESCE(EXCLUDED.{x}, t.{x})'
                       for x in keys
                       if x not in _ignore_keys)
    if not fields:
        raise ValueError('No fields set')

    return cast(LiteralString, f'{query} DO UPDATE SET {fields}')
