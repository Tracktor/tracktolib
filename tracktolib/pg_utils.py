from typing_extensions import LiteralString


def get_tmp_table_query(schema: LiteralString,
                        table: LiteralString):
    tmp_table_name = f'{schema}_{table}_tmp'
    create_tmp_table_query = f"""
    CREATE TEMP TABLE {tmp_table_name}
    (LIKE {schema}.{table} INCLUDING DEFAULTS)
    ON COMMIT DROP;
    """
    insert_query = f"""
        INSERT INTO {schema}.{table}
        SELECT *
        FROM {tmp_table_name}
        ON CONFLICT DO NOTHING;
    """
    return tmp_table_name, create_tmp_table_query, insert_query
