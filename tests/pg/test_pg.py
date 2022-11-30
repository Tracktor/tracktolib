import pprint

import asyncpg
import pytest

from tracktolib.pg_sync import fetch_all


def test_insert_one_query():
    from tracktolib.pg import PGInsertQuery
    query = PGInsertQuery('schema.table', [{'foo': 1}]).query
    assert query == 'INSERT INTO schema.table AS t (foo) VALUES ( $1 )'


def test_insert_many_query():
    from tracktolib.pg import PGInsertQuery
    query = PGInsertQuery('schema.table',
                          [{'foo': 1}, {'foo': 2}]).query
    assert query == 'INSERT INTO schema.table AS t (foo) VALUES ( $1 )'


def compare_strings(str1: str, str2: str):
    str1 = ' '.join(x.strip() for x in str1.split('\n'))
    str2 = ' '.join(x.strip() for x in str2.split('\n'))
    assert str1.strip() == str2.strip()


@pytest.mark.parametrize('data,on_conflict,expected', [
    (
            {'foo': 1},
            {'keys': ['id']},
            'INSERT INTO schema.table AS t (foo) VALUES ( $1 ) '
            'ON CONFLICT (id) DO UPDATE SET foo = COALESCE(EXCLUDED.foo, t.foo)'
    ),
    (
            {'foo': 1, 'bar': 2},
            {'keys': ['id']},
            'INSERT INTO schema.table AS t (bar, foo) VALUES ( $1, $2 ) '
            'ON CONFLICT (id) DO UPDATE SET bar = COALESCE(EXCLUDED.bar, t.bar), '
            'foo = COALESCE(EXCLUDED.foo, t.foo)'
    ),
    (
            {'foo': 1, 'bar': 2},
            {'keys': ['id'], 'ignore_keys': ['foo']},
            'INSERT INTO schema.table AS t (bar, foo) VALUES ( $1, $2 ) '
            'ON CONFLICT (id) DO UPDATE SET bar = COALESCE(EXCLUDED.bar, t.bar)'
    ),
    (
            {'foo': 1, 'bar': 2},
            {'query': 'ON CONFLICT DO NOTHING'},
            'INSERT INTO schema.table AS t (bar, foo) VALUES ( $1, $2 ) '
            'ON CONFLICT DO NOTHING'
    ),
    (
            {'foo': 1, 'bar': 2},
            {'constraint': 'my_constraint'},
            """
            INSERT INTO schema.table AS t (bar, foo) VALUES ( $1, $2 ) 
            ON CONFLICT ON CONSTRAINT my_constraint 
            DO UPDATE SET 
                bar = COALESCE(EXCLUDED.bar, t.bar), 
                foo = COALESCE(EXCLUDED.foo, t.foo)
            """
    )
])
def test_insert_conflict_query(data, on_conflict, expected):
    from tracktolib.pg import PGInsertQuery, PGConflictQuery
    query = PGInsertQuery('schema.table', [data],
                          on_conflict=PGConflictQuery(**on_conflict)).query

    compare_strings(query, expected)


@pytest.mark.usefixtures('setup_tables')
def test_insert_one(loop, aengine, engine):
    from tracktolib.pg import insert_one
    loop.run_until_complete(insert_one(aengine, 'foo.foo', {'foo': 1}))

    db_data = fetch_all(engine, 'SELECT bar, foo FROM foo.foo')
    assert db_data == [{'bar': None, 'foo': 1}]


@pytest.mark.usefixtures('setup_tables')
def test_insert_many(loop, aengine, engine):
    from tracktolib.pg import insert_many
    loop.run_until_complete(
        insert_many(aengine, 'foo.foo', [{'foo': 1}, {'bar': 'hello'}],
                    fill=True)
    )

    db_data = fetch_all(engine, 'SELECT bar, foo FROM foo.foo ORDER BY foo')
    assert db_data == [{'bar': None, 'foo': 1}, {'bar': 'hello', 'foo': None}]


@pytest.fixture()
def insert_data(engine):
    from tracktolib.pg_sync import insert_many
    data = [
        {'id': 1, 'foo': 10, 'bar': 'baz'},
        {'id': 2, 'foo': 20, 'bar': None}
    ]
    insert_many(engine, 'foo.foo', data)


@pytest.mark.usefixtures('setup_tables', 'insert_data')
def test_insert_conflict_one(loop, aengine, engine):
    from tracktolib.pg_sync import fetch_all
    from tracktolib.pg import insert_one, Conflict

    loop.run_until_complete(insert_one(aengine,
                                       'foo.foo', {'id': 1, 'foo': 1},
                                       on_conflict=Conflict(keys=['id'])))
    db_data = fetch_all(engine, 'SELECT bar, foo FROM foo.foo WHERE id = 1')
    assert db_data == [{'bar': 'baz', 'foo': 1}]


@pytest.mark.usefixtures('setup_tables', 'insert_data')
def test_insert_conflict_many(loop, aengine, engine):
    from tracktolib.pg import insert_many, Conflict

    data = [
        {'id': 1, 'foo': 1},
        {'id': 2, 'bar': 'hello'}
    ]
    loop.run_until_complete(
        insert_many(aengine, 'foo.foo', data, fill=True,
                    on_conflict=Conflict(keys=['id'], ignore_keys=['foo'])
                    )
    )
    db_data = fetch_all(engine, 'SELECT bar, foo FROM foo.foo ORDER BY foo')
    assert [{'bar': 'baz', 'foo': 10},
            {'bar': 'hello', 'foo': 20}
            ] == db_data


@pytest.mark.usefixtures('setup_tables')
def test_insert_one_returning_one(loop, aengine, engine):
    from tracktolib.pg import insert_returning
    new_id = loop.run_until_complete(
        insert_returning(aengine, 'foo.foo', {'id': 1, 'foo': 1},
                         returning='id')
    )
    assert new_id is not None

    db_data = fetch_all(engine, 'SELECT bar, foo FROM foo.foo WHERE id = %s',
                        new_id)
    assert db_data == [{'bar': None, 'foo': 1}]


@pytest.mark.usefixtures('setup_tables')
def test_insert_one_returning_many(loop, aengine, engine):
    from tracktolib.pg import insert_returning

    returned_value = loop.run_until_complete(
        insert_returning(aengine, 'foo.foo', {'id': 1, 'foo': 1}, returning=['id', 'bar'])
    )
    returned_value = dict(returned_value)
    assert returned_value.pop('id') is not None
    assert returned_value == {'bar': None}


@pytest.fixture()
def insert_iterate_data(engine):
    from tracktolib.pg_sync import insert_many
    data = [
        {'id': i, 'foo': 10, 'bar': 'baz'} for i in range(10)
    ]
    insert_many(engine, 'foo.foo', data)


@pytest.mark.usefixtures('setup_tables', 'insert_iterate_data')
def test_iterate_pg(aengine, loop):
    from tracktolib.pg import iterate_pg
    from tracktolib.tests import assert_equals
    output = []

    async def _test():
        nonlocal output
        query = 'SELECT id FROM foo.foo'
        async for c in iterate_pg(aengine, query, from_offset=2, chunk_size=3):
            output.append([dict(x) for x in c])

    loop.run_until_complete(_test())
    expected = [
        [{'id': 2}, {'id': 3}, {'id': 4}],
        [{'id': 5}, {'id': 6}, {'id': 7}],
        [{'id': 8}, {'id': 9}]
    ]

    assert_equals(output, expected)


@pytest.mark.usefixtures('setup_tables')
def test_upload_csv(aengine, loop, static_dir, engine):
    from tracktolib.pg import upsert_csv
    from tracktolib.tests import assert_equals

    file = static_dir / 'test.csv'

    loop.run_until_complete(upsert_csv(aengine, file, schema='foo', table='bar'))
    db = fetch_all(engine, 'SELECT * FROM foo.bar ORDER BY foo')
    expected = [{'bar': '2', 'foo': 1}]
    assert_equals(db, expected)
    # Run again works
    loop.run_until_complete(upsert_csv(aengine, file, schema='foo', table='bar'))