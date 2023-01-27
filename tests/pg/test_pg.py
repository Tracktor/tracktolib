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


@pytest.mark.parametrize('data,on_conflict,returning,expected', [
    (
            {'foo': 1},
            {'keys': ['id']},
            None,
            'INSERT INTO schema.table AS t (foo) VALUES ( $1 ) '
            'ON CONFLICT (id) DO UPDATE SET foo = COALESCE(EXCLUDED.foo, t.foo)'
    ),
    (
            {'foo': 1, 'bar': 2},
            {'keys': ['id']},
            None,
            'INSERT INTO schema.table AS t (bar, foo) VALUES ( $1, $2 ) '
            'ON CONFLICT (id) DO UPDATE SET bar = COALESCE(EXCLUDED.bar, t.bar), '
            'foo = COALESCE(EXCLUDED.foo, t.foo)'
    ),
    (
            {'foo': 1, 'bar': 2},
            {'keys': ['id'], 'ignore_keys': ['foo']},
            None,
            'INSERT INTO schema.table AS t (bar, foo) VALUES ( $1, $2 ) '
            'ON CONFLICT (id) DO UPDATE SET bar = COALESCE(EXCLUDED.bar, t.bar)'
    ),
    (
            {'foo': 1, 'bar': 2},
            {'query': 'ON CONFLICT DO NOTHING'},
            None,
            'INSERT INTO schema.table AS t (bar, foo) VALUES ( $1, $2 ) '
            'ON CONFLICT DO NOTHING'
    ),
    (
            {'foo': 1, 'bar': 2},
            {'query': 'ON CONFLICT DO NOTHING'},
            None,
            'INSERT INTO schema.table AS t (bar, foo) VALUES ( $1, $2 ) '
            'ON CONFLICT DO NOTHING'
    ),
    (
            {'foo': 1, 'bar': 2},
            {'query': 'ON CONFLICT DO NOTHING'},
            {'key': 'bar'},
            'INSERT INTO schema.table AS t (bar, foo) VALUES ( $1, $2 ) '
            'ON CONFLICT DO NOTHING RETURNING bar'
    ),
    (
            {'foo': 1, 'bar': 2},
            {'constraint': 'my_constraint'},
            None,
            """
            INSERT INTO schema.table AS t (bar, foo) VALUES ( $1, $2 ) 
            ON CONFLICT ON CONSTRAINT my_constraint 
            DO UPDATE SET 
                bar = COALESCE(EXCLUDED.bar, t.bar), 
                foo = COALESCE(EXCLUDED.foo, t.foo)
            """
    )
])
def test_pg_insert_query(data, on_conflict, returning, expected):
    from tracktolib.pg import PGInsertQuery, PGConflictQuery, PGReturningQuery
    _returning = PGReturningQuery.load(**returning) if returning else None
    query = PGInsertQuery('schema.table', [data],
                          on_conflict=PGConflictQuery(**on_conflict),
                          returning=_returning).query

    compare_strings(query, expected)


@pytest.mark.parametrize('async_engine', ['connection', 'pool'])
@pytest.mark.usefixtures('setup_tables')
def test_insert_one(loop, aengine, apool, engine, async_engine):
    from tracktolib.pg import insert_one

    _engine = aengine if async_engine == 'connection' else apool

    loop.run_until_complete(insert_one(_engine, 'foo.foo', {'foo': 1}))

    db_data = fetch_all(engine, 'SELECT bar, foo FROM foo.foo')
    assert db_data == [{'bar': None, 'foo': 1}]


@pytest.mark.parametrize('async_engine', ['connection', 'pool'])
@pytest.mark.usefixtures('setup_tables')
def test_insert_many(loop, aengine, apool, engine, async_engine):
    from tracktolib.pg import insert_many

    _engine = aengine if async_engine == 'connection' else apool

    loop.run_until_complete(
        insert_many(_engine, 'foo.foo', [{'foo': 1}, {'bar': 'hello'}],
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


@pytest.mark.parametrize('on_conflict', [None, 'ON CONFLICT DO NOTHING'])
@pytest.mark.usefixtures('setup_tables')
def test_insert_one_returning_one(loop, aengine, engine, on_conflict):
    from tracktolib.pg import insert_returning
    new_id = loop.run_until_complete(
        insert_returning(aengine, 'foo.foo', {'id': 1, 'foo': 1},
                         returning='id', on_conflict=on_conflict)
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


@pytest.mark.usefixtures('setup_tables', 'insert_iterate_data')
def test_fetch_count(aengine, loop):
    from tracktolib.pg import fetch_count

    count = loop.run_until_complete(
        fetch_count(aengine, 'SELECT 1 FROM foo.foo')
    )
    assert count == 10


@pytest.mark.parametrize('data,params,expected', [
    (
            {'foo': 1},
            {'start_from': 0, 'where': None, 'returning': None},
            {'query': 'UPDATE schema.table SET foo = $1', 'values': [1]}
    ),
    (
            {'foo': 1},
            {'start_from': 1, 'where': 'WHERE bar = $1', 'returning': None},
            {'query': 'UPDATE schema.table SET foo = $2 WHERE bar = $1', 'values': [1]}
    ),
    (
            {'foo': 1},
            {'returning': ['foo']},
            {'query': 'UPDATE schema.table SET foo = $1 RETURNING foo', 'values': [1]}
    ),
    (
            {'foo': 1, 'id': 1},
            {'where_keys': ['id']},
            {'query': 'UPDATE schema.table SET foo = $1 WHERE id = $2', 'values': [1, 1]}
    ),
    (
            {'foo': 1, 'id': 1},
            {'where_keys': ['id'], 'return_keys': True},
            {'query': 'UPDATE schema.table SET foo = $1 WHERE id = $2 RETURNING foo', 'values': [1, 1]}
    ),
    (
            {'foo': 2, 'id': 1, 'bar': 3},
            {'where_keys': ['id', 'bar'], 'return_keys': True},
            {'query': 'UPDATE schema.table SET foo = $1 WHERE bar = $2 AND id = $3 RETURNING foo', 'values': [2, 3, 1]}
    ),
    (
            {'foo': 2, 'id': 1, 'bar': 3},
            {'where_keys': ['bar', 'id'], 'return_keys': True},
            {'query': 'UPDATE schema.table SET foo = $1 WHERE bar = $2 AND id = $3 RETURNING foo', 'values': [2, 3, 1]}
    ),
])
def test_pg_update_query(data, params, expected):
    from tracktolib.pg import PGUpdateQuery
    query = PGUpdateQuery('schema.table', [data], **params)
    compare_strings(query.query, expected['query'])
    assert query.values == expected['values']


@pytest.mark.usefixtures('setup_tables', 'insert_data')
def test_update_one(aengine, loop, engine):
    from tracktolib.pg import update_one
    loop.run_until_complete(
        update_one(aengine, 'foo.foo', {'foo': 1},
                   1,
                   start_from=1,
                   where='WHERE id = $1')
    )


@pytest.mark.usefixtures('setup_tables', 'insert_data')
def test_update_one_keys(aengine, loop, engine):
    from tracktolib.pg import update_one
    loop.run_until_complete(
        update_one(aengine, 'foo.foo', {'foo': 2, 'id': 1},
                   keys=['id'])
    )


@pytest.mark.parametrize('param_args,params_kwargs,expected', [
    ([{'foo': 1}, 1], {'returning': 'foo', 'start_from': 1, 'where': 'WHERE id =$1'}, 1),
    ([{'foo': 2, 'id': 1}], {'returning': 'foo', 'keys': ['id']}, 2),
    ([{'foo': 2, 'id': 1}], {'returning': 'foo', 'keys': ['id']}, 2),
    ([{'foo': 2, 'id': 1}], {'return_keys': True, 'keys': ['id']}, {'foo': 2}),
])
@pytest.mark.usefixtures('setup_tables', 'insert_data')
def test_update_one_returning(aengine, loop, engine,
                              param_args,
                              params_kwargs,
                              expected):
    from tracktolib.pg import update_returning
    value = loop.run_until_complete(
        update_returning(aengine, 'foo.foo',
                         *param_args,
                         **params_kwargs)
    )
    assert value if not isinstance(value, asyncpg.Record) else dict(value) == expected


async def check_update_one_types(aengine):
    """This won't be called, only used to check types"""
    from tracktolib.pg import update_returning

    await update_returning(aengine, 'foo.foo', {'foo': 1}, returning='foo')
    await update_returning(aengine, 'foo.foo', {'foo': 1}, return_keys=True)
