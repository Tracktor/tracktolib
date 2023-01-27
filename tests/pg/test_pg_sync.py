import pytest


@pytest.mark.usefixtures('setup_tables')
def test_insert_fetch_many(engine):
    from tracktolib.pg_sync import insert_many, fetch_all

    data = [
        {'foo': 1, 'bar': 'baz'},
        {'foo': 2, 'bar': 'bazz'}
    ]
    insert_many(engine, 'foo.bar', data)
    db_data = fetch_all(engine, 'SELECT foo, bar FROM foo.bar ORDER BY foo')
    assert data == db_data


@pytest.fixture()
def insert_data(engine):
    from tracktolib.pg_sync import insert_many

    data = [
        {'foo': 1, 'bar': 'baz'},
        {'foo': 2, 'bar': 'bazz'}
    ]

    insert_many(engine, 'foo.bar', data)


@pytest.mark.usefixtures('setup_tables', 'insert_data')
def test_fetch_one(engine):
    from tracktolib.pg_sync import fetch_one

    db_data = fetch_one(engine, 'SELECT foo, bar FROM foo.bar ORDER BY foo')
    assert db_data == {'foo': 1, 'bar': 'baz'}


@pytest.mark.usefixtures('setup_tables', 'insert_data')
def test_fetch_count(engine):
    from tracktolib.pg_sync import fetch_count

    assert fetch_count(engine, 'foo.bar') == 2
    assert fetch_count(engine, 'foo.bar', where='foo = 1') == 1
    assert fetch_count(engine, 'foo.bar', 1, where='foo = %s') == 1


def test_insert_many(engine):
    from tracktolib.pg_sync import insert_many, fetch_all
    from tracktolib.tests import assert_equals
    data = [{'bar': {'foo': 1}, 'baz': {'foo': 2}}]
    insert_many(engine, 'foo.baz', data)
    db_data = fetch_all(engine, 'SELECT bar, baz FROM foo.baz')
    assert_equals(data, db_data)


@pytest.mark.usefixtures('setup_tables')
def test_insert_csv(engine, static_dir):
    from tracktolib.pg_sync import insert_csv, fetch_all
    from tracktolib.tests import assert_equals

    file = static_dir / 'test.csv'

    for i in range(0, 2):
        # Works on conflict
        with engine.cursor() as c:
            insert_csv(c, 'foo', 'bar', file)
        engine.commit()

        db_data = fetch_all(engine, 'SELECT foo, bar FROM foo.bar ORDER BY foo')
        assert_equals(db_data, [{'bar': '2', 'foo': 1}])
