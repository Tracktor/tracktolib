import asyncpg
import pytest

from tracktolib.pg import Conflict, PGConflictQuery
from tracktolib.pg_sync import fetch_all, insert_one
from tracktolib.tests import assert_equals


def test_insert_one_query():
    from tracktolib.pg import PGInsertQuery

    query = PGInsertQuery("schema.table", [{"foo": 1}]).query
    assert query == "INSERT INTO schema.table AS t (foo) VALUES ($1)"


def test_insert_many_query():
    from tracktolib.pg import PGInsertQuery

    query = PGInsertQuery("schema.table", [{"foo": 1}, {"foo": 2}]).query
    assert query == "INSERT INTO schema.table AS t (foo) VALUES ($1)"


def compare_strings(str1: str, str2: str):
    str1 = " ".join(x.strip() for x in str1.split("\n"))
    str2 = " ".join(x.strip() for x in str2.split("\n"))

    assert str1.strip() == str2.strip()


@pytest.mark.parametrize(
    "data,on_conflict,returning,expected,quote_columns",
    [
        (
            {"foo": 1},
            {"keys": ["id"]},
            None,
            "INSERT INTO schema.table AS t (foo) VALUES ($1) "
            "ON CONFLICT (id) DO UPDATE SET foo = COALESCE(EXCLUDED.foo, t.foo)",
            False,
        ),
        (
            {"foo": 1, "bar": 2},
            {"keys": ["id"]},
            None,
            "INSERT INTO schema.table AS t (bar, foo) VALUES ($1, $2) "
            "ON CONFLICT (id) DO UPDATE SET bar = COALESCE(EXCLUDED.bar, t.bar), "
            "foo = COALESCE(EXCLUDED.foo, t.foo)",
            False,
        ),
        (
            {"foo": 1, "bar": 2},
            {"keys": ["id"], "where": "id = 2"},
            None,
            "INSERT INTO schema.table AS t (bar, foo) VALUES ($1, $2) "
            "ON CONFLICT (id) WHERE id = 2 DO UPDATE SET bar = COALESCE(EXCLUDED.bar, t.bar), "
            "foo = COALESCE(EXCLUDED.foo, t.foo)",
            False,
        ),
        (
            {"foo": 1, "bar": 2},
            {"keys": ["id"], "ignore_keys": ["foo"]},
            None,
            "INSERT INTO schema.table AS t (bar, foo) VALUES ($1, $2) "
            "ON CONFLICT (id) DO UPDATE SET bar = COALESCE(EXCLUDED.bar, t.bar)",
            False,
        ),
        (
            {"foo": 1, "bar": 2},
            {"query": "ON CONFLICT DO NOTHING"},
            None,
            "INSERT INTO schema.table AS t (bar, foo) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            False,
        ),
        (
            {"foo": 1, "bar": 2},
            {"query": "ON CONFLICT DO NOTHING"},
            None,
            "INSERT INTO schema.table AS t (bar, foo) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            False,
        ),
        (
            {"foo": 1, "bar": 2},
            {"query": "ON CONFLICT DO NOTHING"},
            {"key": "bar"},
            "INSERT INTO schema.table AS t (bar, foo) VALUES ($1, $2) ON CONFLICT DO NOTHING RETURNING bar",
            False,
        ),
        (
            {"foo": 1, "bar": 2},
            {"constraint": "my_constraint"},
            None,
            """
                INSERT INTO schema.table AS t (bar, foo)
                VALUES ($1, $2)
                ON CONFLICT ON CONSTRAINT my_constraint
                    DO UPDATE SET bar = COALESCE(EXCLUDED.bar, t.bar),
                                  foo = COALESCE(EXCLUDED.foo, t.foo)
                """,
            False,
        ),
        (
            {"foo": 1, "bar": 2},
            {"constraint": "my_constraint"},
            None,
            """
                INSERT INTO schema.table AS t ("bar", "foo")
                VALUES ($1, $2)
                ON CONFLICT ON CONSTRAINT my_constraint
                    DO UPDATE SET "bar" = COALESCE(EXCLUDED."bar", t."bar"),
                                  "foo" = COALESCE(EXCLUDED."foo", t."foo")
                """,
            True,
        ),
        (
            {"foo": 1, "bar": 2},
            {"merge_keys": ["foo"], "keys": ["bar"]},
            None,
            """
                INSERT INTO schema.table AS t (bar, foo)
                VALUES ($1, $2)
                ON CONFLICT (bar)
                    DO UPDATE SET foo = COALESCE(t.foo, JSONB_BUILD_OBJECT()) || EXCLUDED.foo
                """,
            False,
        ),
    ],
)
def test_pg_insert_query(data, on_conflict, returning, expected, quote_columns):
    from tracktolib.pg import PGInsertQuery, PGConflictQuery, PGReturningQuery

    _returning = PGReturningQuery.load(**returning) if returning else None
    query = PGInsertQuery(
        "schema.table",
        [data],
        on_conflict=PGConflictQuery(**on_conflict),
        returning=_returning,
        quote_columns=quote_columns,
    ).query

    compare_strings(query, expected)


@pytest.mark.parametrize("async_engine", ["connection", "pool"])
@pytest.mark.usefixtures("setup_tables")
def test_insert_one(loop, aengine, apool, engine, async_engine):
    from tracktolib.pg import insert_one

    _engine = aengine if async_engine == "connection" else apool

    loop.run_until_complete(insert_one(_engine, "foo.foo", {"foo": 1}))

    db_data = fetch_all(engine, "SELECT bar, foo FROM foo.foo")
    assert db_data == [{"bar": None, "foo": 1}]


@pytest.mark.parametrize("quote_columns", [True, False])
@pytest.mark.parametrize("async_engine", ["connection", "pool"])
@pytest.mark.usefixtures("setup_tables")
def test_insert_many(loop, aengine, apool, engine, async_engine, quote_columns):
    from tracktolib.pg import insert_many

    _engine = aengine if async_engine == "connection" else apool

    loop.run_until_complete(
        insert_many(_engine, "foo.foo", [{"foo": 1}, {"bar": "hello"}], fill=True, quote_columns=quote_columns)
    )

    db_data = fetch_all(engine, "SELECT bar, foo FROM foo.foo ORDER BY foo")
    assert db_data == [{"bar": None, "foo": 1}, {"bar": "hello", "foo": None}]


@pytest.fixture()
def insert_data(engine):
    from tracktolib.pg_sync import insert_many

    data = [{"id": 1, "foo": 10, "bar": "baz"}, {"id": 2, "foo": 20, "bar": None}]
    insert_many(engine, "foo.foo", data)


@pytest.mark.parametrize(
    "setup_fn, insert_params, expected_query, expected",
    [
        pytest.param(
            None,
            {"table": "foo.foo", "item": {"id": 1, "foo": 1}, "on_conflict": Conflict(keys=["id"])},
            "SELECT bar, foo FROM foo.foo WHERE id = 1",
            [{"bar": "baz", "foo": 1}],
            id="on_conflict",
        ),
        pytest.param(
            lambda engine: (insert_one(engine, "foo.baz", {"id": 0, "baz": {"foo": 1}})),
            {
                "table": "foo.baz",
                "item": {"id": 0, "baz": {"bar": "hello"}},
                "on_conflict": PGConflictQuery(keys=["id"], merge_keys=["baz"]),
            },
            "SELECT bar, baz FROM foo.baz WHERE id = 0",
            [{"bar": None, "baz": {"foo": 1, "bar": "hello"}}],
            id="on conflict merge",
        ),
        pytest.param(
            lambda engine: (insert_one(engine, "foo.baz", {"id": 0, "bar": {"foo": 1}})),
            {
                "table": "foo.baz",
                "item": {"id": 0, "bar": {"foo": 2}},
                "on_conflict": PGConflictQuery(keys=["id"], merge_keys=["baz"]),
            },
            "SELECT bar, baz FROM foo.baz WHERE id = 0",
            [{"bar": {"foo": 2}, "baz": None}],
            id="on conflict merge no left",
        ),
        pytest.param(
            lambda engine: (insert_one(engine, "foo.baz", {"id": 0, "baz": {"foo": 2}})),
            {
                "table": "foo.baz",
                "item": {"id": 0, "baz": None},
                "on_conflict": PGConflictQuery(keys=["id"], merge_keys=["baz"]),
            },
            "SELECT bar, baz FROM foo.baz WHERE id = 0",
            [{"bar": None, "baz": None}],
            id="on conflict merge set None",
        ),
    ],
)
@pytest.mark.usefixtures("setup_tables", "insert_data")
def test_insert_conflict_one(loop, aengine, engine, setup_fn, insert_params, expected_query, expected):
    from tracktolib.pg_sync import fetch_all
    from tracktolib.pg import insert_one

    if setup_fn is not None:
        setup_fn(engine)

    loop.run_until_complete(insert_one(aengine, **insert_params))
    db_data = fetch_all(engine, expected_query)
    assert db_data == expected


@pytest.mark.usefixtures("setup_tables", "insert_data")
def test_insert_conflict_many(loop, aengine, engine):
    from tracktolib.pg import insert_many, Conflict

    data = [{"id": 1, "foo": 1}, {"id": 2, "bar": "hello"}]
    loop.run_until_complete(
        insert_many(aengine, "foo.foo", data, fill=True, on_conflict=Conflict(keys=["id"], ignore_keys=["foo"]))
    )
    db_data = fetch_all(engine, "SELECT bar, foo FROM foo.foo ORDER BY foo")
    assert [{"bar": "baz", "foo": 10}, {"bar": "hello", "foo": 20}] == db_data


@pytest.mark.parametrize("on_conflict", [None, "ON CONFLICT DO NOTHING"])
@pytest.mark.usefixtures("setup_tables")
def test_insert_one_returning_one(loop, aengine, engine, on_conflict):
    from tracktolib.pg import insert_returning

    new_id = loop.run_until_complete(
        insert_returning(aengine, "foo.foo", {"id": 1, "foo": 1}, returning="id", on_conflict=on_conflict)
    )
    assert new_id is not None

    db_data = fetch_all(engine, "SELECT bar, foo FROM foo.foo WHERE id = %s", new_id)
    assert db_data == [{"bar": None, "foo": 1}]


@pytest.mark.usefixtures("setup_tables")
def test_insert_one_returning_many(loop, aengine, engine):
    from tracktolib.pg import insert_returning

    returned_value = loop.run_until_complete(
        insert_returning(aengine, "foo.foo", {"id": 1, "foo": 1}, returning=["id", "bar"])
    )
    returned_value = dict(returned_value)
    assert returned_value.pop("id") is not None
    assert returned_value == {"bar": None}
    returned_values = loop.run_until_complete(insert_returning(aengine, "foo.foo", {"id": 2, "foo": 2}, returning="*"))
    assert_equals(dict(returned_values), {"id": 2, "foo": 2, "bar": None})


@pytest.fixture()
def insert_iterate_data(engine):
    from tracktolib.pg_sync import insert_many

    data = [{"id": i, "foo": 10, "bar": "baz"} for i in range(10)]
    insert_many(engine, "foo.foo", data)


@pytest.mark.usefixtures("setup_tables", "insert_iterate_data")
def test_iterate_pg(aengine, loop):
    from tracktolib.pg import iterate_pg
    from tracktolib.tests import assert_equals

    output = []

    async def _test():
        nonlocal output
        query = "SELECT id FROM foo.foo"
        async for c in iterate_pg(aengine, query, from_offset=2, chunk_size=3):
            output.append([dict(x) for x in c])

    loop.run_until_complete(_test())
    expected = [[{"id": 2}, {"id": 3}, {"id": 4}], [{"id": 5}, {"id": 6}, {"id": 7}], [{"id": 8}, {"id": 9}]]

    assert_equals(output, expected)


@pytest.mark.usefixtures("setup_tables")
def test_upload_csv(aengine, loop, static_dir, engine):
    from tracktolib.pg import upsert_csv
    from tracktolib.tests import assert_equals

    file = static_dir / "test.csv"
    file2 = static_dir / "test2.csv"

    loop.run_until_complete(upsert_csv(aengine, file, schema="foo", table="bar"))
    db = fetch_all(engine, "SELECT * FROM foo.bar ORDER BY foo")
    expected = [{"bar": "2", "foo": 1}]
    assert_equals(db, expected)
    # Run again works with on conflict ignore
    loop.run_until_complete(upsert_csv(aengine, file, schema="foo", table="bar"))
    # Run again works with update
    loop.run_until_complete(upsert_csv(aengine, file, schema="foo", table="bar", on_conflict_keys=["foo"]))
    # Run again works with update and other file
    loop.run_until_complete(
        upsert_csv(
            aengine,
            file2,
            schema="foo",
            table="bar",
            on_conflict_keys=["foo"],
            delimiter=";",
            col_names=["foo", "bar"],
            skip_header=True,
        )
    )
    db = fetch_all(engine, "SELECT * FROM foo.bar ORDER BY foo")
    expected = [{"bar": "11", "foo": 1}, {"bar": "22", "foo": 2}]
    assert_equals(db, expected)


@pytest.mark.usefixtures("setup_tables", "insert_iterate_data")
def test_fetch_count(aengine, loop):
    from tracktolib.pg import fetch_count

    count = loop.run_until_complete(fetch_count(aengine, "SELECT 1 FROM foo.foo"))
    assert count == 10


@pytest.mark.parametrize(
    "data,params,expected",
    [
        (
            {"foo": 1},
            {"start_from": 0, "where": None, "returning": None},
            {"query": "UPDATE schema.table t SET foo = $1", "values": [1]},
        ),
        (
            {"foo": 1},
            {"start_from": 1, "where": "WHERE bar = $1", "returning": None},
            {"query": "UPDATE schema.table t SET foo = $2 WHERE bar = $1", "values": [1]},
        ),
        (
            {"foo": 1},
            {"returning": ["foo"]},
            {"query": "UPDATE schema.table t SET foo = $1 RETURNING foo", "values": [1]},
        ),
        (
            {"foo": 1, "id": 1},
            {"where_keys": ["id"]},
            {"query": "UPDATE schema.table t SET foo = $1 WHERE id = $2", "values": [1, 1]},
        ),
        (
            {"foo": 1, "id": 1},
            {"where_keys": ["id"], "return_keys": True},
            {"query": "UPDATE schema.table t SET foo = $1 WHERE id = $2 RETURNING foo", "values": [1, 1]},
        ),
        (
            {"foo": 2, "id": 1, "bar": 3},
            {"where_keys": ["id", "bar"], "return_keys": True},
            {
                "query": "UPDATE schema.table t SET foo = $1 WHERE bar = $2 AND id = $3 RETURNING foo",
                "values": [2, 3, 1],
            },
        ),
        (
            {"foo": 2, "id": 1, "bar": 3},
            {"where_keys": ["bar", "id"], "return_keys": True},
            {
                "query": "UPDATE schema.table t SET foo = $1 WHERE bar = $2 AND id = $3 RETURNING foo",
                "values": [2, 3, 1],
            },
        ),
        (
            {"bar": {"foo": 1}},
            {"merge_keys": ["bar"]},
            {
                "query": "UPDATE schema.table t SET bar = COALESCE(t.bar, JSONB_BUILD_OBJECT()) || $1",
                "values": [{"foo": 1}],
            },
        ),
    ],
)
def test_pg_update_query(data, params, expected):
    from tracktolib.pg import PGUpdateQuery

    query = PGUpdateQuery("schema.table", [data], **params)
    compare_strings(query.query, expected["query"])
    assert query.values == expected["values"]


@pytest.mark.parametrize(
    "setup_fn,params,expected_query,expected",
    [
        pytest.param(
            None,
            {"table": "foo.foo", "item": {"foo": 1, "id": 1}, "keys": ["id"]},
            "SELECT * FROM foo.foo WHERE id = 1",
            [{"bar": "baz", "foo": 1, "id": 1}],
            id="default",
        ),
        pytest.param(
            lambda engine: insert_one(engine, "foo.baz", {"id": 0, "baz": {"foo": 1}}),
            {"table": "foo.baz", "item": {"baz": {"hello": "world"}, "id": 0}, "keys": ["id"], "merge_keys": ["baz"]},
            "SELECT * FROM foo.baz WHERE id = 0",
            [{"bar": None, "baz": {"foo": 1, "hello": "world"}, "id": 0}],
            id="merge keys",
        ),
        pytest.param(
            lambda engine: insert_one(engine, "foo.baz", {"id": 0, "baz": {"foo": 1}}),
            {"table": "foo.baz", "item": {"bar": {"foo": 2}, "id": 0}, "keys": ["id"], "merge_keys": ["baz"]},
            "SELECT * FROM foo.baz WHERE id = 0",
            [{"bar": {"foo": 2}, "baz": {"foo": 1}, "id": 0}],
            id="merge keys missing key",
        ),
    ],
)
@pytest.mark.usefixtures("setup_tables", "insert_data")
def test_update_one(aengine, loop, engine, setup_fn, params, expected_query, expected):
    from tracktolib.pg import update_one

    if setup_fn:
        setup_fn(engine)

    loop.run_until_complete(update_one(aengine, **params))
    db_data = fetch_all(engine, expected_query)
    assert db_data == expected


@pytest.mark.usefixtures("setup_tables", "insert_data")
def test_update_one_keys(aengine, loop, engine):
    from tracktolib.pg import update_one

    loop.run_until_complete(update_one(aengine, "foo.foo", {"foo": 2, "id": 1}, keys=["id"]))


@pytest.mark.parametrize(
    "param_args,params_kwargs,expected",
    [
        ([{"foo": 1}, 1], {"returning": "foo", "start_from": 1, "where": "WHERE id =$1"}, 1),
        ([{"foo": 2, "id": 1}], {"returning": "foo", "keys": ["id"]}, 2),
        ([{"foo": 2, "id": 1}], {"returning": "foo", "keys": ["id"]}, 2),
        ([{"foo": 2, "id": 1}], {"return_keys": True, "keys": ["id"]}, {"foo": 2}),
    ],
)
@pytest.mark.usefixtures("setup_tables", "insert_data")
def test_update_one_returning(aengine, loop, engine, param_args, params_kwargs, expected):
    from tracktolib.pg import update_returning

    value = loop.run_until_complete(update_returning(aengine, "foo.foo", *param_args, **params_kwargs))
    assert value if not isinstance(value, asyncpg.Record) else dict(value) == expected


async def check_update_one_types(aengine):
    """This won't be called, only used to check types"""
    from tracktolib.pg import update_returning

    await update_returning(aengine, "foo.foo", {"foo": 1}, returning="foo")
    await update_returning(aengine, "foo.foo", {"foo": 1}, return_keys=True)


def test_safe_pg(aengine, loop):
    from tracktolib.pg import insert_one, safe_pg, PGError, PGException, safe_pg_context

    @safe_pg([PGError("bar_unique", "Another bar value exists")])
    async def insert_bar():
        await insert_one(aengine, "foo.generated", {"bar": "foo"})

    loop.run_until_complete(insert_bar())
    with pytest.raises(PGException):
        loop.run_until_complete(insert_bar())

    with pytest.raises(PGException):
        with safe_pg_context([PGError("bar_unique", "Another bar value exists")]):
            loop.run_until_complete(insert_bar())


@pytest.mark.parametrize(
    "setup_fn,params,expected_query,expected",
    [
        pytest.param(
            None,
            {"table": "foo.foo", "items": [{"id": 1, "foo": 1}, {"id": 2, "foo": 22}], "keys": ["id"]},
            "SELECT bar, foo, id FROM foo.foo WHERE id IN (1, 2) ORDER BY id",
            [{"bar": "baz", "foo": 1, "id": 1}, {"bar": None, "foo": 22, "id": 2}],
            id="default",
        ),
    ],
)
@pytest.mark.usefixtures("setup_tables", "insert_data")
def test_update_many(aengine, loop, engine, setup_fn, params, expected_query, expected):
    from tracktolib.pg import update_many

    if setup_fn:
        setup_fn(engine)

    loop.run_until_complete(update_many(aengine, **params))
    db_data = fetch_all(engine, expected_query)
    assert db_data == expected


# Tests for query_callback feature


@pytest.mark.usefixtures("setup_tables")
def test_insert_one_query_callback(aengine, loop, engine):
    """Test that query_callback is invoked for insert_one and receives the correct query object"""
    from tracktolib.pg import insert_one, PGInsertQuery

    callback_invoked = []

    def callback(query: PGInsertQuery):
        callback_invoked.append(True)
        assert isinstance(query, PGInsertQuery)
        assert query.table == "foo.foo"
        assert query.items == [{"foo": 1}]
        assert "INSERT INTO foo.foo" in query.query

    loop.run_until_complete(insert_one(aengine, "foo.foo", {"foo": 1}, query_callback=callback))

    assert len(callback_invoked) == 1
    db_data = fetch_all(engine, "SELECT bar, foo FROM foo.foo")
    assert db_data == [{"bar": None, "foo": 1}]


@pytest.mark.usefixtures("setup_tables")
def test_insert_one_query_callback_with_conflict(aengine, loop, engine):
    """Test query_callback with on_conflict parameter"""
    from tracktolib.pg import insert_one, PGInsertQuery, Conflict

    callback_queries = []

    def callback(query: PGInsertQuery):
        callback_queries.append(query)
        assert query.has_conflict
        assert "ON CONFLICT" in query.query

    # Insert initial data
    loop.run_until_complete(insert_one(aengine, "foo.foo", {"id": 1, "foo": 10}))
    
    # Insert with conflict
    loop.run_until_complete(
        insert_one(
            aengine,
            "foo.foo",
            {"id": 1, "foo": 20},
            on_conflict=Conflict(keys=["id"]),
            query_callback=callback
        )
    )

    assert len(callback_queries) == 1
    assert callback_queries[0].on_conflict is not None
    db_data = fetch_all(engine, "SELECT foo FROM foo.foo WHERE id = 1")
    assert db_data == [{"foo": 20}]


@pytest.mark.usefixtures("setup_tables")
def test_insert_one_query_callback_none(aengine, loop, engine):
    """Test that passing None as query_callback works correctly"""
    from tracktolib.pg import insert_one

    # Should not raise any errors
    loop.run_until_complete(insert_one(aengine, "foo.foo", {"foo": 1}, query_callback=None))

    db_data = fetch_all(engine, "SELECT bar, foo FROM foo.foo")
    assert db_data == [{"bar": None, "foo": 1}]


@pytest.mark.usefixtures("setup_tables")
def test_insert_many_query_callback(aengine, loop, engine):
    """Test that query_callback is invoked for insert_many"""
    from tracktolib.pg import insert_many, PGInsertQuery

    callback_data = []

    def callback(query: PGInsertQuery):
        callback_data.append({
            "table": query.table,
            "items": query.items,
            "query": query.query
        })
        assert isinstance(query, PGInsertQuery)
        assert len(query.items) == 2

    items = [{"foo": 1}, {"bar": "hello"}]
    loop.run_until_complete(
        insert_many(aengine, "foo.foo", items, fill=True, query_callback=callback)
    )

    assert len(callback_data) == 1
    assert callback_data[0]["table"] == "foo.foo"
    assert len(callback_data[0]["items"]) == 2
    
    db_data = fetch_all(engine, "SELECT bar, foo FROM foo.foo ORDER BY foo")
    assert db_data == [{"bar": None, "foo": 1}, {"bar": "hello", "foo": None}]


@pytest.mark.usefixtures("setup_tables")
def test_insert_many_query_callback_with_conflict(aengine, loop, engine):
    """Test query_callback for insert_many with on_conflict"""
    from tracktolib.pg import insert_many, PGInsertQuery, Conflict

    callback_invoked = []

    def callback(query: PGInsertQuery):
        callback_invoked.append(query)
        assert query.has_conflict
        assert "ON CONFLICT" in query.query

    # Insert initial data
    loop.run_until_complete(insert_many(aengine, "foo.foo", [{"id": 1, "foo": 10}, {"id": 2, "foo": 20}]))
    
    # Insert with conflict
    items = [{"id": 1, "foo": 11}, {"id": 2, "foo": 22}]
    loop.run_until_complete(
        insert_many(
            aengine,
            "foo.foo",
            items,
            on_conflict=Conflict(keys=["id"]),
            query_callback=callback
        )
    )

    assert len(callback_invoked) == 1


@pytest.mark.usefixtures("setup_tables")
def test_insert_returning_query_callback_single_value(aengine, loop, engine):
    """Test query_callback with insert_returning for single return value"""
    from tracktolib.pg import insert_returning, PGInsertQuery

    callback_data = []

    def callback(query: PGInsertQuery):
        callback_data.append(query)
        assert query.is_returning
        assert "RETURNING" in query.query
        assert query.returning is not None

    new_id = loop.run_until_complete(
        insert_returning(
            aengine,
            "foo.foo",
            {"id": 1, "foo": 100},
            returning="id",
            query_callback=callback
        )
    )

    assert new_id == 1
    assert len(callback_data) == 1
    assert callback_data[0].returning.returning_ids == ["id"]


@pytest.mark.usefixtures("setup_tables")
def test_insert_returning_query_callback_multiple_values(aengine, loop, engine):
    """Test query_callback with insert_returning for multiple return values"""
    from tracktolib.pg import insert_returning, PGInsertQuery

    callback_queries = []

    def callback(query: PGInsertQuery):
        callback_queries.append(query)
        assert query.is_returning
        assert list(query.returning.returning_ids) == ["id", "foo"]

    result = loop.run_until_complete(
        insert_returning(
            aengine,
            "foo.foo",
            {"id": 1, "foo": 42},
            returning=["id", "foo"],
            query_callback=callback
        )
    )

    assert len(callback_queries) == 1
    assert dict(result) == {"id": 1, "foo": 42}


@pytest.mark.usefixtures("setup_tables")
def test_insert_returning_query_callback_with_conflict(aengine, loop, engine):
    """Test query_callback with insert_returning and on_conflict"""
    from tracktolib.pg import insert_returning, PGInsertQuery

    callback_data = []

    def callback(query: PGInsertQuery):
        callback_data.append({
            "has_conflict": query.has_conflict,
            "is_returning": query.is_returning,
            "query": query.query
        })

    # Insert initial data
    loop.run_until_complete(insert_returning(aengine, "foo.foo", {"id": 1, "foo": 10}, returning="id"))
    
    # Insert with conflict
    new_id = loop.run_until_complete(
        insert_returning(
            aengine,
            "foo.foo",
            {"id": 1, "foo": 20},
            returning="id",
            on_conflict="ON CONFLICT (id) DO UPDATE SET foo = EXCLUDED.foo",
            query_callback=callback
        )
    )

    assert len(callback_data) == 1
    assert callback_data[0]["has_conflict"]
    assert callback_data[0]["is_returning"]
    assert "ON CONFLICT" in callback_data[0]["query"]
    assert "RETURNING" in callback_data[0]["query"]


@pytest.mark.usefixtures("setup_tables", "insert_data")
def test_update_one_query_callback(aengine, loop, engine):
    """Test that query_callback is invoked for update_one"""
    from tracktolib.pg import update_one, PGUpdateQuery

    callback_queries = []

    def callback(query: PGUpdateQuery):
        callback_queries.append(query)
        assert isinstance(query, PGUpdateQuery)
        assert query.table == "foo.foo"
        assert "UPDATE foo.foo" in query.query

    loop.run_until_complete(
        update_one(
            aengine,
            "foo.foo",
            {"foo": 999, "id": 1},
            keys=["id"],
            query_callback=callback
        )
    )

    assert len(callback_queries) == 1
    assert callback_queries[0].where_keys == ["id"]
    
    db_data = fetch_all(engine, "SELECT foo FROM foo.foo WHERE id = 1")
    assert db_data == [{"foo": 999}]


@pytest.mark.usefixtures("setup_tables", "insert_data")
def test_update_one_query_callback_with_merge(aengine, loop, engine):
    """Test query_callback with update_one using merge_keys"""
    from tracktolib.pg import update_one, PGUpdateQuery
    from tracktolib.pg_sync import insert_one

    # Setup: insert JSONB data
    insert_one(engine, "foo.baz", {"id": 1, "baz": {"key1": "value1"}})
    engine.commit()

    callback_data = []

    def callback(query: PGUpdateQuery):
        callback_data.append({
            "merge_keys": query.merge_keys,
            "query": query.query
        })
        assert "COALESCE" in query.query
        assert "JSONB_BUILD_OBJECT()" in query.query

    loop.run_until_complete(
        update_one(
            aengine,
            "foo.baz",
            {"id": 1, "baz": {"key2": "value2"}},
            keys=["id"],
            merge_keys=["baz"],
            query_callback=callback
        )
    )

    assert len(callback_data) == 1
    assert callback_data[0]["merge_keys"] == ["baz"]
    
    db_data = fetch_all(engine, "SELECT baz FROM foo.baz WHERE id = 1")
    assert db_data[0]["baz"] == {"key1": "value1", "key2": "value2"}


@pytest.mark.usefixtures("setup_tables", "insert_data")
def test_update_one_query_callback_none(aengine, loop, engine):
    """Test that update_one works correctly when query_callback is None"""
    from tracktolib.pg import update_one

    # Should not raise any errors
    loop.run_until_complete(
        update_one(aengine, "foo.foo", {"foo": 555, "id": 1}, keys=["id"], query_callback=None)
    )

    db_data = fetch_all(engine, "SELECT foo FROM foo.foo WHERE id = 1")
    assert db_data == [{"foo": 555}]


@pytest.mark.usefixtures("setup_tables", "insert_data")
def test_update_returning_query_callback_single_value(aengine, loop, engine):
    """Test query_callback with update_returning for single return value"""
    from tracktolib.pg import update_returning, PGUpdateQuery

    callback_data = []

    def callback(query: PGUpdateQuery):
        callback_data.append(query)
        assert isinstance(query, PGUpdateQuery)
        assert query.returning == ["foo"]
        assert "RETURNING" in query.query

    result = loop.run_until_complete(
        update_returning(
            aengine,
            "foo.foo",
            {"foo": 777, "id": 1},
            returning="foo",
            keys=["id"],
            query_callback=callback
        )
    )

    assert result == 777
    assert len(callback_data) == 1


@pytest.mark.usefixtures("setup_tables", "insert_data")
def test_update_returning_query_callback_multiple_values(aengine, loop, engine):
    """Test query_callback with update_returning for multiple return values"""
    from tracktolib.pg import update_returning, PGUpdateQuery

    callback_queries = []

    def callback(query: PGUpdateQuery):
        callback_queries.append(query)
        assert query.returning == ["foo", "bar"]

    result = loop.run_until_complete(
        update_returning(
            aengine,
            "foo.foo",
            {"foo": 888, "id": 1},
            returning=["foo", "bar"],
            keys=["id"],
            query_callback=callback
        )
    )

    assert len(callback_queries) == 1
    result_dict = dict(result)
    assert result_dict["foo"] == 888
    assert result_dict["bar"] == "baz"


@pytest.mark.usefixtures("setup_tables", "insert_data")
def test_update_returning_query_callback_return_keys(aengine, loop, engine):
    """Test query_callback with update_returning using return_keys=True"""
    from tracktolib.pg import update_returning, PGUpdateQuery

    callback_data = []

    def callback(query: PGUpdateQuery):
        callback_data.append({
            "return_keys": query.return_keys,
            "where_keys": query.where_keys,
            "query": query.query
        })

    result = loop.run_until_complete(
        update_returning(
            aengine,
            "foo.foo",
            {"foo": 333, "id": 1},
            return_keys=True,
            keys=["id"],
            query_callback=callback
        )
    )

    assert len(callback_data) == 1
    assert callback_data[0]["return_keys"] is True
    assert callback_data[0]["where_keys"] == ["id"]
    assert dict(result) == {"foo": 333}


@pytest.mark.usefixtures("setup_tables", "insert_data")
def test_update_many_query_callback(aengine, loop, engine):
    """Test that query_callback is invoked for update_many"""
    from tracktolib.pg import update_many, PGUpdateQuery

    callback_queries = []

    def callback(query: PGUpdateQuery):
        callback_queries.append(query)
        assert isinstance(query, PGUpdateQuery)
        assert len(query.items) == 2
        assert query.table == "foo.foo"

    items = [{"id": 1, "foo": 111}, {"id": 2, "foo": 222}]
    loop.run_until_complete(
        update_many(
            aengine,
            "foo.foo",
            items,
            keys=["id"],
            query_callback=callback
        )
    )

    assert len(callback_queries) == 1
    
    db_data = fetch_all(engine, "SELECT foo, id FROM foo.foo WHERE id IN (1, 2) ORDER BY id")
    assert db_data == [{"foo": 111, "id": 1}, {"foo": 222, "id": 2}]


@pytest.mark.usefixtures("setup_tables", "insert_data")
def test_update_many_query_callback_none(aengine, loop, engine):
    """Test that update_many works correctly when query_callback is None"""
    from tracktolib.pg import update_many

    items = [{"id": 1, "foo": 444}, {"id": 2, "foo": 555}]
    
    # Should not raise any errors
    loop.run_until_complete(
        update_many(aengine, "foo.foo", items, keys=["id"], query_callback=None)
    )

    db_data = fetch_all(engine, "SELECT foo, id FROM foo.foo WHERE id IN (1, 2) ORDER BY id")
    assert db_data == [{"foo": 444, "id": 1}, {"foo": 555, "id": 2}]


@pytest.mark.usefixtures("setup_tables")
def test_query_callback_inspection_and_logging(aengine, loop, engine):
    """Test using query_callback for query inspection and logging"""
    from tracktolib.pg import insert_one, insert_many, update_one, PGInsertQuery, PGUpdateQuery

    logged_queries = []

    def log_callback(query):
        """Simulate logging queries for debugging/monitoring"""
        logged_queries.append({
            "type": type(query).__name__,
            "table": query.table,
            "query_sql": query.query,
            "values": query.values,
            "item_count": len(query.items)
        })

    # Insert one with logging
    loop.run_until_complete(
        insert_one(aengine, "foo.foo", {"foo": 1}, query_callback=log_callback)
    )

    # Insert many with logging
    loop.run_until_complete(
        insert_many(aengine, "foo.foo", [{"foo": 2}, {"foo": 3}], query_callback=log_callback)
    )

    # Update with logging
    loop.run_until_complete(
        update_one(aengine, "foo.foo", {"foo": 10, "id": 1}, keys=["id"], query_callback=log_callback)
    )

    assert len(logged_queries) == 3
    assert logged_queries[0]["type"] == "PGInsertQuery"
    assert logged_queries[0]["item_count"] == 1
    assert logged_queries[1]["type"] == "PGInsertQuery"
    assert logged_queries[1]["item_count"] == 2
    assert logged_queries[2]["type"] == "PGUpdateQuery"
    assert logged_queries[2]["item_count"] == 1


@pytest.mark.usefixtures("setup_tables")
def test_query_callback_exception_handling(aengine, loop, engine):
    """Test that exceptions in query_callback are propagated correctly"""
    from tracktolib.pg import insert_one

    def failing_callback(query):
        raise ValueError("Callback intentionally failed")

    with pytest.raises(ValueError, match="Callback intentionally failed"):
        loop.run_until_complete(
            insert_one(aengine, "foo.foo", {"foo": 1}, query_callback=failing_callback)
        )

    # Verify no data was inserted due to the error
    db_data = fetch_all(engine, "SELECT * FROM foo.foo")
    assert db_data == []


@pytest.mark.usefixtures("setup_tables")
def test_query_callback_type_hints(aengine, loop, engine):
    """Test that query_callback receives properly typed query objects"""
    from tracktolib.pg import (
        insert_one, insert_many, insert_returning,
        update_one, update_returning, update_many,
        PGInsertQuery, PGUpdateQuery
    )

    insert_callbacks = []
    update_callbacks = []

    def insert_callback(query: PGInsertQuery):
        insert_callbacks.append(type(query))
        assert hasattr(query, "has_conflict")
        assert hasattr(query, "is_returning")

    def update_callback(query: PGUpdateQuery):
        update_callbacks.append(type(query))
        assert hasattr(query, "where_keys")
        assert hasattr(query, "merge_keys")

    # Test insert functions
    loop.run_until_complete(insert_one(aengine, "foo.foo", {"foo": 1}, query_callback=insert_callback))
    loop.run_until_complete(insert_many(aengine, "foo.foo", [{"foo": 2}], query_callback=insert_callback))
    loop.run_until_complete(
        insert_returning(aengine, "foo.foo", {"id": 3, "foo": 3}, returning="id", query_callback=insert_callback)
    )

    # Test update functions
    loop.run_until_complete(
        update_one(aengine, "foo.foo", {"foo": 10, "id": 1}, keys=["id"], query_callback=update_callback)
    )
    loop.run_until_complete(
        update_returning(
            aengine, "foo.foo", {"foo": 20, "id": 1}, returning="foo", keys=["id"], query_callback=update_callback
        )
    )
    loop.run_until_complete(
        update_many(aengine, "foo.foo", [{"foo": 30, "id": 1}], keys=["id"], query_callback=update_callback)
    )

    assert len(insert_callbacks) == 3
    assert all(cb.__name__ == "PGInsertQuery" for cb in insert_callbacks)
    assert len(update_callbacks) == 3
    assert all(cb.__name__ == "PGUpdateQuery" for cb in update_callbacks)


@pytest.mark.usefixtures("setup_tables")
def test_query_callback_access_to_query_properties(aengine, loop, engine):
    """Test that query_callback can access all relevant query properties"""
    from tracktolib.pg import insert_one, update_one, PGInsertQuery, PGUpdateQuery

    insert_properties = {}
    update_properties = {}

    def insert_callback(query: PGInsertQuery):
        insert_properties["table"] = query.table
        insert_properties["items"] = query.items
        insert_properties["keys"] = query.keys
        insert_properties["columns"] = query.columns
        insert_properties["values"] = query.values
        insert_properties["fill"] = query.fill
        insert_properties["quote_columns"] = query.quote_columns

    def update_callback(query: PGUpdateQuery):
        update_properties["table"] = query.table
        update_properties["items"] = query.items
        update_properties["keys"] = query.keys
        update_properties["where_keys"] = query.where_keys
        update_properties["values"] = query.values
        update_properties["start_from"] = query.start_from

    loop.run_until_complete(
        insert_one(aengine, "foo.foo", {"foo": 100}, fill=True, query_callback=insert_callback)
    )

    loop.run_until_complete(
        update_one(aengine, "foo.foo", {"foo": 200, "id": 1}, keys=["id"], query_callback=update_callback)
    )

    # Verify insert properties
    assert insert_properties["table"] == "foo.foo"
    assert insert_properties["items"] == [{"foo": 100, "bar": None}]
    assert "foo" in insert_properties["keys"]
    assert insert_properties["fill"] is True

    # Verify update properties
    assert update_properties["table"] == "foo.foo"
    assert update_properties["where_keys"] == ["id"]
    assert update_properties["items"] == [{"foo": 200, "id": 1}]


@pytest.mark.parametrize("async_engine", ["connection", "pool"])
@pytest.mark.usefixtures("setup_tables")
def test_query_callback_works_with_both_connection_types(aengine, apool, loop, engine, async_engine):
    """Test that query_callback works with both Connection and Pool"""
    from tracktolib.pg import insert_one, PGInsertQuery

    _engine = aengine if async_engine == "connection" else apool

    callback_invoked = []

    def callback(query: PGInsertQuery):
        callback_invoked.append(True)

    loop.run_until_complete(
        insert_one(_engine, "foo.foo", {"foo": 123}, query_callback=callback)
    )

    assert len(callback_invoked) == 1
    db_data = fetch_all(engine, "SELECT foo FROM foo.foo")
    assert db_data == [{"foo": 123}]
