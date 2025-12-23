# Tracktolib

[![Python versions](https://img.shields.io/pypi/pyversions/tracktolib)](https://pypi.python.org/pypi/tracktolib)
[![Latest PyPI version](https://img.shields.io/pypi/v/tracktolib?logo=pypi)](https://pypi.python.org/pypi/tracktolib)
[![CircleCI](https://circleci.com/gh/Tracktor/tracktolib/tree/master.svg?style=shield)](https://app.circleci.com/pipelines/github/Tracktor/tracktolib?branch=master)

Utility library for Python 3.12+

## Installation

```bash
uv add tracktolib
```

With specific extras:

```bash
uv add tracktolib[pg,api]
```

## Modules

### logs

Utility functions to initialize logging formatting and streams.

```python
import logging
from tracktolib.logs import init_logging

logger = logging.getLogger()
formatter, stream_handler = init_logging(logger, 'json', version='0.0.1')
```

### pg

Async PostgreSQL helpers using [asyncpg](https://github.com/MagicStack/asyncpg).

```bash
uv add tracktolib[pg]
```

### pg-sync

Sync PostgreSQL helpers using [psycopg](https://www.psycopg.org/psycopg3/) (v3).

```bash
uv add tracktolib[pg-sync]
```

```python
from psycopg import connect
from tracktolib.pg_sync import insert_many, fetch_one, fetch_count, fetch_all

conn = connect('postgresql://user:pass@localhost/db')

data = [
    {'foo': 'bar', 'value': 1},
    {'foo': 'baz', 'value': 2}
]
insert_many(conn, 'public.test', data)

query = 'SELECT foo from public.test order by value asc'
value = fetch_one(conn, query, required=True)  # {'foo': 'bar'}, raises if not found

assert fetch_count(conn, 'public.test') == 2

query = 'SELECT * from public.test order by value asc'
assert fetch_all(conn, query) == data
```

### s3

Async S3 helpers using [aiobotocore](https://github.com/aio-libs/aiobotocore).

```bash
uv add tracktolib[s3]
```

### s3-minio

S3 helpers using [minio](https://min.io/docs/minio/linux/developers/python/API.html).

```bash
uv add tracktolib[s3-minio]
```

### http

HTTP client helpers using [httpx](https://www.python-httpx.org/).

```bash
uv add tracktolib[http]
```

### api

FastAPI utilities using [fastapi](https://fastapi.tiangolo.com/) and [pydantic](https://docs.pydantic.dev/).

```bash
uv add tracktolib[api]
```

### notion

Notion API helpers using [niquests](https://github.com/jawah/niquests).

```bash
uv add tracktolib[notion]
```

### tests

Testing utilities using [deepdiff](https://github.com/seperman/deepdiff).

```bash
uv add tracktolib[tests]
```