# Tracktolib

[![Python versions](https://img.shields.io/pypi/pyversions/tracktolib)](https://pypi.python.org/pypi/tracktolib)
[![Latest PyPI version](https://img.shields.io/pypi/v/tracktolib?logo=pypi)](https://pypi.python.org/pypi/tracktolib)
[![CircleCI](https://circleci.com/gh/Tracktor/tracktolib/tree/master.svg?style=shield)](https://app.circleci.com/pipelines/github/Tracktor/tracktolib?branch=master)

Utility library for python

# Installation

You can choose to not install all the dependencies by specifying
the [extra](https://python-poetry.org/docs/cli/#options-4) parameter such as:

```bash
poetry add tracktolib@latest -E pg-sync -E tests --group dev 
```

Here we only install the utilities using `psycopg` (pg-sync) and `deepdiff` (tests) for the dev environment.

# Utilities

- **log**

Utility functions for logging.

```python
import logging
from tracktolib.logs import init_logging

logger = logging.getLogger()
formatter, stream_handler = init_logging(logger, 'json', version='0.0.1')
```

- **pg**

Utility functions for [asyncpg](https://github.com/MagicStack/asyncpg)

- **pg-sync**

Utility functions based on psycopg such as `fetch_one`, `insert_many`, `fetch_count` ...

To use the functions, create a `Connection` using psycopg: `conn = psycopg2.connect()`

*fetch_one*

```python
from pg.pg_sync import (
    insert_many, fetch_one, fetch_count, fetch_all
)

data = [
    {'foo': 'bar', 'value': 1},
    {'foo': 'baz', 'value': 2}
]
insert_many(conn, 'public.test', data)  # Will insert the 2 dict
query = 'SELECT foo from public.test order by value asc'
value = fetch_one(conn, query, required=True)  # Will return {'foo': 'bar'}, raise an error is not found
assert fetch_count(conn, 'public.test') == 2
query = 'SELECT * from public.test order by value asc'
assert fetch_all(conn, query) == data

```

- **tests**

Utility functions for testing

- **s3-minio**

Utility functions for [minio](https://min.io/docs/minio/linux/developers/python/API.html)

- **s3**

Utility functions for [aiobotocore](https://github.com/aio-libs/aiobotocore)

- **logs**

Utility functions to initialize the logging formatting and streams

- **http**

Utility functions using [httpx](https://www.python-httpx.org/)

- **api**

Utility functions using [fastapi](https://fastapi.tiangolo.com/)
