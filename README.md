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

Here we only install the utilities using `psycopg2` (pg-sync) and `deepdiff` (tests) for the dev environment.

# Utilities

- **log**

Utility functions for logging.

- **pg-sync**

Utility functions based on psycopg2 such as `fetch_one`, `insert_many`, `fetch_count` ...

- **tests**

Utility functions for tests such as `get_uuid` (that generates a test uuid based on an integer)

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
