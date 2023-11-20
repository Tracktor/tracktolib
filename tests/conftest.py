import asyncio
from pathlib import Path

import pytest
from minio import Minio

_cur_dir = Path(__file__).parent

STATIC_DIR = _cur_dir / "static"


@pytest.fixture()
def static_dir():
    return STATIC_DIR


MINIO_URL = "localhost:9000"
MINIO_ACCESS_KEY = "foo"
MINIO_SECRET_KEY = "foobarbaz"

S3_BUCKET = "test"


@pytest.fixture()
def minio_client():
    client = Minio(MINIO_URL, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)

    yield client


@pytest.fixture(scope="function")
def s3_bucket():
    return S3_BUCKET


@pytest.fixture(scope="session")
def loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.stop()
