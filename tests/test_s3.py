import contextlib

import pytest
from aiobotocore.session import get_session

from .conftest import MINIO_URL, MINIO_SECRET_KEY, MINIO_ACCESS_KEY

S3_BUCKET = 'test'


@pytest.fixture(scope='function')
def s3_bucket():
    return S3_BUCKET


@contextlib.asynccontextmanager
async def get_s3_client():
    session = get_session()
    async with session.create_client('s3',
                                     endpoint_url=f'http://{MINIO_URL}',
                                     aws_secret_access_key=MINIO_SECRET_KEY,
                                     aws_access_key_id=MINIO_ACCESS_KEY,
                                     ) as client:
        yield client


@pytest.fixture()
def setup_bucket(minio_client, s3_bucket):
    from tracktolib.s3.minio import bucket_rm
    minio_client.make_bucket(s3_bucket)
    yield
    bucket_rm(minio_client, s3_bucket)


@pytest.mark.usefixtures('setup_bucket')
def test_upload_list_file(s3_bucket, loop,
                          static_dir,
                          minio_client):
    from tracktolib.s3.s3 import upload_file, list_files

    async def _test():
        async with get_s3_client() as client:
            await upload_file(client, s3_bucket,
                              static_dir / 'test.csv',
                              'foo/test.csv')
            bucket_data = await list_files(client, s3_bucket, 'foo')
            assert [x['Key'] for x in bucket_data] == ['foo/test.csv']

    loop.run_until_complete(_test())
