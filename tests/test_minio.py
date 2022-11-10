import tempfile

import pytest

from minio import Minio

S3_BUCKET = 'test'


@pytest.fixture(scope='function')
def s3_bucket():
    return S3_BUCKET


@pytest.fixture()
def minio_client():
    client = Minio(
        'localhost:9000',
        access_key='foo',
        secret_key='foobarbaz',
        secure=False
    )

    yield client


@pytest.fixture()
def setup_minio(minio_client, s3_bucket):
    from tracktolib.s3 import bucket_rm
    if minio_client.bucket_exists(s3_bucket):
        bucket_rm(minio_client, s3_bucket)
    minio_client.make_bucket(s3_bucket)
    yield


@pytest.mark.usefixtures('setup_minio')
def test_upload_download_object(minio_client, static_dir, s3_bucket,
                                tmp_path):
    from tracktolib.s3 import upload_object, download_bucket
    upload_object(minio_client, s3_bucket, 'test.log', static_dir / 'test.log')
    download_bucket(minio_client, s3_bucket, tmp_path)
    assert [x.name for x in tmp_path.glob('*.log')] == ['test.log']


@pytest.mark.usefixtures('setup_minio')
def test_rm_bucket(minio_client, s3_bucket):
    from tracktolib.s3 import bucket_rm
    assert minio_client.bucket_exists(s3_bucket)
    bucket_rm(minio_client, s3_bucket)
    assert not minio_client.bucket_exists(s3_bucket)
