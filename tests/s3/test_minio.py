import pytest


@pytest.fixture()
def setup_minio(minio_client, s3_bucket):
    from tracktolib.s3.minio import bucket_rm

    if minio_client.bucket_exists(s3_bucket):
        bucket_rm(minio_client, s3_bucket)
    minio_client.make_bucket(s3_bucket)
    yield


@pytest.mark.usefixtures("setup_minio")
def test_upload_download_object(minio_client, static_dir, s3_bucket, tmp_path):
    from tracktolib.s3.minio import download_bucket, upload_object

    upload_object(minio_client, s3_bucket, "test.csv", static_dir / "test.csv")
    download_bucket(minio_client, s3_bucket, tmp_path)
    assert [x.name for x in tmp_path.glob("*.csv")] == ["test.csv"]


@pytest.mark.usefixtures("setup_minio")
def test_rm_bucket(minio_client, s3_bucket):
    from tracktolib.s3.minio import bucket_rm

    assert minio_client.bucket_exists(s3_bucket)
    bucket_rm(minio_client, s3_bucket)
    assert not minio_client.bucket_exists(s3_bucket)
