import botocore.session
import contextlib
import minio.error
import niquests
import pytest
from botocore.config import Config

from tests.conftest import MINIO_URL, MINIO_SECRET_KEY, MINIO_ACCESS_KEY

S3_BUCKET = "test-niquests"

S3_CONFIG = Config(signature_version="s3v4", s3={"addressing_style": "path"})


@pytest.fixture(scope="function")
def s3_bucket():
    return S3_BUCKET


@pytest.fixture(scope="function")
def s3_client(loop):
    from tracktolib.s3.niquests import S3Session

    client = S3Session(
        endpoint_url=f"http://{MINIO_URL}",
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        config=S3_CONFIG,
    )
    loop.run_until_complete(client.__aenter__())
    yield client
    loop.run_until_complete(client.__aexit__(None, None, None))


@contextlib.contextmanager
def get_botocore_client():
    session = botocore.session.Session()
    client = session.create_client(
        "s3",
        endpoint_url=f"http://{MINIO_URL}",
        aws_secret_access_key=MINIO_SECRET_KEY,
        aws_access_key_id=MINIO_ACCESS_KEY,
        region_name="us-east-1",
        config=S3_CONFIG,
    )
    yield client
    client.close()


@pytest.fixture()
def setup_bucket(minio_client, s3_bucket):
    from tracktolib.s3.minio import bucket_rm

    try:
        bucket_rm(minio_client, s3_bucket)
    except minio.error.S3Error:
        pass
    minio_client.make_bucket(s3_bucket)
    yield
    bucket_rm(minio_client, s3_bucket)


@pytest.mark.usefixtures("setup_bucket")
class TestS3Session:
    def test_session_put_get_delete(self, s3_bucket, loop, s3_client):
        """Test S3Session wrapper class."""

        async def _test():
            # Put object
            test_data = b"Hello from S3Session!"
            resp = await s3_client.put_object(s3_bucket, "session-test/hello.txt", test_data, acl=None)
            assert resp.status_code == 200

            # Get object
            result = await s3_client.get_object(s3_bucket, "session-test/hello.txt")
            assert result == test_data

            # List files
            files = [f async for f in s3_client.list_files(s3_bucket, "session-test/")]
            assert len(files) == 1
            assert files[0].get("Key") == "session-test/hello.txt"

            # Delete object
            resp = await s3_client.delete_object(s3_bucket, "session-test/hello.txt")
            assert resp.status_code == 204

        loop.run_until_complete(_test())

    def test_session_streaming_small_file(self, s3_bucket, loop, s3_client):
        """Test S3Session streaming upload with small file."""

        async def _test():
            test_data = b"Small streaming content"

            async def data_stream():
                yield test_data

            await s3_client.file_upload(
                s3_bucket,
                "session-test/stream.txt",
                data_stream(),
                content_length=len(test_data),
            )

            result = await s3_client.get_object(s3_bucket, "session-test/stream.txt")
            assert result == test_data

        loop.run_until_complete(_test())


@pytest.mark.usefixtures("setup_bucket")
class TestS3NiquestsBasicOperations:
    @pytest.mark.parametrize(
        ("operation", "key", "data", "expected_status", "expected_content"),
        [
            pytest.param("put", "test/hello.txt", b"Hello, World!", 200, None, id="put_object"),
            pytest.param("get", "test/hello.txt", b"Hello, World!", None, b"Hello, World!", id="get_object"),
            pytest.param("get_nonexistent", "test/nonexistent.txt", None, None, None, id="get_nonexistent"),
            pytest.param("delete", "test/hello.txt", b"Hello, World!", 204, None, id="delete_object"),
        ],
    )
    def test_basic_operations(self, s3_bucket, loop, operation, key, data, expected_status, expected_content):
        from tracktolib.s3.niquests import s3_put_object, s3_get_object, s3_delete_object

        async def _test():
            with get_botocore_client() as s3:
                async with niquests.AsyncSession() as client:
                    if operation == "put":
                        resp = await s3_put_object(s3, client, s3_bucket, key, data, acl=None)
                        assert resp.status_code == expected_status
                    elif operation == "get":
                        await s3_put_object(s3, client, s3_bucket, key, data, acl=None)
                        result = await s3_get_object(s3, client, s3_bucket, key)
                        assert result == expected_content
                    elif operation == "get_nonexistent":
                        result = await s3_get_object(s3, client, s3_bucket, key)
                        assert result is None
                    elif operation == "delete":
                        await s3_put_object(s3, client, s3_bucket, key, data, acl=None)
                        resp = await s3_delete_object(s3, client, s3_bucket, key)
                        assert resp.status_code == expected_status
                        result = await s3_get_object(s3, client, s3_bucket, key)
                        assert result is None

        loop.run_until_complete(_test())

    def test_list_files(self, s3_bucket, loop):
        """Test listing files with a prefix."""
        from tracktolib.s3.niquests import s3_put_object, s3_list_files

        async def _test():
            with get_botocore_client() as s3:
                async with niquests.AsyncSession() as client:
                    # Create some test files (acl=None to avoid MinIO ACL issues)
                    await s3_put_object(s3, client, s3_bucket, "prefix/file1.txt", b"content1", acl=None)
                    await s3_put_object(s3, client, s3_bucket, "prefix/file2.txt", b"content2", acl=None)
                    await s3_put_object(s3, client, s3_bucket, "other/file3.txt", b"content3", acl=None)

                    # List with prefix (async iterator)
                    files = [f async for f in s3_list_files(s3, client, s3_bucket, "prefix/")]
                    keys = [k for f in files if (k := f.get("Key"))]
                    assert sorted(keys) == ["prefix/file1.txt", "prefix/file2.txt"]

                    # Verify S3Object typing - Size should be int
                    for f in files:
                        assert isinstance(f.get("Size"), int)

        loop.run_until_complete(_test())


@pytest.mark.usefixtures("setup_bucket")
class TestS3StreamingUpload:
    def test_multipart_upload_manual(self, s3_bucket, loop):
        """Test multipart upload with manual part uploads."""
        from tracktolib.s3.niquests import s3_multipart_upload, s3_get_object

        async def _test():
            with get_botocore_client() as s3:
                async with niquests.AsyncSession() as client:
                    key = "test/multipart-manual.bin"
                    # MinIO minimum part size is 5MB
                    part1 = b"A" * (5 * 1024 * 1024)  # 5MB
                    part2 = b"B" * (5 * 1024 * 1024)  # 5MB

                    async with s3_multipart_upload(s3, client, s3_bucket, key) as mpart:
                        await mpart.upload_part(part1)
                        await mpart.upload_part(part2)

                    # Verify uploaded content
                    result = await s3_get_object(s3, client, s3_bucket, key)
                    assert result == part1 + part2

        loop.run_until_complete(_test())

    def test_multipart_upload_abort(self, s3_bucket, loop):
        """Test aborting a multipart upload."""
        from tracktolib.s3.niquests import s3_multipart_upload, s3_get_object

        async def _test():
            with get_botocore_client() as s3:
                async with niquests.AsyncSession() as client:
                    key = "test/multipart-abort.bin"
                    part1 = b"A" * (5 * 1024 * 1024)

                    async with s3_multipart_upload(s3, client, s3_bucket, key) as mpart:
                        await mpart.upload_part(part1)
                        await mpart.fetch_abort()

                    # Verify file doesn't exist (upload was aborted)
                    result = await s3_get_object(s3, client, s3_bucket, key)
                    assert result is None

        loop.run_until_complete(_test())

    @pytest.mark.parametrize(
        ("key", "data_size", "chunk_size", "content_length"),
        [
            pytest.param("test/small-stream.txt", 18, 18, 18, id="small_file"),
            pytest.param("test/large-stream.bin", 12 * 1024 * 1024, 3 * 1024 * 1024, None, id="large_file"),
            pytest.param("test/single-part.bin", 4 * 1024 * 1024, 1024 * 1024, None, id="single_part"),
        ],
    )
    def test_s3_file_upload(self, s3_bucket, loop, key, data_size, chunk_size, content_length):
        from tracktolib.s3.niquests import s3_file_upload, s3_get_object

        async def _test():
            with get_botocore_client() as s3:
                async with niquests.AsyncSession() as client:
                    test_data = b"X" * data_size

                    async def data_stream():
                        for i in range(0, len(test_data), chunk_size):
                            yield test_data[i : i + chunk_size]

                    received_size = 0

                    def on_chunk(chunk: bytes):
                        nonlocal received_size
                        received_size += len(chunk)

                    await s3_file_upload(
                        s3,
                        client,
                        s3_bucket,
                        key,
                        data_stream(),
                        min_part_size=5 * 1024 * 1024,
                        on_chunk_received=on_chunk,
                        content_length=content_length,
                    )

                    assert received_size == data_size
                    result = await s3_get_object(s3, client, s3_bucket, key)
                    assert result == test_data

        loop.run_until_complete(_test())
