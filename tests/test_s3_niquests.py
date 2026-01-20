import contextlib

import botocore.session
import minio.error
import niquests
import pytest

from tests.conftest import MINIO_URL, MINIO_SECRET_KEY, MINIO_ACCESS_KEY

S3_BUCKET = "test-niquests"


@pytest.fixture(scope="function")
def s3_bucket():
    return S3_BUCKET


@contextlib.contextmanager
def get_botocore_client():
    session = botocore.session.Session()
    client = session.create_client(
        "s3",
        endpoint_url=f"http://{MINIO_URL}",
        aws_secret_access_key=MINIO_SECRET_KEY,
        aws_access_key_id=MINIO_ACCESS_KEY,
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


class TestGetStreamChunk:
    def test_exact_chunks(self, loop):
        """Test streaming with data that divides evenly into chunks."""
        from tracktolib.s3.niquests import get_stream_chunk

        async def _test():
            async def async_data():
                # 20 bytes total, min_size=5 should give us 4 chunks
                yield b"12345"
                yield b"67890"
                yield b"abcde"
                yield b"fghij"

            chunks = []
            async for chunk in get_stream_chunk(async_data(), min_size=5):
                chunks.append(chunk)

            # Should get 2 chunks of 10 bytes each (buffering behavior)
            total_size = sum(len(c) for c in chunks)
            assert total_size == 20
            assert all(len(c) >= 5 for c in chunks)

        loop.run_until_complete(_test())

    def test_small_final_chunk(self, loop):
        """Test streaming when final chunk is smaller than min_part_size."""
        from tracktolib.s3.niquests import get_stream_chunk

        async def _test():
            async def async_data():
                yield b"12345678901234567890"  # 20 bytes
                yield b"abc"  # 3 bytes

            chunks = []
            async for chunk in get_stream_chunk(async_data(), min_size=10):
                chunks.append(chunk)

            total_size = sum(len(c) for c in chunks)
            assert total_size == 23

        loop.run_until_complete(_test())

    def test_single_small_chunk(self, loop):
        """Test streaming with only data smaller than min_part_size."""
        from tracktolib.s3.niquests import get_stream_chunk

        async def _test():
            async def async_data():
                yield b"small"

            chunks = []
            async for chunk in get_stream_chunk(async_data(), min_size=100):
                chunks.append(chunk)

            assert len(chunks) == 1
            assert chunks[0] == b"small"

        loop.run_until_complete(_test())

    def test_empty_chunks_ignored(self, loop):
        """Test that empty chunks in the stream are ignored."""
        from tracktolib.s3.niquests import get_stream_chunk

        async def _test():
            async def async_data():
                yield b"hello"
                yield b""
                yield b"world"

            chunks = []
            async for chunk in get_stream_chunk(async_data(), min_size=5):
                chunks.append(chunk)

            total_size = sum(len(c) for c in chunks)
            assert total_size == 10

        loop.run_until_complete(_test())


@pytest.mark.usefixtures("setup_bucket")
class TestS3Session:
    def test_session_put_get_delete(self, s3_bucket, loop):
        """Test S3Session wrapper class."""
        from tracktolib.s3.niquests import S3Session

        async def _test():
            async with S3Session(
                endpoint_url=f"http://{MINIO_URL}",
                access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY,
            ) as s3:
                # Put object
                test_data = b"Hello from S3Session!"
                resp = await s3.put_object(s3_bucket, "session-test/hello.txt", test_data, acl=None)
                assert resp.status_code == 200

                # Get object
                result = await s3.get_object(s3_bucket, "session-test/hello.txt")
                assert result == test_data

                # List files
                files = s3.list_files(s3_bucket, "session-test/")
                assert len(files) == 1
                assert files[0]["Key"] == "session-test/hello.txt"

                # Delete object
                resp = await s3.delete_object(s3_bucket, "session-test/hello.txt")
                assert resp.status_code == 204

        loop.run_until_complete(_test())

    def test_session_streaming_small_file(self, s3_bucket, loop):
        """Test S3Session streaming upload with small file."""
        from tracktolib.s3.niquests import S3Session

        async def _test():
            async with S3Session(
                endpoint_url=f"http://{MINIO_URL}",
                access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY,
            ) as s3:
                test_data = b"Small streaming content"

                async def data_stream():
                    yield test_data

                await s3.file_upload(
                    s3_bucket,
                    "session-test/stream.txt",
                    data_stream(),
                    content_length=len(test_data),
                )

                result = await s3.get_object(s3_bucket, "session-test/stream.txt")
                assert result == test_data

        loop.run_until_complete(_test())


@pytest.mark.usefixtures("setup_bucket")
class TestS3NiquestsBasicOperations:
    def test_put_get_delete_object(self, s3_bucket, loop):
        from tracktolib.s3.niquests import s3_put_object, s3_get_object, s3_delete_object

        async def _test():
            with get_botocore_client() as s3:
                async with niquests.AsyncSession() as client:
                    # Put object (acl=None to avoid MinIO ACL issues with presigned URLs)
                    test_data = b"Hello, World!"
                    resp = await s3_put_object(s3, client, s3_bucket, "test/hello.txt", test_data, acl=None)
                    assert resp.status_code == 200

                    # Get object
                    result = await s3_get_object(s3, client, s3_bucket, "test/hello.txt")
                    assert result == test_data

                    # Get non-existent object
                    result = await s3_get_object(s3, client, s3_bucket, "test/nonexistent.txt")
                    assert result is None

                    # Delete object
                    resp = await s3_delete_object(s3, client, s3_bucket, "test/hello.txt")
                    assert resp.status_code == 204

                    # Verify deleted
                    result = await s3_get_object(s3, client, s3_bucket, "test/hello.txt")
                    assert result is None

        loop.run_until_complete(_test())

    def test_list_files(self, s3_bucket, loop):
        from tracktolib.s3.niquests import s3_put_object, s3_list_files

        async def _test():
            with get_botocore_client() as s3:
                async with niquests.AsyncSession() as client:
                    # Create some test files (acl=None to avoid MinIO ACL issues)
                    await s3_put_object(s3, client, s3_bucket, "prefix/file1.txt", b"content1", acl=None)
                    await s3_put_object(s3, client, s3_bucket, "prefix/file2.txt", b"content2", acl=None)
                    await s3_put_object(s3, client, s3_bucket, "other/file3.txt", b"content3", acl=None)

                    # List with prefix
                    files = s3_list_files(s3, s3_bucket, "prefix/")
                    keys = [f["Key"] for f in files]
                    assert sorted(keys) == ["prefix/file1.txt", "prefix/file2.txt"]

        loop.run_until_complete(_test())


@pytest.mark.usefixtures("setup_bucket")
class TestS3StreamingUpload:
    @pytest.mark.skip(reason="MinIO does not support presigned URLs for multipart complete operation")
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

    @pytest.mark.skip(reason="MinIO does not support presigned URLs for multipart abort operation")
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

    def test_s3_file_upload_small_file(self, s3_bucket, loop):
        """Test streaming upload with small file (uses single PUT)."""
        from tracktolib.s3.niquests import s3_file_upload, s3_get_object

        async def _test():
            with get_botocore_client() as s3:
                async with niquests.AsyncSession() as client:
                    key = "test/small-stream.txt"
                    test_data = b"Small file content"

                    async def data_stream():
                        yield test_data

                    received_chunks = []

                    def on_chunk(chunk: bytes):
                        received_chunks.append(chunk)

                    await s3_file_upload(
                        s3,
                        client,
                        s3_bucket,
                        key,
                        data_stream(),
                        min_part_size=5 * 1024 * 1024,
                        on_chunk_received=on_chunk,
                        content_length=len(test_data),
                    )

                    # Verify callback was called
                    assert len(received_chunks) == 1
                    assert received_chunks[0] == test_data

                    # Verify uploaded content
                    result = await s3_get_object(s3, client, s3_bucket, key)
                    assert result == test_data

        loop.run_until_complete(_test())

    @pytest.mark.skip(reason="MinIO does not support presigned URLs for multipart operations")
    def test_s3_file_upload_large_file(self, s3_bucket, loop):
        """Test streaming upload with large file (uses multipart)."""
        from tracktolib.s3.niquests import s3_file_upload, s3_get_object

        async def _test():
            with get_botocore_client() as s3:
                async with niquests.AsyncSession() as client:
                    key = "test/large-stream.bin"
                    # Create 12MB of data (should trigger multipart with 5MB min)
                    chunk_size = 3 * 1024 * 1024  # 3MB chunks
                    total_chunks = 4  # 12MB total
                    test_data = b"X" * (chunk_size * total_chunks)

                    async def data_stream():
                        for i in range(total_chunks):
                            yield test_data[i * chunk_size : (i + 1) * chunk_size]

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
                    )

                    # Verify callback received all data
                    assert received_size == len(test_data)

                    # Verify uploaded content
                    result = await s3_get_object(s3, client, s3_bucket, key)
                    assert result == test_data

        loop.run_until_complete(_test())

    def test_s3_file_upload_exactly_one_part(self, s3_bucket, loop):
        """Test streaming upload with data smaller than min_part_size (falls back to PUT)."""
        from tracktolib.s3.niquests import s3_file_upload, s3_get_object

        async def _test():
            with get_botocore_client() as s3:
                async with niquests.AsyncSession() as client:
                    key = "test/single-part-fallback.bin"
                    # 4MB - less than 5MB min, should use single PUT via fallback
                    test_data = b"Y" * (4 * 1024 * 1024)

                    async def data_stream():
                        # Yield in smaller chunks
                        chunk_size = 1024 * 1024
                        for i in range(0, len(test_data), chunk_size):
                            yield test_data[i : i + chunk_size]

                    await s3_file_upload(s3, client, s3_bucket, key, data_stream(), min_part_size=5 * 1024 * 1024)

                    # Verify uploaded content
                    result = await s3_get_object(s3, client, s3_bucket, key)
                    assert result == test_data

        loop.run_until_complete(_test())
