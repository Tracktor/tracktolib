from __future__ import annotations

from collections import namedtuple
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import AsyncIterator, Callable, Literal, Self, TypedDict

from tracktolib.utils import async_get_byte_chunks

try:
    import botocore.client
    import botocore.session
    from botocore.exceptions import ClientError
except ImportError as e:
    raise ImportError("botocore is required for S3 operations. Install with tracktolib[s3-niquests]") from e

try:
    import niquests
    from niquests import HTTPError
except ImportError as e:
    raise ImportError("niquests is required for S3 operations. Install with tracktolib[s3-niquests]") from e

# Alias for backward compatibility
get_stream_chunk = async_get_byte_chunks

__all__ = (
    "S3Session",
    "s3_delete_object",
    "s3_delete_objects",
    "s3_list_files",
    "s3_put_object",
    "s3_get_object",
    "s3_upload_file",
    "s3_multipart_upload",
    "s3_file_upload",
    "get_stream_chunk",
    "S3MultipartUpload",
    "UploadPart",
)

ACL = Literal[
    "private",
    "public-read",
    "public-read-write",
    "authenticated-read",
    "aws-exec-read",
    "bucket-owner-read",
    "bucket-owner-full-control",
]


@dataclass
class S3Session:
    """
    Utility class that wraps botocore S3 client and niquests async session.

    Usage:
        async with S3Session(
            endpoint_url='http://localhost:9000',
            access_key='foo',
            secret_key='bar',
        ) as s3:
            await s3.put_object('my-bucket', 'path/to/file.txt', b'content')
            content = await s3.get_object('my-bucket', 'path/to/file.txt')
    """

    endpoint_url: str
    access_key: str
    secret_key: str
    region: str = "us-east-1"

    _s3_client: botocore.client.BaseClient | None = None
    _http: niquests.AsyncSession | None = None

    @property
    def s3_client(self) -> botocore.client.BaseClient:
        if self._s3_client is None:
            raise RuntimeError("S3Session not initialized. Use async with S3Session(...) as s3:")
        return self._s3_client

    @property
    def http(self) -> niquests.AsyncSession:
        if self._http is None:
            raise RuntimeError("S3Session not initialized. Use async with S3Session(...) as s3:")
        return self._http

    async def __aenter__(self) -> Self:
        session = botocore.session.Session()
        self._s3_client = session.create_client(
            "s3",
            endpoint_url=self.endpoint_url,
            region_name=self.region,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )
        self._http = niquests.AsyncSession()
        await self._http.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._http is not None:
            await self._http.__aexit__(exc_type, exc_val, exc_tb)
            self._http = None
        if self._s3_client is not None:
            self._s3_client.close()
            self._s3_client = None

    async def delete_object(self, bucket: str, key: str) -> niquests.Response:
        """Delete an object from S3."""
        return await s3_delete_object(self.s3_client, self.http, bucket, key)

    async def delete_objects(self, bucket: str, keys: list[str]) -> list[niquests.Response]:
        """Delete multiple objects from S3."""
        return await s3_delete_objects(self.s3_client, self.http, bucket, keys)

    def list_files(self, bucket: str, prefix: str) -> list[dict]:
        """List files in an S3 bucket with a given prefix."""
        return s3_list_files(self.s3_client, bucket, prefix)

    async def put_object(self, bucket: str, key: str, data: bytes, *, acl: ACL | None = "private") -> niquests.Response:
        """Upload an object to S3."""
        return await s3_put_object(self.s3_client, self.http, bucket, key, data, acl=acl)

    async def upload_file(
        self, bucket: str, file: Path, path: str, *, acl: ACL | None = "private"
    ) -> niquests.Response:
        """Upload a file to S3."""
        return await s3_upload_file(self.s3_client, self.http, bucket, file, path, acl=acl)

    async def get_object(self, bucket: str, key: str) -> bytes | None:
        """Download an object from S3."""
        return await s3_get_object(self.s3_client, self.http, bucket, key)

    def multipart_upload(self, bucket: str, key: str, *, expires_in: int = 3600):
        """Create a multipart upload context manager."""
        return s3_multipart_upload(self.s3_client, self.http, bucket, key, expires_in=expires_in)

    async def file_upload(
        self,
        bucket: str,
        key: str,
        data: AsyncIterator[bytes],
        min_part_size: int = 5 * 1024 * 1024,
        on_chunk_received: Callable[[bytes], None] | None = None,
        content_length: int | None = None,
    ) -> None:
        """Upload a file to S3 using streaming (multipart for large files)."""
        return await s3_file_upload(
            self.s3_client,
            self.http,
            bucket,
            key,
            data,
            min_part_size=min_part_size,
            on_chunk_received=on_chunk_received,
            content_length=content_length,
        )


S3MultipartUpload = namedtuple(
    "S3MultipartUpload", ["fetch_complete", "upload_part", "generate_presigned_url", "fetch_abort"]
)


class UploadPart(TypedDict):
    PartNumber: int
    ETag: str | None


async def s3_delete_object(
    s3: botocore.client.BaseClient, client: niquests.AsyncSession, bucket: str, key: str
) -> niquests.Response:
    """Delete an object from S3 using presigned URL."""
    url = s3.generate_presigned_url(
        ClientMethod="delete_object",
        Params={
            "Bucket": bucket,
            "Key": key,
        },
    )
    resp = await client.delete(url)
    resp.raise_for_status()
    return resp


async def s3_delete_objects(
    s3: botocore.client.BaseClient, client: niquests.AsyncSession, bucket: str, keys: list[str]
) -> list[niquests.Response]:
    """Delete multiple objects from S3 using presigned URLs."""
    responses = []
    for key in keys:
        resp = await s3_delete_object(s3, client, bucket, key)
        responses.append(resp)
    return responses


def s3_list_files(
    s3: botocore.client.BaseClient,
    bucket: str,
    prefix: str,
) -> list[dict]:
    """
    List files in an S3 bucket with a given prefix.
    Uses sync botocore client directly (list operations don't use presigned URLs).

    Returns a list of dicts with 'Key', 'LastModified', 'Size', etc.
    """
    paginator = s3.get_paginator("list_objects")
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

    files = []
    for result in page_iterator:
        for item in result.get("Contents", []):
            files.append(item)
    return files


async def s3_put_object(
    s3: botocore.client.BaseClient,
    client: niquests.AsyncSession,
    bucket: str,
    key: str,
    data: bytes,
    *,
    acl: ACL | None = "private",
) -> niquests.Response:
    """Upload an object to S3 using presigned URL."""
    params: dict = {
        "Bucket": bucket,
        "Key": key,
    }
    if acl is not None:
        params["ACL"] = acl

    url = s3.generate_presigned_url(
        ClientMethod="put_object",
        Params=params,
    )
    resp = await client.put(url, data=data)
    try:
        resp.raise_for_status()
    except HTTPError:
        raise
    return resp


async def s3_upload_file(
    s3: botocore.client.BaseClient,
    client: niquests.AsyncSession,
    bucket: str,
    file: Path,
    path: str,
    *,
    acl: ACL | None = "private",
) -> niquests.Response:
    """
    Upload a file to S3 using presigned URL.
    This is a convenience wrapper around s3_put_object that reads the file content.
    """
    return await s3_put_object(s3, client, bucket, path, file.read_bytes(), acl=acl)


async def s3_get_object(
    s3: botocore.client.BaseClient, client: niquests.AsyncSession, bucket: str, key: str
) -> bytes | None:
    """Download an object from S3 using presigned URL."""
    url = s3.generate_presigned_url(
        ClientMethod="get_object",
        Params={
            "Bucket": bucket,
            "Key": key,
        },
    )
    resp = await client.get(url)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.content


@asynccontextmanager
async def s3_multipart_upload(
    s3: botocore.client.BaseClient,
    client: niquests.AsyncSession,
    bucket: str,
    key: str,
    *,
    expires_in: int = 3600,
) -> AsyncIterator[S3MultipartUpload]:
    """Async context manager for S3 multipart upload with automatic cleanup."""
    upload_id: str | None = None
    _part_number: int = 1
    _parts: list[UploadPart] = []
    _has_been_aborted = False

    async def fetch_complete():
        if upload_id is None:
            raise ValueError("Upload ID is not set")
        complete_url = _generate_presigned_url("complete_multipart_upload", UploadId=upload_id)
        # Create XML payload for completing multipart upload
        parts_xml = "".join(
            f"<Part><PartNumber>{part['PartNumber']}</PartNumber><ETag>{part['ETag']}</ETag></Part>" for part in _parts
        )
        xml_payload = f"<CompleteMultipartUpload>{parts_xml}</CompleteMultipartUpload>"

        complete_resp = await client.post(complete_url, data=xml_payload, headers={"Content-Type": "application/xml"})
        complete_resp.raise_for_status()
        return complete_resp

    async def fetch_abort():
        nonlocal _has_been_aborted
        if upload_id is None:
            raise ValueError("Upload ID is not set")
        abort_url = _generate_presigned_url("abort_multipart_upload", UploadId=upload_id)
        abort_resp = await client.delete(abort_url)
        abort_resp.raise_for_status()
        _has_been_aborted = True
        return abort_resp

    async def upload_part(data: bytes) -> UploadPart:
        nonlocal _part_number, _parts
        if upload_id is None:
            raise ValueError("Upload ID is not set")
        presigned_url = _generate_presigned_url("upload_part", UploadId=upload_id, PartNumber=_part_number)
        upload_resp = await client.put(presigned_url, data=data)
        upload_resp.raise_for_status()

        _etag = upload_resp.headers.get("ETag")
        etag: str | None = _etag.decode() if isinstance(_etag, bytes) else _etag
        _part: UploadPart = {"PartNumber": _part_number, "ETag": etag}
        _parts.append(_part)
        _part_number += 1
        return _part

    def _generate_presigned_url(method: str, **params):
        return s3.generate_presigned_url(
            ClientMethod=method, Params={"Bucket": bucket, "Key": key, **params}, ExpiresIn=expires_in
        )

    try:
        response = s3.create_multipart_upload(Bucket=bucket, Key=key)
        upload_id = response["UploadId"]
        yield S3MultipartUpload(
            fetch_complete=fetch_complete,
            upload_part=upload_part,
            fetch_abort=fetch_abort,
            generate_presigned_url=_generate_presigned_url,
        )
    except ClientError as e:
        raise Exception(f"Failed to initiate multipart upload: {e}") from e
    except Exception as e:
        if not _has_been_aborted and upload_id is not None:
            await fetch_abort()
        raise e
    else:
        if not _has_been_aborted and upload_id is not None:
            await fetch_complete()


async def s3_file_upload(
    s3: botocore.client.BaseClient,
    client: niquests.AsyncSession,
    bucket: str,
    key: str,
    data: AsyncIterator[bytes],
    # 5MB minimum for S3 parts
    min_part_size: int = 5 * 1024 * 1024,
    on_chunk_received: Callable[[bytes], None] | None = None,
    content_length: int | None = None,
) -> None:
    """
    Upload a file to S3 using multipart upload from an async byte stream.

    Args:
        s3: Botocore S3 client for generating presigned URLs
        client: Niquests async session for HTTP requests
        bucket: S3 bucket name
        key: S3 object key
        data: Async iterator yielding bytes chunks
        min_part_size: Minimum size for each part (default 5MB, S3 minimum)
        on_chunk_received: Optional callback called for each chunk received
        content_length: Optional content length hint. If provided and less than
            min_part_size, uses single PUT instead of multipart upload.
    """
    if content_length is not None and content_length < min_part_size:
        # Small file - use single PUT operation
        _data = b""
        async for chunk in data:
            _data += chunk
            if on_chunk_received:
                on_chunk_received(chunk)
        await s3_put_object(s3, client, bucket=bucket, key=key, data=_data, acl=None)
        return

    async with s3_multipart_upload(s3, client, bucket=bucket, key=key) as mpart:
        async for chunk in get_stream_chunk(data, min_size=min_part_size):
            if on_chunk_received:
                on_chunk_received(chunk)
            if len(chunk) < min_part_size:
                # Final chunk is smaller than min_part_size, abort multipart and use single PUT
                await mpart.fetch_abort()
                await s3_put_object(s3, client, bucket=bucket, key=key, data=chunk, acl=None)
                break
            await mpart.upload_part(chunk)
