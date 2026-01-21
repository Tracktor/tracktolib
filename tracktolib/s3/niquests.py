from __future__ import annotations

from collections import namedtuple
from pathlib import Path

import http
import xml.etree.ElementTree as ET
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator, Callable, Literal, Self, TypedDict

try:
    import botocore.client
    import botocore.session
    import jmespath
    from botocore.config import Config
except ImportError as e:
    raise ImportError("botocore is required for S3 operations. Install with tracktolib[s3-niquests]") from e

try:
    import niquests
except ImportError as e:
    raise ImportError("niquests is required for S3 operations. Install with tracktolib[s3-niquests]") from e

from ..utils import get_stream_chunk

__all__ = (
    "S3Session",
    "s3_delete_object",
    "s3_delete_objects",
    "s3_list_files",
    "s3_put_object",
    "s3_get_object",
    "s3_download_file",
    "s3_upload_file",
    "s3_create_multipart_upload",
    "s3_multipart_upload",
    "s3_file_upload",
    "S3MultipartUpload",
    "S3Object",
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

        # With custom clients:
        async with S3Session(
            endpoint_url='...',
            access_key='...',
            secret_key='...',
            s3_client=my_s3_client,
            http_client=my_http_session,
        ) as s3:
            ...
    """

    endpoint_url: str
    access_key: str
    secret_key: str
    region: str
    s3_config: Config | None = None
    _s3_client: botocore.client.BaseClient | None = None
    _http_client: niquests.AsyncSession | None = None

    def __post_init__(self):
        if self._s3_client is None:
            session = botocore.session.Session()
            self._s3_client = session.create_client(
                "s3",
                endpoint_url=self.endpoint_url,
                region_name=self.region,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                config=self.s3_config,
            )
        if self._http_client is None:
            self._http_client = niquests.AsyncSession()

    @property
    def s3_client(self) -> botocore.client.BaseClient:
        if self._s3_client is None:
            raise ValueError("s3_client is not initialized")
        return self._s3_client

    @property
    def http_client(self) -> niquests.AsyncSession:
        if self._http_client is None:
            raise ValueError("http_client is not initialized")
        return self._http_client

    async def __aenter__(self) -> Self:
        await self.http_client.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.http_client.__aexit__(exc_type, exc_val, exc_tb)
        self.s3_client.close()

    async def delete_object(self, bucket: str, key: str) -> niquests.Response:
        """Delete an object from S3."""
        return await s3_delete_object(self.s3_client, self.http_client, bucket, key)

    async def delete_objects(self, bucket: str, keys: list[str]) -> list[niquests.Response]:
        """Delete multiple objects from S3."""
        return await s3_delete_objects(self.s3_client, self.http_client, bucket, keys)

    def list_files(
        self,
        bucket: str,
        prefix: str,
        *,
        search_query: str | None = None,
        max_items: int | None = None,
        page_size: int | None = None,
        starting_token: str | None = None,
    ) -> AsyncIterator[S3Object]:
        """List files in an S3 bucket with a given prefix."""
        return s3_list_files(
            self.s3_client,
            self.http_client,
            bucket,
            prefix,
            search_query=search_query,
            max_items=max_items,
            page_size=page_size,
            starting_token=starting_token,
        )

    async def put_object(self, bucket: str, key: str, data: bytes, *, acl: ACL | None = "private") -> niquests.Response:
        """Upload an object to S3."""
        return await s3_put_object(self.s3_client, self.http_client, bucket, key, data, acl=acl)

    async def upload_file(
        self, bucket: str, file: Path, path: str, *, acl: ACL | None = "private"
    ) -> niquests.Response:
        """Upload a file to S3."""
        return await s3_upload_file(self.s3_client, self.http_client, bucket, file, path, acl=acl)

    async def get_object(self, bucket: str, key: str) -> bytes | None:
        """Download an object from S3."""
        return await s3_get_object(self.s3_client, self.http_client, bucket, key)

    async def download_file(
        self,
        bucket: str,
        key: str,
        on_chunk: Callable[[bytes], None] | None = None,
        chunk_size: int = 1024 * 1024,
    ) -> AsyncIterator[bytes]:
        """Download a file from S3 with streaming support."""
        async for chunk in s3_download_file(self.s3_client, self.http_client, bucket, key, chunk_size=chunk_size):
            if on_chunk:
                on_chunk(chunk)
            yield chunk

    def multipart_upload(self, bucket: str, key: str, *, expires_in: int = 3600):
        """Create a multipart upload context manager."""
        return s3_multipart_upload(self.s3_client, self.http_client, bucket, key, expires_in=expires_in)

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
            self.http_client,
            bucket,
            key,
            data,
            min_part_size=min_part_size,
            on_chunk_received=on_chunk_received,
            content_length=content_length,
        )


S3MultipartUpload = namedtuple(
    "S3MultipartUpload", ["fetch_create", "fetch_complete", "upload_part", "generate_presigned_url", "fetch_abort"]
)


class UploadPart(TypedDict):
    PartNumber: int
    ETag: str | None


class S3Object(TypedDict, total=False):
    Key: str
    LastModified: str
    ETag: str
    Size: int
    StorageClass: str


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
    return (await client.delete(url)).raise_for_status()


async def s3_delete_objects(
    s3: botocore.client.BaseClient, client: niquests.AsyncSession, bucket: str, keys: list[str]
) -> list[niquests.Response]:
    """Delete multiple objects from S3 using presigned URLs."""
    responses = []
    for key in keys:
        resp = await s3_delete_object(s3, client, bucket, key)
        responses.append(resp)
    return responses


async def s3_list_files(
    s3: botocore.client.BaseClient,
    client: niquests.AsyncSession,
    bucket: str,
    prefix: str,
    *,
    search_query: str | None = None,
    max_items: int | None = None,
    page_size: int | None = None,
    starting_token: str | None = None,
) -> AsyncIterator[S3Object]:
    """
    List files in an S3 bucket with a given prefix.

    Yields dicts with 'Key', 'LastModified', 'Size', etc. Use `search_query` for
    JMESPath filtering (e.g. "Contents[?Size > `100`][]"), `max_items` to limit
    total results, `page_size` to control items per request, and `starting_token`
    to resume from a previous continuation token.
    """
    api_version = s3.meta.service_model.api_version
    ns = {"s3": f"http://s3.amazonaws.com/doc/{api_version}/"}

    continuation_token = starting_token
    items_yielded = 0

    while True:
        params: dict = {"Bucket": bucket, "Prefix": prefix}
        if continuation_token:
            params["ContinuationToken"] = continuation_token
        if page_size is not None:
            params["MaxKeys"] = page_size

        url = s3.generate_presigned_url(
            ClientMethod="list_objects_v2",
            Params=params,
        )

        resp = (await client.get(url)).raise_for_status()
        if resp.content is None:
            return
        root = ET.fromstring(resp.content)

        page_items: list[S3Object] = []
        for contents in root.findall("s3:Contents", ns):
            item: S3Object = {}
            for child in contents:
                tag = child.tag.replace(f"{{{ns['s3']}}}", "")
                item[tag] = child.text
            if "Size" in item:
                item["Size"] = int(item["Size"])
            page_items.append(item)

        if search_query:
            page_items = jmespath.search(search_query, {"Contents": page_items}) or []

        for item in page_items:
            if max_items is not None and items_yielded >= max_items:
                return
            yield item
            items_yielded += 1

        is_truncated = root.find("s3:IsTruncated", ns)
        if is_truncated is not None and is_truncated.text == "true":
            next_token = root.find("s3:NextContinuationToken", ns)
            continuation_token = next_token.text if next_token is not None else None
        else:
            break


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
    resp = (await client.put(url, data=data)).raise_for_status()
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
    if resp.status_code == http.HTTPStatus.NOT_FOUND:
        return None
    resp.raise_for_status()
    return resp.content


async def s3_download_file(
    s3: botocore.client.BaseClient,
    client: niquests.AsyncSession,
    bucket: str,
    key: str,
    *,
    chunk_size: int = 1024 * 1024,
) -> AsyncIterator[bytes]:
    """Download an object from S3 with streaming support."""
    url = s3.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": bucket, "Key": key},
    )
    resp = await client.get(url, stream=True)
    resp.raise_for_status()
    async for chunk in await resp.iter_content(chunk_size):
        yield chunk


async def s3_create_multipart_upload(
    s3: botocore.client.BaseClient,
    client: niquests.AsyncSession,
    bucket: str,
    key: str,
    *,
    expires_in: int = 3600,
    generate_presigned_url: Callable[..., str] | None = None,
) -> str:
    """Initiate a multipart upload and return the UploadId."""
    if generate_presigned_url is not None:
        url = generate_presigned_url("create_multipart_upload")
    else:
        url = s3.generate_presigned_url(
            ClientMethod="create_multipart_upload",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=expires_in,
        )
    resp = (await client.post(url)).raise_for_status()
    if resp.content is None:
        raise ValueError("Empty response from create_multipart_upload")
    api_version = s3.meta.service_model.api_version
    ns = {"s3": f"http://s3.amazonaws.com/doc/{api_version}/"}
    root = ET.fromstring(resp.content)
    upload_id_elem = root.find("s3:UploadId", ns)
    if upload_id_elem is None or upload_id_elem.text is None:
        raise ValueError("UploadId not found in response")
    return upload_id_elem.text


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

        return (
            await client.post(complete_url, data=xml_payload, headers={"Content-Type": "application/xml"})
        ).raise_for_status()

    async def fetch_abort():
        nonlocal _has_been_aborted
        if upload_id is None:
            raise ValueError("Upload ID is not set")
        abort_url = _generate_presigned_url("abort_multipart_upload", UploadId=upload_id)
        abort_resp = (await client.delete(abort_url)).raise_for_status()
        _has_been_aborted = True
        return abort_resp

    async def upload_part(data: bytes) -> UploadPart:
        nonlocal _part_number, _parts
        if upload_id is None:
            raise ValueError("Upload ID is not set")
        presigned_url = _generate_presigned_url("upload_part", UploadId=upload_id, PartNumber=_part_number)
        upload_resp = (await client.put(presigned_url, data=data)).raise_for_status()
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

    async def fetch_create() -> str:
        nonlocal upload_id
        upload_id = await s3_create_multipart_upload(
            s3, client, bucket, key, expires_in=expires_in, generate_presigned_url=_generate_presigned_url
        )
        return upload_id

    try:
        yield S3MultipartUpload(
            fetch_create=fetch_create,
            fetch_complete=fetch_complete,
            upload_part=upload_part,
            fetch_abort=fetch_abort,
            generate_presigned_url=_generate_presigned_url,
        )
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
    Upload a file to S3 from an async byte stream.

    Uses multipart upload for large files. If `content_length` is provided and smaller
    than `min_part_size`, uses a single PUT instead. Use `on_chunk_received` callback
    to track upload progress.
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
        await mpart.fetch_create()
        has_uploaded_parts = False
        async for chunk in get_stream_chunk(data, min_size=min_part_size):
            if on_chunk_received:
                on_chunk_received(chunk)
            if len(chunk) < min_part_size:
                if not has_uploaded_parts:
                    # No parts uploaded yet, abort multipart and use single PUT
                    await mpart.fetch_abort()
                    await s3_put_object(s3, client, bucket=bucket, key=key, data=chunk, acl=None)
                else:
                    # Parts already uploaded, upload final chunk as last part (S3 allows last part to be smaller)
                    await mpart.upload_part(chunk)
                return
            await mpart.upload_part(chunk)
            has_uploaded_parts = True
