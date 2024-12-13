import datetime as dt
from io import BytesIO
from pathlib import Path
from typing import TypedDict, Literal, Callable

try:
    from aiobotocore.client import AioBaseClient
except ImportError:
    raise ImportError('Please install aiobotocore or tracktolib with "s3" to use this module')

ACL = Literal[
    "private",
    "public-read",
    "public-read-write",
    "authenticated-read",
    "aws-exec-read",
    "bucket-owner-read",
    "bucket-owner-full-control",
]


async def upload_file(
    client: AioBaseClient, bucket: str, file: Path, path: str, *, acl: ACL | None = "private"
) -> dict[str, str]:
    """
    Upload a file to s3.
    See:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html?highlight=put_object
    #S3.Bucket.put_object
    for more options
    """
    extra_args = {}
    if acl is not None:
        extra_args["ACL"] = acl
    resp = await client.put_object(Bucket=bucket, Key=path, Body=file.read_bytes(), **extra_args)  # type: ignore
    return resp


type ContentLength = int
type ChunkSize = int
type OnUpdateDownload = Callable[[ChunkSize], None]
type OnStartDownload = Callable[[ContentLength], None]


async def download_file(
    client: AioBaseClient,
    bucket: str,
    path: str,
    *,
    chunk_size: int = -1,
    on_start: OnStartDownload | None = None,
    on_update: OnUpdateDownload | None = None,
) -> BytesIO | None:
    """
    Loads a file from a s3 bucket.
    If chunk_size is -1, the file will be loaded in one go
    otherwise, the file will be loaded in chunks of size `chunk_size`.
    When downloading in chunked, you can specify an `on_start` and `on_update`
    callback to get the total size of the file and the size of each chunk downloaded respectively.
    """
    try:
        resp = await client.get_object(Bucket=bucket, Key=path)  # type: ignore
    except client.exceptions.NoSuchKey:
        return None

    if on_start is not None:
        on_start(resp["ContentLength"])

    async with resp["Body"] as stream:
        if chunk_size == -1:
            _data = await stream.read()
            _file = BytesIO(_data)
        else:
            chunks = []
            while chunk := await stream.content.read(chunk_size):
                chunks.append(chunk)
                if on_update is not None:
                    on_update(len(chunk))
            _file = BytesIO(b"".join(chunks)) if chunks else None

    return _file


async def delete_file(client: AioBaseClient, bucket: str, path: str) -> dict:
    """
    Delete a file from an S3 bucket.

    Args:
        client (AioBaseClient): The client to interact with the S3 service.
        bucket (str): The name of the S3 bucket.
        path (str): The path to the file within the S3 bucket.

    Return:
        dict: The response from the S3 service after attempting to delete the file.
              This typically includes metadata about the operation, such as HTTP status code,
              any errors encountered, and information about the deleted object.
    """
    return await client.delete_object(Bucket=bucket, Key=path)  # type:ignore


async def delete_files(client: AioBaseClient, bucket: str, paths: list[str], quiet: bool = True) -> dict:
    """
    Delete multiple files from an S3 bucket.

    Args:
        client (AioBaseClient): The client to interact with the S3 service.
        bucket (str): The name of the S3 bucket.
        paths (str): The paths to the files to delete within the S3 bucket.
        quiet (bool): Whether to suppress printing messages to stdout (default: True).

    Return:
        dict: The response from the S3 service after attempting to delete the files.
              This typically includes metadata about the operation, such as HTTP status code,
              any errors encountered, and information about the deleted object.
    """
    delete_request = {"Objects": [{"Key": path} for path in paths], "Quiet": quiet}
    return await client.delete_objects(Bucket=bucket, Delete=delete_request)  # type:ignore


class S3Item(TypedDict):
    """
    Represents an item in a s3 bucket
    ```
    {
        'Key': 'production/articles/',
        'LastModified': datetime.datetime(2017, 11, 6, 16, 16, 58, tzinfo=tzutc()),
        'ETag': '"d41d8cd98f00b204e9800998ecf8427e"',
        'Size': 0,
        'StorageClass': 'STANDARD',
        'Owner': {
            'DisplayName': 'tech',
            'ID': '1808ad4d71cfad18f4b5b2f162f5649f8d7233b0f040596704819e937082655f'
        }
    }
    """

    Key: str
    LastModified: dt.datetime
    ETag: str
    Size: int
    StorageClass: str
    Owner: dict[str, str]


async def list_files(
    client: AioBaseClient,
    bucket: str,
    path: str,
    *,
    search_query: str | None = None,
    max_items: int | None = None,
    page_size: int | None = None,
) -> list[S3Item]:
    """
    See https://jmespath.org/ for the search query syntax.
    Example of search query: "Contents[?Size > `100`][]" or "Contents[?Size > `100` && LastModified > `2021-01-01`][]
    """
    paginator = client.get_paginator("list_objects")
    config = {}
    if max_items is not None:
        config["max_items"] = max_items
    if page_size is not None:
        config["page_size"] = page_size

    page_iterator = paginator.paginate(Bucket=bucket, Prefix=path, PaginationConfig=config if config else {})
    filtered_iterator = page_iterator.search(search_query) if search_query else page_iterator

    files = []

    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/paginators.html#customizing-page-iterators
    async for result in filtered_iterator:  # type: ignore
        if search_query:
            files.append(result)
        else:
            for c in result.get("Contents", []):
                files.append(c)
    return files
