from io import BytesIO
from pathlib import Path
import datetime as dt
from typing import TypedDict

try:
    from aiobotocore.client import AioBaseClient
except ImportError:
    raise ImportError('Please install aiobotocore or tracktolib with "s3" to use this module')


async def upload_file(client: AioBaseClient,
                      bucket: str,
                      file: Path,
                      path: str):
    """
    Upload a file to s3
    """
    resp = await client.put_object(Bucket=bucket,
                                   Key=path,
                                   Body=file.read_bytes())
    return resp


async def download_file(client: AioBaseClient,
                        bucket: str,
                        path: str) -> BytesIO | None:
    """
    Loads a file from a s3 bucket
    """
    try:
        resp = await client.get_object(Bucket=bucket,
                                       Key=path)
    except client.exceptions.NoSuchKey:
        _file = None
    else:
        async with resp['Body'] as stream:
            _data = await stream.read()
            _file = BytesIO(_data)
    return _file


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


async def list_files(client: AioBaseClient,
                     bucket: str,
                     path: str,
                     *,
                     search_query: str | None = None,
                     max_items: int | None = None,
                     page_size: int | None = None
                     ) -> list[S3Item]:
    """
    See https://jmespath.org/ for the search query syntax.
    Example of search query: "Contents[?Size > `100`][]" or "Contents[?Size > `100` && LastModified > `2021-01-01`][]
    """
    paginator = client.get_paginator('list_objects')
    config = {}
    if max_items is not None:
        config['max_items'] = max_items
    if page_size is not None:
        config['page_size'] = page_size

    page_iterator = paginator.paginate(Bucket=bucket,
                                       Prefix=path,
                                       PaginationConfig=config if config else {}
                                       )
    filtered_iterator = page_iterator.search(search_query) if search_query else page_iterator

    files = []

    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/paginators.html#customizing-page-iterators
    async for result in filtered_iterator:  # type: ignore
        if search_query:
            files.append(result)
        else:
            for c in result.get('Contents', []):
                files.append(c)
    return files
