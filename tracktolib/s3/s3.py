from io import BytesIO
from pathlib import Path

try:
    from aiobotocore.client import AioBaseClient
except ImportError:
    raise ImportError('Please install tracktolib with "s3" to use this module')


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


async def list_files(client: AioBaseClient,
                     bucket: str,
                     path: str):
    paginator = client.get_paginator('list_objects')
    files = []

    async for result in paginator.paginate(Bucket=bucket, Prefix=path):  # type: ignore
        for c in result.get('Contents', []):
            files.append(c)
    return files
