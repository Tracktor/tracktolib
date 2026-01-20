---
title: 'S3 (Async)'
---

# S3 (Async)

Async S3 helpers using [niquests](https://github.com/jawah/niquests) and [botocore](https://github.com/boto/botocore).

## Installation

```bash
uv add tracktolib[s3-niquests]
```

## Dependencies

- [niquests](https://github.com/jawah/niquests) - HTTP/3 capable requests replacement
- [botocore](https://github.com/boto/botocore) - AWS SDK core (for presigned URLs generation)

## Overview

This module provides async S3 functionality using `niquests` as the HTTP backend. All S3 operations use presigned URLs, making it compatible with any S3-compatible storage (AWS S3, MinIO, etc.).

Key features:

- Async context manager for session management
- Presigned URL-based operations
- Multipart upload support for large files
- Streaming upload from async iterators

## S3Session

The recommended way to interact with S3 is through the `S3Session` class, which manages both the botocore client and niquests async session.

```python
from tracktolib.s3.niquests import S3Session

async with S3Session(
    endpoint_url='http://localhost:9000',
    access_key='minioadmin',
    secret_key='minioadmin',
    region='us-east-1',
) as s3:
    # Upload an object
    await s3.put_object('my-bucket', 'path/to/file.txt', b'Hello, World!')

    # Download an object
    content = await s3.get_object('my-bucket', 'path/to/file.txt')

    # Delete an object
    await s3.delete_object('my-bucket', 'path/to/file.txt')
```

### Methods

#### `put_object(bucket, key, data, *, acl='private')`

Upload bytes to S3.

```python
await s3.put_object('my-bucket', 'file.txt', b'content', acl='public-read')
```

#### `get_object(bucket, key)`

Download an object. Returns `None` if not found.

```python
content = await s3.get_object('my-bucket', 'file.txt')
if content is None:
    print('File not found')
```

#### `upload_file(bucket, file, path, *, acl='private')`

Upload a file from disk.

```python
from pathlib import Path

await s3.upload_file('my-bucket', Path('local.txt'), 'remote/path.txt')
```

#### `delete_object(bucket, key)`

Delete a single object.

```python
await s3.delete_object('my-bucket', 'file.txt')
```

#### `delete_objects(bucket, keys)`

Delete multiple objects.

```python
await s3.delete_objects('my-bucket', ['file1.txt', 'file2.txt'])
```

#### `list_files(bucket, prefix, *, search_query=None, max_items=None, page_size=None, starting_token=None)`

List files with a given prefix. Returns an async iterator.

```python
async for f in s3.list_files('my-bucket', 'uploads/'):
    print(f['Key'], f['Size'])

# With pagination
async for f in s3.list_files('my-bucket', 'uploads/', max_items=100, page_size=50):
    print(f['Key'])

# With JMESPath filter (files larger than 100 bytes)
async for f in s3.list_files('my-bucket', 'uploads/', search_query="Contents[?Size > `100`][]"):
    print(f['Key'], f['Size'])
```

#### `file_upload(bucket, key, data, ...)`

Stream upload from an async iterator. Automatically uses multipart upload for large files.

```python
async def read_chunks():
    with open('large_file.bin', 'rb') as f:
        while chunk := f.read(1024 * 1024):
            yield chunk

await s3.file_upload('my-bucket', 'large_file.bin', read_chunks())
```

#### `multipart_upload(bucket, key, *, expires_in=3600)`

Low-level multipart upload context manager.

```python
async with s3.multipart_upload('my-bucket', 'large_file.bin') as upload:
    await upload.upload_part(chunk1)
    await upload.upload_part(chunk2)
    # Automatically completes on exit, or aborts on exception
```

## Standalone Functions

For more control, you can use the standalone functions directly with your own botocore client and niquests session.

```python
import botocore.session
import niquests
from tracktolib.s3.niquests import s3_put_object, s3_get_object

session = botocore.session.Session()
s3_client = session.create_client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
)

async with niquests.AsyncSession() as http:
    await s3_put_object(s3_client, http, 'bucket', 'key', b'data')
    content = await s3_get_object(s3_client, http, 'bucket', 'key')
```

### Available Functions

| Function | Description |
|----------|-------------|
| `s3_put_object` | Upload bytes to S3 |
| `s3_get_object` | Download an object (returns `None` if not found) |
| `s3_upload_file` | Upload a file from disk |
| `s3_delete_object` | Delete a single object |
| `s3_delete_objects` | Delete multiple objects |
| `s3_list_files` | List files with prefix (async iterator) |
| `s3_multipart_upload` | Multipart upload context manager |
| `s3_file_upload` | Stream upload from async iterator |

## ACL Options

The `acl` parameter accepts the following values:

- `'private'` (default)
- `'public-read'`
- `'public-read-write'`
- `'authenticated-read'`
- `'aws-exec-read'`
- `'bucket-owner-read'`
- `'bucket-owner-full-control'`

Set `acl=None` to not include any ACL header.

## Multipart Upload

For large files, use multipart upload to stream data efficiently.

### Using `file_upload`

The simplest way to upload large files from an async stream:

```python
async def stream_from_request(request):
    async for chunk in request.stream():
        yield chunk

await s3.file_upload(
    'my-bucket',
    'uploaded_file.bin',
    stream_from_request(request),
    min_part_size=5 * 1024 * 1024,  # 5MB minimum for S3
    on_chunk_received=lambda chunk: print(f'Received {len(chunk)} bytes'),
    content_length=request.headers.get('content-length'),  # optional hint
)
```

### Using `multipart_upload` directly

For more control over the upload process:

```python
async with s3.multipart_upload('my-bucket', 'file.bin', expires_in=3600) as upload:
    # upload_part returns an UploadPart dict with PartNumber and ETag
    part1 = await upload.upload_part(chunk1)
    part2 = await upload.upload_part(chunk2)

    # Generate presigned URL for external upload
    url = upload.generate_presigned_url('upload_part', PartNumber=3)

    # Abort if needed (otherwise completes automatically)
    # await upload.fetch_abort()
```

The context manager automatically:

- Completes the upload on successful exit
- Aborts the upload on exception