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

All upload methods accept `S3ObjectParams` as keyword arguments. See [S3 Object Parameters](#s3-object-parameters) for the full list.

#### `put_object`

Upload bytes to S3.

```python
# Basic upload
await s3.put_object('my-bucket', 'file.txt', b'content')

# With parameters
await s3.put_object(
    'my-bucket', 'data.json', b'{"key": "value"}',
    acl='private',
    content_type='application/json',
    cache_control='max-age=3600',
    metadata={'author': 'me', 'version': '1.0'},
)
```

#### `get_object`

Download an object. Returns `None` if not found.

```python
content = await s3.get_object('my-bucket', 'file.txt')
if content is None:
    print('File not found')
```

#### `upload_file`

Upload a file from disk.

```python
from pathlib import Path

await s3.upload_file('my-bucket', Path('local.txt'), 'remote/path.txt')

# With content type
await s3.upload_file(
    'my-bucket', Path('image.png'), 'images/photo.png',
    content_type='image/png',
    cache_control='max-age=86400',
)
```

#### `delete_object`

Delete a single object.

```python
await s3.delete_object('my-bucket', 'file.txt')
```

#### `delete_objects`

Delete multiple objects.

```python
await s3.delete_objects('my-bucket', ['file1.txt', 'file2.txt'])
```

#### `list_files`

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

#### `file_upload`

Stream upload from an async iterator. Automatically uses multipart upload for large files.

```python
async def read_chunks():
    with open('large_file.bin', 'rb') as f:
        while chunk := f.read(1024 * 1024):
            yield chunk

await s3.file_upload('my-bucket', 'large_file.bin', read_chunks())

# With parameters
await s3.file_upload(
    'my-bucket', 'video.mp4', read_chunks(),
    content_type='video/mp4',
    storage_class='STANDARD_IA',
    metadata={'duration': '120'},
)
```

#### `multipart_upload`

Low-level multipart upload context manager.

```python
async with s3.multipart_upload('my-bucket', 'large_file.bin', acl='private') as upload:
    await upload.fetch_create()
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
| `build_s3_headers` | Build HTTP headers from `S3ObjectParams` |
| `build_s3_presigned_params` | Build presigned URL params from `S3ObjectParams` |

### Types

| Type | Description |
|------|-------------|
| `S3ObjectParams` | TypedDict for S3 object parameters |
| `S3Object` | TypedDict for S3 object metadata |
| `UploadPart` | TypedDict for multipart upload part info |

## S3 Object Parameters

All upload methods (`put_object`, `upload_file`, `file_upload`, `multipart_upload`) accept the following keyword arguments via `S3ObjectParams`:

| Parameter | Type | Description |
|-----------|------|-------------|
| `acl` | `str \| None` | Canned ACL (optional, no header if omitted) |
| `content_type` | `str \| None` | MIME type (e.g., `'application/json'`) |
| `content_disposition` | `str \| None` | Content-Disposition header |
| `content_encoding` | `str \| None` | Content encoding (e.g., `'gzip'`) |
| `content_language` | `str \| None` | Content language |
| `cache_control` | `str \| None` | Cache-Control header (e.g., `'max-age=3600'`) |
| `storage_class` | `str \| None` | Storage class (see below) |
| `server_side_encryption` | `str \| None` | SSE algorithm (`'AES256'`, `'aws:kms'`) |
| `sse_kms_key_id` | `str \| None` | KMS key ID for SSE-KMS |
| `tagging` | `str \| None` | URL-encoded tags (`'key1=value1&key2=value2'`) |
| `metadata` | `dict[str, str] \| None` | User-defined metadata |

### ACL Values

- `'private'` (default)
- `'public-read'`
- `'public-read-write'`
- `'authenticated-read'`
- `'aws-exec-read'`
- `'bucket-owner-read'`
- `'bucket-owner-full-control'`

Set `acl=None` to not include any ACL header.

### Storage Classes

- `'STANDARD'` (default)
- `'STANDARD_IA'`
- `'ONEZONE_IA'`
- `'INTELLIGENT_TIERING'`
- `'GLACIER'`
- `'DEEP_ARCHIVE'`
- `'GLACIER_IR'`
- `'EXPRESS_ONEZONE'`

### Example

```python
await s3.put_object(
    'my-bucket', 'reports/data.json', json_bytes,
    content_type='application/json',
    cache_control='max-age=86400',
    storage_class='STANDARD_IA',
    metadata={'generated_by': 'report-service', 'version': '2.0'},
    tagging='env=production&team=analytics',
)
```

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
    await upload.fetch_create()

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