---
title: "S3 (MinIO)"
---

# S3 (MinIO)

S3 helpers using the [MinIO Python client](https://min.io/docs/minio/linux/developers/python/API.html).

## Installation

```bash
uv add tracktolib[s3-minio]
```

## Dependencies

- [minio](https://github.com/minio/minio-py)
- [pycryptodome](https://github.com/Legrandin/pycryptodome)

## Quick Start

```python
from minio import Minio
from pathlib import Path
from tracktolib.s3.minio import download_bucket, upload_object, bucket_rm

# Create client
minio = Minio(
    'localhost:9000',
    access_key='foo',
    secret_key='foobarbaz',
    secure=False
)

# Upload a file
upload_object(minio, 'my-bucket', 'remote/path.txt', Path('local.txt'))

# Download entire bucket
files = download_bucket(minio, 'my-bucket', Path('./downloads'))

# Remove bucket and all contents
bucket_rm(minio, 'my-bucket')
```

## Functions

### `upload_object`

Upload a file to a MinIO bucket.

```python
from pathlib import Path
from tracktolib.s3.minio import upload_object

upload_object(
    minio,
    bucket_name='my-bucket',
    object_name='uploads/document.pdf',
    path=Path('/local/path/document.pdf')
)
```

### `download_bucket`

Download all objects from a bucket to a local directory.

```python
from pathlib import Path
from tracktolib.s3.minio import download_bucket

output_dir = Path('./downloaded')
files = download_bucket(minio, 'my-bucket', output_dir)

# Returns list of downloaded file paths
for file_path in files:
    print(f"Downloaded: {file_path}")
```

The function:

- Recursively lists all objects in the bucket
- Creates necessary subdirectories
- Downloads files in 32KB chunks
- Returns a list of all downloaded `Path` objects

### `bucket_rm`

Remove a bucket and all its contents.

```python
from tracktolib.s3.minio import bucket_rm

# This will:
# 1. List all objects in the bucket
# 2. Delete all objects
# 3. Remove the bucket itself
bucket_rm(minio, 'my-bucket')
```

!!! warning
    This operation is destructive and cannot be undone. All objects in the bucket will be permanently deleted.

## Example: Backup and Restore

```python
from minio import Minio
from pathlib import Path
from tracktolib.s3.minio import download_bucket, upload_object

minio = Minio('localhost:9000', access_key='key', secret_key='secret', secure=False)

# Backup: Download entire bucket
backup_dir = Path('./backup')
files = download_bucket(minio, 'production-data', backup_dir)
print(f"Backed up {len(files)} files")

# Restore: Upload files to new bucket
minio.make_bucket('restored-data')
for local_file in backup_dir.rglob('*'):
    if local_file.is_file():
        object_name = str(local_file.relative_to(backup_dir))
        upload_object(minio, 'restored-data', object_name, local_file)
```

## Connection Examples

### Local MinIO

```python
minio = Minio(
    'localhost:9000',
    access_key='minioadmin',
    secret_key='minioadmin',
    secure=False  # HTTP
)
```

### AWS S3

```python
minio = Minio(
    's3.amazonaws.com',
    access_key='AWS_ACCESS_KEY',
    secret_key='AWS_SECRET_KEY',
    secure=True  # HTTPS
)
```

### With Region

```python
minio = Minio(
    's3.eu-west-1.amazonaws.com',
    access_key='AWS_ACCESS_KEY',
    secret_key='AWS_SECRET_KEY',
    region='eu-west-1'
)
```
