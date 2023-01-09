from pathlib import Path

try:
    from minio.deleteobjects import DeleteObject
    from minio import Minio
except ImportError:
    raise ImportError('Please install minio or tracktolib with "s3-minio" to use this module')


def download_bucket(minio: Minio, bucket_name: str, output_dir: Path) -> list[Path]:
    files = []
    for obj in minio.list_objects(bucket_name, recursive=True):
        data = minio.get_object(bucket_name, obj.object_name)
        _file = (output_dir / obj.object_name)
        _file.parent.mkdir(exist_ok=True, parents=True)
        with (output_dir / obj.object_name).open('wb') as file_data:
            for d in data.stream(32 * 1024):
                file_data.write(d)
        files.append(_file)

    return files


def bucket_rm(minio: Minio, bucket_name: str):
    names = [DeleteObject(x.object_name)
             for x in minio.list_objects(bucket_name, recursive=True)]
    errors = minio.remove_objects(bucket_name, names)
    errors = list(errors)
    if errors:
        raise NotImplementedError(f'Got errors: {errors}')
    minio.remove_bucket(bucket_name)


def upload_object(minio: Minio, bucket_name: str, object_name: str, path: Path):
    minio.fput_object(
        bucket_name, object_name, path.absolute())
