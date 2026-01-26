---
title: "HTTP (Deprecated)"
---

# HTTP (Deprecated)

!!! warning "Deprecation Notice"
This module is deprecated and will be removed in a future version. Consider using [httpx](https://www.python-httpx.org/)
directly or other HTTP clients.

HTTP client helpers using [httpx](https://www.python-httpx.org/).

## Installation

```bash
uv add tracktolib[http]
```

## Dependencies

- [httpx](https://www.python-httpx.org/)

## Functions

### `download_file`

Download a file with streaming and progress callbacks.

```python
import httpx
from pathlib import Path
from tracktolib.http_utils import download_file

async with httpx.AsyncClient() as client:
    with open('output.bin', 'wb') as f:
        await download_file(
            url='https://example.com/large-file.zip',
            client=client,
            output_file=f,
            chunk_size=10 * 1024 * 1024  # 10MB chunks
        )
```

## Complete Example

```python
import httpx
from pathlib import Path
from tracktolib.http_utils import download_file, get_progress


async def download_with_progress(url: str, output_path: Path):
    total_downloaded = 0

    def on_progress(total_size: int, chunk_size: int):
        nonlocal total_downloaded
        total_downloaded += chunk_size
        percent = (total_downloaded / total_size) * 100
        print(f"\rDownloading: {percent:.1f}% ({total_downloaded}/{total_size})", end='')

    on_response, on_chunk = get_progress(on_progress)

    async with httpx.AsyncClient() as client:
        with output_path.open('wb') as f:
            await download_file(
                url=url,
                client=client,
                output_file=f,
                on_response=on_response,
                on_chunk_received=on_chunk
            )

    print("\nDownload complete!")


# Usage
import asyncio

asyncio.run(download_with_progress(
    'https://example.com/large-file.zip',
    Path('downloaded.zip')
))
```
