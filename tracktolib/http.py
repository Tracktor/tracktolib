import typing
from typing import BinaryIO, Callable

try:
    import httpx
    from httpx._types import QueryParamTypes
except ImportError:
    raise ImportError('Please install httpx or tracktolib with "http" to use this module')

MB_1 = 1024 * 1024


async def download_file(url: str,
                        client: httpx.AsyncClient,
                        output_file: BinaryIO,
                        *,
                        chunk_size: int = MB_1 * 10,
                        on_chunk_received: Callable[[bytes], None] | None = None,
                        on_response: Callable[[httpx.Response], None] | None = None,
                        params: QueryParamTypes | None = None,
                        headers: dict[str, str] | None = None):
    """
    on_chunk_received: Useful to compute the hash for install or display progress
    """
    async with client.stream('GET', url, params=params, headers=headers) as r:
        if on_response:
            on_response(r)
        async for data in r.aiter_bytes(chunk_size=chunk_size):
            output_file.write(data)
            if on_chunk_received:
                on_chunk_received(data)


ContentLength: typing.TypeAlias = int


def get_progress(update_fn: Callable[[ContentLength, int], None]):
    content_length: ContentLength = 0

    def on_response(resp: httpx.Response):
        nonlocal content_length
        content_length = int(resp.headers["Content-length"])

    def on_chunk_received(data: bytes):
        update_fn(content_length, len(data))

    return on_response, on_chunk_received
