import typing
from io import TextIOWrapper, BufferedWriter
from typing import BinaryIO, Callable, TextIO

try:
    import httpx
    from httpx._types import QueryParamTypes
except ImportError:
    raise ImportError('Please install httpx or tracktolib with "http" to use this module')

MB_1 = 1024 * 1024


@typing.overload
async def download_file(
    url: str,
    client: httpx.AsyncClient,
    output_file: TextIO | TextIOWrapper,
    *,
    chunk_size: int = MB_1 * 10,
    on_chunk_received: Callable[[str], None] | None = None,
    on_response: Callable[[httpx.Response], None] | None = None,
    params: QueryParamTypes | None = None,
    headers: dict[str, str] | None = None,
):
    ...


@typing.overload
async def download_file(
    url: str,
    client: httpx.AsyncClient,
    output_file: BinaryIO | BufferedWriter,
    *,
    chunk_size: int = MB_1 * 10,
    on_chunk_received: Callable[[bytes], None] | None = None,
    on_response: Callable[[httpx.Response], None] | None = None,
    params: QueryParamTypes | None = None,
    headers: dict[str, str] | None = None,
):
    ...


async def download_file(
    url: str,
    client: httpx.AsyncClient,
    output_file: BinaryIO | TextIO | BufferedWriter | TextIOWrapper,
    *,
    chunk_size: int = MB_1 * 10,
    on_chunk_received: Callable[[typing.Any], None] | None = None,
    on_response: Callable[[httpx.Response], None] | None = None,
    params: QueryParamTypes | None = None,
    headers: dict[str, str] | None = None,
):
    """
    on_chunk_received: Useful to compute the hash for install or display progress
    """

    async with client.stream("GET", url, params=params, headers=headers) as r:
        if on_response:
            on_response(r)

        match output_file:
            case TextIO() | TextIOWrapper():
                _iter_fn = r.aiter_text
            case BinaryIO() | BufferedWriter():
                _iter_fn = r.aiter_bytes
            case _:
                raise NotImplementedError("output_file must be a TextIO or a BinaryIO")

        async for data in _iter_fn(chunk_size=chunk_size):
            output_file.write(data)  # type: ignore
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
