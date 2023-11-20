import asyncio
import os
from http.server import HTTPServer, SimpleHTTPRequestHandler
from threading import Thread

import httpx
import pytest


class QuietHTTPRequestHandler(SimpleHTTPRequestHandler):
    def log_message(self, format, *args):
        # Overridden to prevent logging
        pass


@pytest.fixture
def http_server(static_dir):
    # Change the current working directory to the 'foo' directory
    os.chdir(static_dir)

    # Create a simple HTTP server
    server = HTTPServer(("localhost", 0), QuietHTTPRequestHandler)

    # Start the server in a separate thread
    thread = Thread(target=server.serve_forever)
    thread.daemon = True
    thread.start()

    yield server

    # Stop the server
    server.shutdown()
    server.server_close()


@pytest.mark.parametrize("file, mode", [("test.csv", "w"), ("test-bytes.bytes", "wb")])
def test_download_file(http_server, file, tmp_path, mode):
    from tracktolib.download import download_file

    base_url = "http://{}:{}/".format(*http_server.server_address)
    file_uri = base_url + file

    async def _test():
        async with httpx.AsyncClient() as client:
            with (tmp_path / file).open(mode) as f:
                await download_file(url=file_uri, client=client, output_file=f)

    asyncio.run(_test())
