"""The curl shim uses a connection budget distinct from either response window."""
import asyncio
import unittest

import httpx
from curl_cffi import CurlOpt
from curl_cffi.requests.exceptions import Timeout as CurlTimeout

from impersonate import _create_upstream_client


class CurlConnectBudget(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def client(connect=.075):
        return _create_upstream_client(
            {"timeout": httpx.Timeout(connect=connect, read=.5, write=.2, pool=.1), "trust_env": False},
            ("chrome", 10),
        )

    async def read(self, client, url, stream):
        if stream:
            async with client.stream("POST", url, json={}) as response:
                return await response.aread()
        return (await client.post(url, json={})).content

    async def test_each_request_times_out_in_quiet_tls_handshake_and_releases_handle(self):
        peers = set()
        async def accept(reader, writer):
            peers.add(asyncio.current_task())
            try:
                await reader.read()
            finally:
                writer.close()
                await writer.wait_closed()
                peers.discard(asyncio.current_task())

        async with await asyncio.start_server(accept, "127.0.0.1", 0) as server:
            client = self.client()
            try:
                url = f"https://127.0.0.1:{server.sockets[0].getsockname()[1]}/fixture"
                for stream in (True, False):
                    with self.subTest(stream=stream):
                        async with asyncio.timeout(2.0):
                            with self.assertRaises(CurlTimeout):
                                await self.read(client, url, stream)
                        self.assertEqual(client._s.pool.qsize(), client._s.max_clients)
            finally:
                await client.aclose()
                await asyncio.gather(*peers)

    async def test_connection_timeout_does_not_shorten_wait_after_connect(self):
        async def respond(reader, writer):
            await reader.readuntil(b"\r\n\r\n")
            await asyncio.sleep(.15)
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK")
            await writer.drain()
            writer.close()
            await writer.wait_closed()

        async with await asyncio.start_server(respond, "127.0.0.1", 0) as server:
            url = f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}/fixture"
            client = self.client()
            try:
                self.assertEqual(client._s.curl_options[CurlOpt.CONNECTTIMEOUT_MS], 75)
                self.assertEqual((client._post_timeout, client._stream_timeout), (60, 1800))
                for stream in (True, False):
                    with self.subTest(stream=stream):
                        self.assertEqual(await self.read(client, url, stream), b"OK")
            finally:
                await client.aclose()
