"""Tests for rejected.codecs"""

import asyncio
import gzip
import unittest
from unittest import mock

from rejected import codecs, models


class MissingOptionalDependencyDecodeTests(unittest.IsolatedAsyncioTestCase):
    """Decode must raise DecodeError when the required optional
    dependency for the content type is not installed, rather than
    silently returning the raw bytes."""

    async def test_msgpack_missing_raises_decode_error(self):
        codec = codecs.Codec()
        with mock.patch.object(codecs, 'umsgpack', None):
            with self.assertRaises(codecs.DecodeError) as ctx:
                await codec.decode(b'\x90', 'application/msgpack', None)
        self.assertIn('umsgpack is required', str(ctx.exception))

    async def test_bs4_missing_raises_decode_error(self):
        codec = codecs.Codec()
        with mock.patch.object(codecs, 'bs4', None):
            with self.assertRaises(codecs.DecodeError) as ctx:
                await codec.decode(b'<html></html>', 'text/html', None)
        self.assertIn('bs4 is required', str(ctx.exception))

    async def test_bs4_missing_raises_for_xml(self):
        codec = codecs.Codec()
        with mock.patch.object(codecs, 'bs4', None):
            with self.assertRaises(codecs.DecodeError):
                await codec.decode(b'<root/>', 'text/xml', None)


class CompressionEncodeTests(unittest.IsolatedAsyncioTestCase):
    """Encode must not silently emit an uncompressed body when the
    content_encoding claims an unsupported compression scheme."""

    async def test_unknown_content_encoding_raises(self):
        codec = codecs.Codec()
        with self.assertRaises(codecs.EncodeError) as ctx:
            await codec.encode('hello', 'text/plain', 'deflate')
        self.assertIn('Unsupported content_encoding', str(ctx.exception))

    async def test_gzip_round_trip(self):
        codec = codecs.Codec()
        encoded = await codec.encode('hello', 'text/plain', 'gzip')
        self.assertEqual(gzip.decompress(encoded), b'hello')
        decoded = await codec.decode(encoded, 'text/plain', 'gzip')
        self.assertEqual(decoded, b'hello')

    async def test_bzip2_supported(self):
        codec = codecs.Codec()
        encoded = await codec.encode('hello', 'text/plain', 'bzip2')
        self.assertNotEqual(encoded, b'hello')


def _http_codec():
    return codecs.Codec(
        models.SchemaRegistryConfig(
            type='http', uri='http://registry/{0}.avsc'
        )
    )


def _fake_httpx():
    """A stand-in httpx module whose RequestError is a real
    exception class so ``except httpx.RequestError`` works."""
    fake = mock.Mock()
    fake.RequestError = type('RequestError', (Exception,), {})
    return fake


class AvroHttpSchemaTests(unittest.IsolatedAsyncioTestCase):
    """HTTP schema loading must fail fast on client errors and must
    not serialize loads of different message types."""

    async def test_404_fails_fast_without_retry(self):
        codec = _http_codec()
        response = mock.Mock(status_code=404)
        codec._http_client = mock.Mock()
        codec._http_client.get = mock.AsyncMock(return_value=response)
        with (
            mock.patch.object(codecs, 'httpx', _fake_httpx()),
            mock.patch.object(codecs.asyncio, 'sleep') as sleep,
        ):
            with self.assertRaises(codecs.DecodeError) as ctx:
                await codec._avro_schema('missing_type')
        self.assertIn('HTTP 404', str(ctx.exception))
        self.assertEqual(codec._http_client.get.await_count, 1)
        sleep.assert_not_awaited()

    async def test_500_retries(self):
        codec = _http_codec()
        response = mock.Mock(status_code=503)
        codec._http_client = mock.Mock()
        codec._http_client.get = mock.AsyncMock(return_value=response)
        with (
            mock.patch.object(codecs, 'httpx', _fake_httpx()),
            mock.patch.object(codecs.asyncio, 'sleep', mock.AsyncMock()),
        ):
            with self.assertRaises(codecs.DecodeError):
                await codec._avro_schema('flaky_type')
        self.assertEqual(codec._http_client.get.await_count, 3)

    async def test_different_types_load_concurrently(self):
        codec = _http_codec()
        release = asyncio.Event()
        in_flight = 0
        max_in_flight = 0

        async def fake_get(uri):
            nonlocal in_flight, max_in_flight
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
            await release.wait()
            in_flight -= 1
            resp = mock.Mock(status_code=200)
            resp.json.return_value = {'type': 'record'}
            return resp

        codec._http_client = mock.Mock()
        codec._http_client.get = fake_get
        with mock.patch.object(codecs, 'httpx', _fake_httpx()):
            task_a = asyncio.create_task(codec._avro_schema('type_a'))
            task_b = asyncio.create_task(codec._avro_schema('type_b'))
            for _ in range(10):
                await asyncio.sleep(0)
            self.assertEqual(max_in_flight, 2)
            release.set()
            await asyncio.gather(task_a, task_b)

    async def test_same_type_fetched_once(self):
        codec = _http_codec()
        release = asyncio.Event()
        calls = 0

        async def fake_get(uri):
            nonlocal calls
            calls += 1
            await release.wait()
            resp = mock.Mock(status_code=200)
            resp.json.return_value = {'type': 'record'}
            return resp

        codec._http_client = mock.Mock()
        codec._http_client.get = fake_get
        with mock.patch.object(codecs, 'httpx', _fake_httpx()):
            task1 = asyncio.create_task(codec._avro_schema('same_type'))
            task2 = asyncio.create_task(codec._avro_schema('same_type'))
            for _ in range(10):
                await asyncio.sleep(0)
            release.set()
            results = await asyncio.gather(task1, task2)
        self.assertEqual(calls, 1)
        self.assertEqual(results[0], results[1])


if __name__ == '__main__':
    unittest.main()
