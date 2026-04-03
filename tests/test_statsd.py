import asyncio
import logging
import re
import socket
import unittest
import uuid
from unittest import mock

from rejected import statsd

LOGGER = logging.getLogger(__name__)


class TestCase(unittest.TestCase):
    def setUp(self):
        self.failure_callback = mock.Mock()
        self.name = str(uuid.uuid4())
        self.settings = self.get_settings()
        self.statsd = statsd.Client(
            self.name, self.settings, self.failure_callback
        )

    @staticmethod
    def get_settings():
        return {
            'host': '10.1.1.1',
            'port': 8124,
            'prefix': str(uuid.uuid4()),
            'tcp': False,
        }

    def payload_format(self, key, value, metric_type):
        return self.statsd._build_payload(key, value, metric_type).encode(
            'utf-8'
        )


class UDPTestCase(TestCase):
    def test_address(self):
        self.assertEqual(
            self.statsd._address,
            (self.settings['host'], self.settings['port']),
        )

    def test_consumer_name(self):
        self.assertEqual(self.statsd._consumer_name, self.name)

    def test_prefix(self):
        self.assertEqual(self.statsd._prefix, self.settings['prefix'])

    def test_settings(self):
        for key in self.settings:
            self.assertEqual(
                self.statsd._setting(key, None), self.settings[key]
            )


class UDPSendTestCase(TestCase):
    def setUp(self):
        super().setUp()
        self.socket = mock.Mock()
        self.statsd._udp_sock = self.socket

    def test_hostname_in_metric(self):
        self.statsd.add_timing('foo', 2.5)
        value = self.payload_format('foo', 2500.0, 'ms')
        self.assertIn(
            socket.gethostname().split('.')[0].encode('utf-8'), value
        )

    def test_add_timing(self):
        self.statsd.add_timing('foo', 2.5)
        expectation = self.payload_format('foo', 2500.0, 'ms')
        self.socket.sendto.assert_called_once_with(
            expectation, self.statsd._address
        )

    def test_incr(self):
        self.statsd.incr('bar', 2)
        expectation = self.payload_format('bar', 2, 'c')
        self.socket.sendto.assert_called_once_with(
            expectation, self.statsd._address
        )

    def test_set_gauge(self):
        self.statsd.set_gauge('baz', 98.5)
        expectation = self.payload_format('baz', 98.5, 'g')
        self.socket.sendto.assert_called_once_with(
            expectation, self.statsd._address
        )


class NoHostnameTestCase(TestCase):
    @staticmethod
    def get_settings():
        return {
            'host': '10.1.1.1',
            'port': 8124,
            'prefix': str(uuid.uuid4()),
            'include_hostname': False,
        }

    def test_hostname_in_metric(self):
        self.statsd.add_timing('foo', 2.5)
        value = self.payload_format('foo', 2500.0, 'ms')
        self.assertNotIn(
            socket.gethostname().split('.')[0].encode('utf-8'), value
        )


class StatsdServer(asyncio.Protocol):
    PATTERN = rb'[a-z0-9._-]+:[0-9.]+\|(?:g|c|ms)\n'

    def __init__(self):
        self.event = asyncio.Event()
        self.packets = []
        self.reconnect_receive = False
        self._buffer = b''
        self._transport = None

    def connection_made(self, transport):
        self._transport = transport
        LOGGER.debug('Connected %r', transport.get_extra_info('peername'))

    def data_received(self, data):
        self._buffer += data
        last_end = 0
        for match in re.finditer(self.PATTERN, self._buffer):
            result = match.group(0)
            last_end = match.end()
            self.event.set()
            LOGGER.debug('Received %r', result)
            self.packets.append(result)
            if b'reconnect' in result:
                self.reconnect_receive = True
                if self._transport:
                    self._transport.close()
                self._buffer = self._buffer[last_end:]
                return
        self._buffer = self._buffer[last_end:]

    def connection_lost(self, exc):
        LOGGER.debug('Connection lost: %r', exc)


class TCPTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.failure_callback = mock.Mock()
        self.name = str(uuid.uuid4())

        loop = asyncio.get_running_loop()
        self._server_protocol = StatsdServer()
        self._server = await loop.create_server(
            lambda: self._server_protocol, '127.0.0.1', 0
        )
        self.port = self._server.sockets[0].getsockname()[1]

        self.settings = self.get_settings()
        LOGGER.debug('Settings: %r', self.settings)
        self.statsd = statsd.Client(
            self.name, self.settings, self.failure_callback
        )

    async def asyncTearDown(self):
        if self.statsd._tcp_writer:
            self.statsd.stop()
        self._server.close()
        await self._server.wait_closed()

    def get_settings(self):
        return {
            'host': '127.0.0.1',
            'port': self.port,
            'prefix': str(uuid.uuid4()),
            'tcp': True,
        }

    def payload_format(self, key, value, metric_type):
        return self.statsd._build_payload(key, value, metric_type).encode(
            'utf-8'
        )

    async def test_add_timing(self):
        self.statsd.add_timing('foo', 2.5)
        await asyncio.wait_for(self._server_protocol.event.wait(), timeout=5)
        self.assertIn(
            self.payload_format('foo', 2500.0, 'ms'),
            self._server_protocol.packets,
        )

    async def test_incr(self):
        self.statsd.incr('bar', 2)
        await asyncio.wait_for(self._server_protocol.event.wait(), timeout=5)
        self.assertIn(
            self.payload_format('bar', 2, 'c'), self._server_protocol.packets
        )

    async def test_set_gauge(self):
        self.statsd.set_gauge('baz', 98.5)
        await asyncio.wait_for(self._server_protocol.event.wait(), timeout=5)
        self.assertIn(
            self.payload_format('baz', 98.5, 'g'),
            self._server_protocol.packets,
        )

    async def test_reconnect(self):
        self.statsd.set_gauge('baz', 98.5)
        await asyncio.wait_for(self._server_protocol.event.wait(), timeout=5)
        self._server_protocol.event.clear()
        self.statsd.set_gauge('reconnect', 100)
        await asyncio.wait_for(self._server_protocol.event.wait(), timeout=5)
        self._server_protocol.event.clear()
        await asyncio.sleep(2)
        self.assertTrue(self._server_protocol.reconnect_receive)
        self.statsd._tcp_writer = self.statsd._tcp_socket()
        self.statsd.set_gauge('bar', 10)
        await asyncio.wait_for(self._server_protocol.event.wait(), timeout=5)
        self.assertTrue(self._server_protocol.reconnect_receive)

        self.assertIn(
            self.payload_format('baz', 98.5, 'g'),
            self._server_protocol.packets,
        )
        self.assertIn(
            self.payload_format('reconnect', 100, 'g'),
            self._server_protocol.packets,
        )
        self.assertIn(
            self.payload_format('bar', 10, 'g'), self._server_protocol.packets
        )
