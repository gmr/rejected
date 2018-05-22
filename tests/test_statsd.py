import re
import socket
import unittest
import uuid

import mock
import random
from tornado import gen, iostream, tcpserver, testing

from rejected import statsd

class TestCase(unittest.TestCase):

    def setUp(self):
        self.name = str(uuid.uuid4())
        self.settings = self.get_settings()
        self.statsd = statsd.Client(self.name, self.settings)

    @staticmethod
    def get_settings():
        return {
            'host': '10.1.1.1',
            'port': 8124,
            'prefix': str(uuid.uuid4()),
            'tcp': 'false'
        }

    def payload_format(self, key, value, metric_type):
        return self.statsd._build_payload(
            key, value, metric_type).encode('utf-8')


class UDPTestCase(TestCase):

    def test_address(self):
        self.assertEqual(self.statsd._address,
                         (self.settings['host'], self.settings['port']))

    def test_consumer_name(self):
        self.assertEqual(self.statsd._consumer_name, self.name)

    def test_prefix(self):
        self.assertEqual(self.statsd._prefix, self.settings['prefix'])

    def test_settings(self):
        for key in self.settings:
            self.assertEqual(self.statsd._setting(key, None),
                             self.settings[key])


class UDPSendTestCase(TestCase):

    def setUp(self):
        super(UDPSendTestCase, self).setUp()
        self.socket = mock.Mock()
        self.statsd._udp_sock = self.socket

    def test_hostname_in_metric(self):
        self.statsd.add_timing('foo', 2.5)
        value = self.payload_format('foo', 2500.0, 'ms')
        self.assertIn(socket.gethostname().split('.')[0].encode('utf-8'), value)

    def test_add_timing(self):
        self.statsd.add_timing('foo', 2.5)
        expectation = self.payload_format('foo', 2500.0, 'ms')
        self.socket.sendto.assert_called_once_with(expectation,
                                                   self.statsd._address)

    def test_incr(self):
        self.statsd.incr('bar', 2)
        expectation = self.payload_format('bar', 2, 'c')
        self.socket.sendto.assert_called_once_with(expectation,
                                                   self.statsd._address)

    def test_set_gauge(self):
        self.statsd.set_gauge('baz', 98.5)
        expectation = self.payload_format('baz', 98.5, 'g')
        self.socket.sendto.assert_called_once_with(expectation,
                                                   self.statsd._address)


class NoHostnameTestCase(TestCase):

    @staticmethod
    def get_settings():
        return {
            'host': '10.1.1.1',
            'port': 8124,
            'prefix': str(uuid.uuid4()),
            'include_hostname': False
        }

    def test_hostname_in_metric(self):
        self.statsd.add_timing('foo', 2.5)
        value = self.payload_format('foo', 2500.0, 'ms')
        self.assertNotIn(socket.gethostname().split('.')[0].encode('utf-8'),
                         value)


class StatsdServer(tcpserver.TCPServer):

    PATTERN = br'[a-z0-9._-]+:[0-9.]+\|(?:g|c|ms)'

    def __init__(self, ssl_options=None, max_buffer_size=None,
                 read_chunk_size=None):
        self.packets = []
        self.reconnect_receive = False
        super(StatsdServer, self).__init__(
            ssl_options, max_buffer_size, read_chunk_size)

    def handle_stream(self, stream, address):

        def read_callback(future):
            result = future.result()
            self.packets.append(result)
            if b'reconnect' in result:
                self.reconnect_receive = True
                stream.close()
                return
            inner_future = stream.read_until_regex(self.PATTERN)
            self.io_loop.add_future(inner_future, read_callback)

        future = stream.read_until_regex(self.PATTERN)
        self.io_loop.add_future(future, read_callback)


class TCPTestCase(testing.AsyncTestCase):

    def setUp(self):
        super(TCPTestCase, self).setUp()
        self.sock, self.statsd_port = testing.bind_unused_port()
        self.name = str(uuid.uuid4())
        self.settings = self.get_settings()
        self.statsd = statsd.Client(self.name, self.settings)
        self.server = StatsdServer()
        self.server.add_socket(self.sock)

    def get_settings(self):
        return {
            'host': '127.0.0.1',
            'port': self.statsd_port,
            'prefix': str(uuid.uuid4()),
            'tcp': 'true'
        }

    def payload_format(self, key, value, metric_type):
        return self.statsd._build_payload(key, value, metric_type).encode('utf-8')

    @testing.gen_test
    def test_add_timing(self):
        self.statsd.add_timing('foo', 2.5)
        yield gen.sleep(0.1)
        self.assertIn(self.payload_format('foo', 2500.0, 'ms'), self.server.packets)

    @testing.gen_test
    def test_incr(self):
        self.statsd.incr('bar', 2)
        yield gen.sleep(0.1)
        self.assertIn(self.payload_format('bar', 2, 'c'), self.server.packets)

    @testing.gen_test
    def test_set_gauge(self):
        self.statsd.set_gauge('baz', 98.5)
        yield gen.sleep(0.1)
        self.assertIn(self.payload_format('baz', 98.5, 'g'), self.server.packets)

    @testing.gen_test
    def test_reconnect(self):
        self.statsd.set_gauge('baz', 98.5)
        yield gen.sleep(0.1)
        self.statsd.set_gauge('reconnect', 100)
        yield gen.sleep(0.1)
        self.statsd.set_gauge('bar', 10)
        yield gen.sleep(0.1)
        self.assertIn(self.payload_format('baz', 98.5, 'g'), self.server.packets)
        self.assertIn(self.payload_format('reconnect', 100, 'g'), self.server.packets)
        self.assertIn(self.payload_format('bar', 10, 'g'), self.server.packets)
