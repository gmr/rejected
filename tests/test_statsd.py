import socket
import unittest
import uuid
try:
    from unittest import mock
except ImportError:
    import mock

from rejected import statsd
from tornado import gen, iostream, locks, tcpserver, testing


class TestCase(unittest.TestCase):

    def setUp(self):
        self.failure_callback = mock.Mock()
        self.name = str(uuid.uuid4())
        self.settings = self.get_settings()
        self.statsd = statsd.Client(
            self.name, self.settings, self.failure_callback)

    @staticmethod
    def get_settings():
        return {
            'host': '10.1.1.1',
            'port': 8124,
            'prefix': str(uuid.uuid4()),
            'tcp': False
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
        self.assertIn(socket.gethostname().split('.')[0].encode('utf-8'),
                      value)

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

    PATTERN = br'[a-z0-9._-]+:[0-9.]+\|(?:g|c|ms)\n'

    def __init__(self, ssl_options=None, max_buffer_size=None,
                 read_chunk_size=None):
        self.event = locks.Event()
        self.packets = []
        self.reconnect_receive = False
        super(StatsdServer, self).__init__(
            ssl_options, max_buffer_size, read_chunk_size)

    @gen.coroutine
    def handle_stream(self, stream, address):
        print('Connected', address)
        while True:
            try:
                result = yield stream.read_until_regex(self.PATTERN)
            except iostream.StreamClosedError:
                break
            else:
                self.event.set()
                print('Received %r' % result)
                self.packets.append(result)
                if b'reconnect' in result:
                    self.reconnect_receive = True
                    stream.close()
                    return


class TCPTestCase(testing.AsyncTestCase):

    def setUp(self):
        super(TCPTestCase, self).setUp()
        self.failure_callback = mock.Mock()
        self.sock, self.port = testing.bind_unused_port()
        self.name = str(uuid.uuid4())
        self.settings = self.get_settings()
        print(self.settings)
        self.statsd = statsd.Client(
            self.name, self.settings, self.failure_callback)
        self.server = StatsdServer()
        self.server.add_socket(self.sock)

    def get_settings(self):
        return {
            'host': self.sock.getsockname()[0],
            'port': self.port,
            'prefix': str(uuid.uuid4()),
            'tcp': True
        }

    def payload_format(self, key, value, metric_type):
        return self.statsd._build_payload(
            key, value, metric_type).encode('utf-8')

    @testing.gen_test
    def test_add_timing(self):
        self.statsd.add_timing('foo', 2.5)
        yield self.server.event.wait()
        self.assertIn(self.payload_format('foo', 2500.0, 'ms'),
                      self.server.packets)

    @testing.gen_test
    def test_incr(self):
        self.statsd.incr('bar', 2)
        yield self.server.event.wait()
        self.assertIn(self.payload_format('bar', 2, 'c'), self.server.packets)

    @testing.gen_test
    def test_set_gauge(self):
        self.statsd.set_gauge('baz', 98.5)
        yield self.server.event.wait()
        self.assertIn(self.payload_format('baz', 98.5, 'g'),
                      self.server.packets)

    @testing.gen_test
    def test_reconnect(self):
        self.statsd.set_gauge('baz', 98.5)
        yield self.server.event.wait()
        self.server.event.clear()
        self.statsd.set_gauge('reconnect', 100)
        yield self.server.event.wait()
        self.server.event.clear()
        yield gen.sleep(2)
        self.assertTrue(self.server.reconnect_receive)
        self.statsd.set_gauge('bar', 10)
        yield self.server.event.wait()
        self.assertTrue(self.server.reconnect_receive)

        self.assertIn(self.payload_format('baz', 98.5, 'g'),
                      self.server.packets)
        self.assertIn(self.payload_format('reconnect', 100, 'g'),
                      self.server.packets)
        self.assertIn(self.payload_format('bar', 10, 'g'),
                      self.server.packets)
