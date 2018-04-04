import socket
try:
    import unittest2 as unittest
except ImportError:
    import unittest
import uuid

import mock

from rejected import statsd


class TestCase(unittest.TestCase):

    def setUp(self):
        super(TestCase, self).setUp()
        self.name = str(uuid.uuid4())
        self.settings = self.get_settings()
        self.statsd = statsd.StatsdClient(self.name, self.settings)

    @staticmethod
    def get_settings():
        return {
            'host': '10.1.1.1',
            'port': 8124,
            'prefix': str(uuid.uuid4())
        }

    def payload_format(self, key, value, metric_type):
        return self.statsd._build_payload(key, value, metric_type)

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


class SendTestCase(TestCase):
    def setUp(self):
        super(SendTestCase, self).setUp()
        self.socket = mock.Mock()
        self.statsd._socket = self.socket

    def test_hostname_in_metric(self):
        self.statsd.add_timing('foo', 2.5)
        value = self.payload_format('foo', 2500.0, 'ms')
        self.assertIn(socket.gethostname().split('.')[0], value)

    def test_add_timing(self):
        self.statsd.add_timing('foo', 2.5)
        expectation = self.payload_format('foo', 2500.0, 'ms')
        self.socket.sendto.assert_called_once_with(expectation.encode('utf-8'),
                                                   self.statsd._address)

    def test_incr(self):
        self.statsd.incr('bar', 2)
        expectation = self.payload_format('bar', 2, 'c')
        self.socket.sendto.assert_called_once_with(expectation.encode('utf-8'),
                                                   self.statsd._address)

    def test_set_gauge(self):
        self.statsd.set_gauge('baz', 98.5)
        expectation = self.payload_format('baz', 98.5, 'g')
        self.socket.sendto.assert_called_once_with(expectation.encode('utf-8'),
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
        self.assertNotIn(socket.gethostname().split('.')[0], value)
