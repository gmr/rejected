import unittest
import uuid

import mock

from rejected import statsd


class TestCase(unittest.TestCase):

    def setUp(self):
        super(TestCase, self).setUp()
        self.name = str(uuid.uuid4())
        self.settings = {
            'host': '10.1.1.1',
            'port': 8124,
            'prefix': str(uuid.uuid4())
        }
        self.statsd = statsd.Client(self.name, self.settings)

    def test_address(self):
        self.assertEqual(self.statsd._address,
                         (self.settings['host'], self.settings['port']))

    def test_consumer_name(self):
        self.assertEqual(self.statsd._consumer_name, self.name)

    def test_prefix(self):
        self.assertEqual(self.statsd._prefix, self.settings['prefix'])

    def test_settings(self):
        self.assertDictEqual(self.statsd._settings, self.settings)


class SendTestCase(TestCase):

    def setUp(self):
        super(SendTestCase, self).setUp()
        self.socket = mock.Mock()
        self.statsd._socket = self.socket

    def payload_format(self, key, value, metric_type):
        return self.statsd.PAYLOAD_FORMAT.format(self.settings['prefix'],
                                                 self.statsd._hostname,
                                                 self.name, key, value,
                                                 metric_type).encode('utf-8')

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
