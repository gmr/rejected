"""Tests for rejected.data"""
import mock
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from rejected import data


class TestDataObject(unittest.TestCase):

    ATTRIBUTES = {'test': 'Attribute',
                  'boolean': True,
                  'intval': 10}

    def setUp(self):
        self._obj = data.DataObject()
        for attribute in self.ATTRIBUTES:
            setattr(self._obj, attribute, self.ATTRIBUTES[attribute])

    def testRepr(self):
        self.assertEqual(repr(self._obj),
                         "<DataObject(['test=Attribute', 'boolean=True', "
                         "'intval=10'])>")


class MockHeader(object):

    ATTRIBUTES = {'content_type': 'text/html',
                  'content_encoding': 'gzip',
                  'headers': {'foo': 'bar', 'boolean': True},
                  'delivery_mode': 1,
                  'priority': 0,
                  'correlation_id': 'abc123',
                  'reply_to': 'def456',
                  'expiration': 1341855318,
                  'message_id': 'ghi789',
                  'timestamp': 1341855300,
                  'type': 'test_message',
                  'user_id': 'nose',
                  'app_id': 'nosetests',
                  'cluster_id': 'mock'}

    def __init__(self):
        for attribute in self.ATTRIBUTES:
            setattr(self, attribute, self.ATTRIBUTES[attribute])


class TestProperties(unittest.TestCase):

    def setUp(self):
        self._obj = data.Properties(MockHeader())

    def test_app_id(self):
        self.assertEqual(self._obj.app_id,
                         MockHeader.ATTRIBUTES['app_id'])

    def test_cluster_id(self):
        self.assertEqual(self._obj.cluster_id,
                         MockHeader.ATTRIBUTES['cluster_id'])

    def test_content_encoding(self):
        self.assertEqual(self._obj.content_encoding,
                         MockHeader.ATTRIBUTES['content_encoding'])

    def test_content_type(self):
        self.assertEqual(self._obj.content_type,
                         MockHeader.ATTRIBUTES['content_type'])

    def test_correlation_id(self):
        self.assertEqual(self._obj.correlation_id,
                         MockHeader.ATTRIBUTES['correlation_id'])

    def test_delivery_mode(self):
        self.assertEqual(self._obj.delivery_mode,
                         MockHeader.ATTRIBUTES['delivery_mode'])

    def test_expiration(self):
        self.assertEqual(self._obj.expiration,
                         MockHeader.ATTRIBUTES['expiration'])

    def test_headers(self):
        self.assertEqual(self._obj.headers,
                         MockHeader.ATTRIBUTES['headers'])

    def test_message_id(self):
        self.assertEqual(self._obj.message_id,
                         MockHeader.ATTRIBUTES['message_id'])

    def test_priority(self):
        self.assertEqual(self._obj.priority,
                         MockHeader.ATTRIBUTES['priority'])

    def test_reply_to(self):
        self.assertEqual(self._obj.reply_to,
                         MockHeader.ATTRIBUTES['reply_to'])

    def test_timestamp(self):
        self.assertEqual(self._obj.timestamp,
                         MockHeader.ATTRIBUTES['timestamp'])

    def test_type(self):
        self.assertEqual(self._obj.type,
                         MockHeader.ATTRIBUTES['type'])

    def test_user_id(self):
        self.assertEqual(self._obj.user_id,
                         MockHeader.ATTRIBUTES['user_id'])


class MockMethod(object):

    consumer_tag = 'tag0'
    delivery_tag = 10
    exchange = 'unittests'
    redelivered = False
    routing_key = 'mock_tests'


class TestMessage(unittest.TestCase):

    BODY = 'This is a test Message'
    CHANNEL = 10

    def setUp(self):
        self._method = MockMethod()
        self._obj = data.Message(self.CHANNEL,
                                 self._method,
                                 data.Properties(MockHeader()),
                                 self.BODY)

    def test_app_id(self):
        self.assertEqual(self._obj.properties.app_id,
                         MockHeader.ATTRIBUTES['app_id'])

    def test_method(self):
        self.assertEqual(self._obj.body, self.BODY)

    def test_channel(self):
        self.assertEqual(self._obj.channel, self.CHANNEL)

    def test_cluster_id(self):
        self.assertEqual(self._obj.properties.cluster_id,
                         MockHeader.ATTRIBUTES['cluster_id'])

    def test_consumer_tag(self):
        self.assertEqual(self._obj.consumer_tag, MockMethod.consumer_tag)

    def test_content_encoding(self):
        self.assertEqual(self._obj.properties.content_encoding,
                         MockHeader.ATTRIBUTES['content_encoding'])

    def test_content_type(self):
        self.assertEqual(self._obj.properties.content_type,
                         MockHeader.ATTRIBUTES['content_type'])

    def test_correlation_id(self):
        self.assertEqual(self._obj.properties.correlation_id,
                         MockHeader.ATTRIBUTES['correlation_id'])

    def test_delivery_mode(self):
        self.assertEqual(self._obj.properties.delivery_mode,
                         MockHeader.ATTRIBUTES['delivery_mode'])

    def test_delivery_tag(self):
        self.assertEqual(self._obj.delivery_tag, MockMethod.delivery_tag)

    def test_exchange(self):
        self.assertEqual(self._obj.exchange, MockMethod.exchange)

    def test_expiration(self):
        self.assertEqual(self._obj.properties.expiration,
                         MockHeader.ATTRIBUTES['expiration'])

    def test_headers(self):
        self.assertEqual(self._obj.properties.headers,
                         MockHeader.ATTRIBUTES['headers'])

    def test_message_id(self):
        self.assertEqual(self._obj.properties.message_id,
                         MockHeader.ATTRIBUTES['message_id'])

    def test_method(self):
        self.assertEqual(self._obj.method, self._method)

    def test_priority(self):
        self.assertEqual(self._obj.properties.priority,
                         MockHeader.ATTRIBUTES['priority'])

    def test_redelivered(self):
        self.assertEqual(self._obj.redelivered, MockMethod.redelivered)

    def test_reply_to(self):
        self.assertEqual(self._obj.properties.reply_to,
                         MockHeader.ATTRIBUTES['reply_to'])

    def test_routing_key(self):
        self.assertEqual(self._obj.routing_key, MockMethod.routing_key)

    def test_timestamp(self):
        self.assertEqual(self._obj.properties.timestamp,
                         MockHeader.ATTRIBUTES['timestamp'])

    def test_type(self):
        self.assertEqual(self._obj.properties.type,
                         MockHeader.ATTRIBUTES['type'])

    def test_user_id(self):
        self.assertEqual(self._obj.properties.user_id,
                         MockHeader.ATTRIBUTES['user_id'])
