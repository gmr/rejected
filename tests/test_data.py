"""Tests for rejected.data"""
import sys
# Import unittest if 2.7, unittest2 if other version
if (sys.version_info[0], sys.version_info[1]) == (2, 7):
    import unittest
else:
    import unittest2 as unittest

from rejected import data
from . import mocks


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



class TestProperties(unittest.TestCase):

    def setUp(self):
        self._obj = data.Properties(mocks.MockHeader())

    def test_app_id(self):
        self.assertEqual(self._obj.app_id,
                         mocks.MockHeader.ATTRIBUTES['app_id'])

    def test_cluster_id(self):
        self.assertEqual(self._obj.cluster_id,
                         mocks.MockHeader.ATTRIBUTES['cluster_id'])

    def test_content_encoding(self):
        self.assertEqual(self._obj.content_encoding,
                         mocks.MockHeader.ATTRIBUTES['content_encoding'])

    def test_content_type(self):
        self.assertEqual(self._obj.content_type,
                         mocks.MockHeader.ATTRIBUTES['content_type'])

    def test_correlation_id(self):
        self.assertEqual(self._obj.correlation_id,
                         mocks.MockHeader.ATTRIBUTES['correlation_id'])

    def test_delivery_mode(self):
        self.assertEqual(self._obj.delivery_mode,
                         mocks.MockHeader.ATTRIBUTES['delivery_mode'])

    def test_expiration(self):
        self.assertEqual(self._obj.expiration,
                         mocks.MockHeader.ATTRIBUTES['expiration'])

    def test_headers(self):
        self.assertEqual(self._obj.headers,
                         mocks.MockHeader.ATTRIBUTES['headers'])

    def test_message_id(self):
        self.assertEqual(self._obj.message_id,
                         mocks.MockHeader.ATTRIBUTES['message_id'])

    def test_priority(self):
        self.assertEqual(self._obj.priority,
                         mocks.MockHeader.ATTRIBUTES['priority'])

    def test_reply_to(self):
        self.assertEqual(self._obj.reply_to,
                         mocks.MockHeader.ATTRIBUTES['reply_to'])

    def test_timestamp(self):
        self.assertEqual(self._obj.timestamp,
                         mocks.MockHeader.ATTRIBUTES['timestamp'])

    def test_type(self):
        self.assertEqual(self._obj.type,
                         mocks.MockHeader.ATTRIBUTES['type'])

    def test_user_id(self):
        self.assertEqual(self._obj.user_id,
                         mocks.MockHeader.ATTRIBUTES['user_id'])



class TestMessage(unittest.TestCase):

    BODY = 'This is a test Message'
    CHANNEL = 10

    def setUp(self):
        self._method = mocks.MockMethod()
        self._obj = data.Message(self.CHANNEL,
                                 self._method,
                                 data.Properties(mocks.MockHeader()),
                                 self.BODY)

    def test_app_id(self):
        self.assertEqual(self._obj.properties.app_id,
                         mocks.MockHeader.ATTRIBUTES['app_id'])

    def test_method(self):
        self.assertEqual(self._obj.body, self.BODY)

    def test_channel(self):
        self.assertEqual(self._obj.channel, self.CHANNEL)

    def test_cluster_id(self):
        self.assertEqual(self._obj.properties.cluster_id,
                         mocks.MockHeader.ATTRIBUTES['cluster_id'])

    def test_consumer_tag(self):
        self.assertEqual(self._obj.consumer_tag, mocks.MockMethod.consumer_tag)

    def test_content_encoding(self):
        self.assertEqual(self._obj.properties.content_encoding,
                         mocks.MockHeader.ATTRIBUTES['content_encoding'])

    def test_content_type(self):
        self.assertEqual(self._obj.properties.content_type,
                         mocks.MockHeader.ATTRIBUTES['content_type'])

    def test_correlation_id(self):
        self.assertEqual(self._obj.properties.correlation_id,
                         mocks.MockHeader.ATTRIBUTES['correlation_id'])

    def test_delivery_mode(self):
        self.assertEqual(self._obj.properties.delivery_mode,
                         mocks.MockHeader.ATTRIBUTES['delivery_mode'])

    def test_delivery_tag(self):
        self.assertEqual(self._obj.delivery_tag, mocks.MockMethod.delivery_tag)

    def test_exchange(self):
        self.assertEqual(self._obj.exchange, mocks.MockMethod.exchange)

    def test_expiration(self):
        self.assertEqual(self._obj.properties.expiration,
                         mocks.MockHeader.ATTRIBUTES['expiration'])

    def test_headers(self):
        self.assertEqual(self._obj.properties.headers,
                         mocks.MockHeader.ATTRIBUTES['headers'])

    def test_message_id(self):
        self.assertEqual(self._obj.properties.message_id,
                         mocks.MockHeader.ATTRIBUTES['message_id'])

    def test_method(self):
        self.assertEqual(self._obj.method, self._method)

    def test_priority(self):
        self.assertEqual(self._obj.properties.priority,
                         mocks.MockHeader.ATTRIBUTES['priority'])

    def test_redelivered(self):
        self.assertEqual(self._obj.redelivered, mocks.MockMethod.redelivered)

    def test_reply_to(self):
        self.assertEqual(self._obj.properties.reply_to,
                         mocks.MockHeader.ATTRIBUTES['reply_to'])

    def test_routing_key(self):
        self.assertEqual(self._obj.routing_key, mocks.MockMethod.routing_key)

    def test_timestamp(self):
        self.assertEqual(self._obj.properties.timestamp,
                         mocks.MockHeader.ATTRIBUTES['timestamp'])

    def test_type(self):
        self.assertEqual(self._obj.properties.type,
                         mocks.MockHeader.ATTRIBUTES['type'])

    def test_user_id(self):
        self.assertEqual(self._obj.properties.user_id,
                         mocks.MockHeader.ATTRIBUTES['user_id'])
