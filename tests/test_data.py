"""Tests for rejected.data"""
import unittest

from rejected import data

from . import mocks


class TestProperties(unittest.TestCase):

    def setUp(self):
        self._obj = data.Properties(mocks.PROPERTIES)

    def test_app_id(self):
        self.assertEqual(self._obj.app_id, mocks.PROPERTIES.app_id)

    def test_content_encoding(self):
        self.assertEqual(self._obj.content_encoding,
                         mocks.PROPERTIES.content_encoding)

    def test_content_type(self):
        self.assertEqual(self._obj.content_type, mocks.PROPERTIES.content_type)

    def test_correlation_id(self):
        self.assertEqual(self._obj.correlation_id,
                         mocks.PROPERTIES.correlation_id)

    def test_delivery_mode(self):
        self.assertEqual(self._obj.delivery_mode,
                         mocks.PROPERTIES.delivery_mode)

    def test_expiration(self):
        self.assertEqual(self._obj.expiration, mocks.PROPERTIES.expiration)

    def test_message_id(self):
        self.assertEqual(self._obj.message_id, mocks.PROPERTIES.message_id)

    def test_priority(self):
        self.assertEqual(self._obj.priority, mocks.PROPERTIES.priority)

    def test_reply_to(self):
        self.assertEqual(self._obj.reply_to, mocks.PROPERTIES.reply_to)

    def test_timestamp(self):
        self.assertEqual(self._obj.timestamp, mocks.PROPERTIES.timestamp)

    def test_type(self):
        self.assertEqual(self._obj.type, mocks.PROPERTIES.type)

    def test_user_id(self):
        self.assertEqual(self._obj.user_id, mocks.PROPERTIES.user_id)


class TestMessage(unittest.TestCase):

    def setUp(self):
        self._obj = data.Message(
            'mock', mocks.CHANNEL, mocks.METHOD, mocks.PROPERTIES, mocks.BODY,
            False)

    def test_body(self):
        self.assertEqual(self._obj.body, mocks.BODY)

    def test_channel(self):
        self.assertEqual(self._obj.channel, mocks.CHANNEL)

    def test_consumer_tag(self):
        self.assertEqual(self._obj.consumer_tag, mocks.METHOD.consumer_tag)

    def test_delivery_tag(self):
        self.assertEqual(self._obj.delivery_tag, mocks.METHOD.delivery_tag)

    def test_exchange(self):
        self.assertEqual(self._obj.exchange, mocks.METHOD.exchange)

    def test_method(self):
        self.assertEqual(self._obj.method, mocks.METHOD)

    def test_redelivered(self):
        self.assertEqual(self._obj.redelivered, mocks.METHOD.redelivered)

    def test_routing_key(self):
        self.assertEqual(self._obj.routing_key, mocks.METHOD.routing_key)

    def test_app_id(self):
        self.assertEqual(self._obj.properties.app_id, mocks.PROPERTIES.app_id)

    def test_content_encoding(self):
        self.assertEqual(self._obj.properties.content_encoding,
                         mocks.PROPERTIES.content_encoding)

    def test_content_type(self):
        self.assertEqual(self._obj.properties.content_type,
                         mocks.PROPERTIES.content_type)

    def test_correlation_id(self):
        self.assertEqual(self._obj.properties.correlation_id,
                         mocks.PROPERTIES.correlation_id)

    def test_delivery_mode(self):
        self.assertEqual(self._obj.properties.delivery_mode,
                         mocks.PROPERTIES.delivery_mode)

    def test_expiration(self):
        self.assertEqual(self._obj.properties.expiration,
                         mocks.PROPERTIES.expiration)

    def test_message_id(self):
        self.assertEqual(self._obj.properties.message_id,
                         mocks.PROPERTIES.message_id)

    def test_priority(self):
        self.assertEqual(self._obj.properties.priority,
                         mocks.PROPERTIES.priority)

    def test_reply_to(self):
        self.assertEqual(self._obj.properties.reply_to,
                         mocks.PROPERTIES.reply_to)

    def test_timestamp(self):
        self.assertEqual(self._obj.properties.timestamp,
                         mocks.PROPERTIES.timestamp)

    def test_type(self):
        self.assertEqual(self._obj.properties.type,
                         mocks.PROPERTIES.type)

    def test_user_id(self):
        self.assertEqual(self._obj.properties.user_id,
                         mocks.PROPERTIES.user_id)
