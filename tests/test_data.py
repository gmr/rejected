"""Tests for rejected.data"""
import random
import time
import unittest
import uuid

from rejected import data

from . import mocks


class TestProperties(unittest.TestCase):

    def setUp(self):
        self.properties = data.Properties(mocks.PROPERTIES)

    def test_app_id(self):
        self.assertEqual(self.properties.app_id, mocks.PROPERTIES.app_id)

    def test_content_encoding(self):
        self.assertEqual(self.properties.content_encoding,
                         mocks.PROPERTIES.content_encoding)

    def test_content_type(self):
        self.assertEqual(self.properties.content_type,
                         mocks.PROPERTIES.content_type)

    def test_correlation_id(self):
        self.assertEqual(self.properties.correlation_id,
                         mocks.PROPERTIES.correlation_id)

    def test_delivery_mode(self):
        self.assertEqual(self.properties.delivery_mode,
                         mocks.PROPERTIES.delivery_mode)

    def test_expiration(self):
        self.assertEqual(self.properties.expiration,
                         mocks.PROPERTIES.expiration)

    def test_message_id(self):
        self.assertEqual(self.properties.message_id,
                         mocks.PROPERTIES.message_id)

    def test_priority(self):
        self.assertEqual(self.properties.priority,
                         mocks.PROPERTIES.priority)

    def test_reply_to(self):
        self.assertEqual(self.properties.reply_to,
                         mocks.PROPERTIES.reply_to)

    def test_timestamp(self):
        self.assertEqual(self.properties.timestamp,
                         mocks.PROPERTIES.timestamp)

    def test_type(self):
        self.assertEqual(self.properties.type,
                         mocks.PROPERTIES.type)

    def test_user_id(self):
        self.assertEqual(self.properties.user_id,
                         mocks.PROPERTIES.user_id)


class TestPartialProperties(unittest.TestCase):

    def setUp(self):
        self.properties = data.Properties(
            content_type='application/json', priority=2)

    def test_app_id(self):
        self.assertIsNone(self.properties.app_id)

    def test_content_encoding(self):
        self.assertIsNone(self.properties.content_encoding)

    def test_content_type(self):
        self.assertEqual(self.properties.content_type, 'application/json')

    def test_correlation_id(self):
        self.assertIsNone(self.properties.correlation_id)

    def test_delivery_mode(self):
        self.assertIsNone(self.properties.delivery_mode)

    def test_expiration(self):
        self.assertIsNone(self.properties.expiration)

    def test_message_id(self):
        self.assertIsNone(self.properties.message_id)

    def test_priority(self):
        self.assertEqual(self.properties.priority, 2)

    def test_reply_to(self):
        self.assertIsNone(self.properties.app_id)

    def test_timestamp(self):
        self.assertIsNone(self.properties.timestamp)

    def test_type(self):
        self.assertIsNone(self.properties.type)

    def test_user_id(self):
        self.assertIsNone(self.properties.user_id)


class TestMessage(unittest.TestCase):

    def setUp(self):
        self.message = data.Message(
            'mock', mocks.CHANNEL, mocks.METHOD, mocks.PROPERTIES, mocks.BODY)

    def test_body(self):
        self.assertEqual(self.message.body, mocks.BODY)

    def test_channel(self):
        self.assertEqual(self.message.channel, mocks.CHANNEL)

    def test_consumer_tag(self):
        self.assertEqual(self.message.consumer_tag, mocks.METHOD.consumer_tag)

    def test_delivery_tag(self):
        self.assertEqual(self.message.delivery_tag, mocks.METHOD.delivery_tag)

    def test_exchange(self):
        self.assertEqual(self.message.exchange, mocks.METHOD.exchange)

    def test_method(self):
        self.assertEqual(self.message.method, mocks.METHOD)

    def test_redelivered(self):
        self.assertEqual(self.message.redelivered, mocks.METHOD.redelivered)

    def test_routing_key(self):
        self.assertEqual(self.message.routing_key, mocks.METHOD.routing_key)

    def test_app_id(self):
        self.assertEqual(self.message.properties.app_id,
                         mocks.PROPERTIES.app_id)

    def test_content_encoding(self):
        self.assertEqual(self.message.properties.content_encoding,
                         mocks.PROPERTIES.content_encoding)

    def test_content_type(self):
        self.assertEqual(self.message.properties.content_type,
                         mocks.PROPERTIES.content_type)

    def test_correlation_id(self):
        self.assertEqual(self.message.properties.correlation_id,
                         mocks.PROPERTIES.correlation_id)

    def test_delivery_mode(self):
        self.assertEqual(self.message.properties.delivery_mode,
                         mocks.PROPERTIES.delivery_mode)

    def test_expiration(self):
        self.assertEqual(self.message.properties.expiration,
                         mocks.PROPERTIES.expiration)

    def test_message_id(self):
        self.assertEqual(self.message.properties.message_id,
                         mocks.PROPERTIES.message_id)

    def test_priority(self):
        self.assertEqual(self.message.properties.priority,
                         mocks.PROPERTIES.priority)

    def test_reply_to(self):
        self.assertEqual(self.message.properties.reply_to,
                         mocks.PROPERTIES.reply_to)

    def test_timestamp(self):
        self.assertEqual(self.message.properties.timestamp,
                         mocks.PROPERTIES.timestamp)

    def test_type(self):
        self.assertEqual(self.message.properties.type,
                         mocks.PROPERTIES.type)

    def test_user_id(self):
        self.assertEqual(self.message.properties.user_id,
                         mocks.PROPERTIES.user_id)


class TestMeasurement(unittest.TestCase):

    def setUp(self):
        self.measurement = data.Measurement()

    def test_iter_and_default_values(self):
        for _key, value in self.measurement:
            self.assertDictEqual(dict(value), {})

    def test_repr(self):
        self.assertEqual(repr(self.measurement),
                         '<Measurement id={}>'.format(id(self.measurement)))

    def test_incr_decr(self):
        keys = [str(uuid.uuid4()) for _i in range(0, 10)]
        expectation = {}
        for key in keys:
            self.measurement.incr(key)
            self.measurement.incr(key, 5)
            self.measurement.decr(key)
            self.measurement.decr(key, 2)
            expectation[key] = 3
        self.assertDictEqual(dict(self.measurement.counters), expectation)

    def test_tags(self):
        self.measurement.set_tag('foo', 'bar')
        self.measurement.set_tag('baz', True)
        self.measurement.set_tag('qux', 1)
        self.assertDictEqual(self.measurement.tags,
                             {'foo': 'bar', 'baz': True, 'qux': 1})

    def test_add_duration(self):
        expectation = random.random()
        self.measurement.add_duration('duration1', expectation)
        self.measurement.add_duration('duration1', expectation)
        self.assertEqual(self.measurement.durations['duration1'],
                         [expectation, expectation])

    def test_set_value(self):
        key = str(uuid.uuid4())
        expectation = random.random()
        self.measurement.set_value(key, 10)
        self.measurement.set_value(key, expectation)
        self.assertEqual(self.measurement.values[key], expectation)

    def test_track_duration(self):
        key = str(uuid.uuid4())
        with self.measurement.track_duration(key):
            time.sleep(0.01)
        with self.measurement.track_duration(key):
            time.sleep(0.02)
        self.assertGreaterEqual(self.measurement.durations[key][0], 0.01)
        self.assertGreaterEqual(self.measurement.durations[key][1], 0.02)
