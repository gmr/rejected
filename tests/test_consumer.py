# coding=utf-8
"""Tests for rejected.consumer"""
from tornado import gen
import json
import mock
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from . import mocks
from tornado import testing

from rejected import consumer
from rejected import data


class ConsumerInitializationTests(unittest.TestCase):

    def test_configuration_is_assigned(self):
        cfg = {'foo': 'bar'}
        obj = consumer.Consumer(cfg, None)
        self.assertDictEqual(obj._settings, cfg)

    def test_channel_is_none(self):
        obj = consumer.Consumer({}, None)
        self.assertIsNone(obj._channel)

    def test_message_is_none(self):
        obj = consumer.Consumer({}, None)
        self.assertIsNone(obj._message)

    def test_initialize_is_invoked(self):
        with mock.patch('rejected.consumer.Consumer.initialize') as init:
            consumer.Consumer({}, None)
            init.assert_called_once_with()


class ConsumerDefaultProcessTests(unittest.TestCase):

    def test_process_raises_exception(self):
        obj = consumer.Consumer({}, None)
        self.assertRaises(NotImplementedError, obj.process)


class ConsumerSetChannelTests(unittest.TestCase):

    def test_set_channel_assigns_to_channel(self):
        obj = consumer.Consumer({}, None)
        channel = mock.Mock()
        obj._set_channel(channel)
        self.assertEqual(obj._channel, channel)


class TestConsumer(consumer.Consumer):
    def process(self):
        pass


class ConsumerReceiveTests(testing.AsyncTestCase):

    def setUp(self):
        super(ConsumerReceiveTests, self).setUp()
        self.obj = TestConsumer({}, None)
        self.message = data.Message(mocks.CHANNEL, mocks.METHOD,
                                    mocks.PROPERTIES, mocks.BODY)

    @testing.gen_test
    def test_receive_assigns_message(self):
        yield self.obj._execute(self.message)
        self.assertEqual(self.obj._message, self.message)

    @testing.gen_test
    def test_receive_invokes_process(self):
        with mock.patch.object(self.obj, 'process') as process:
            yield self.obj._execute(self.message)
            process.assert_called_once_with()

    @testing.gen_test
    def test_receive_drops_invalid_message_type(self):
        self.obj.MESSAGE_TYPE = 'fooz'
        self.obj.DROP_INVALID_MESSAGES = True
        with mock.patch.object(self.obj, 'process') as process:
            yield self.obj._execute(self.message)
            process.assert_not_called()

    @testing.gen_test
    def test_raises_with_drop(self):
        self.obj.MESSAGE_TYPE = 'foo'
        self.obj.DROP_INVALID_MESSAGES = True
        result = yield self.obj._execute(self.message)
        self.assertEqual(result, data.MESSAGE_DROP)


class ConsumerPropertyTests(testing.AsyncTestCase):

    @gen.coroutine
    def setUp(self):
        super(ConsumerPropertyTests, self).setUp()
        self.config = {'foo': 'bar', 'baz': 1, 'qux': True}
        self.message = data.Message(mocks.CHANNEL, mocks.METHOD,
                                    mocks.PROPERTIES, mocks.BODY)
        self.obj = TestConsumer(self.config, None)
        yield self.obj._execute(self.message)

    def test_app_id_property(self):
        self.assertEqual(self.obj.app_id, mocks.PROPERTIES.app_id)

    def test_body_property(self):
        self.assertEqual(self.obj.body, mocks.BODY)

    def test_configuration_property(self):
        self.assertDictEqual(self.obj.configuration, self.config)

    def test_content_encoding_property(self):
        self.assertEqual(self.obj.content_encoding,
                         mocks.PROPERTIES.content_encoding)

    def test_content_type_property(self):
        self.assertEqual(self.obj.content_type, mocks.PROPERTIES.content_type)

    def test_correlation_id_property(self):
        self.assertEqual(self.obj.correlation_id,
                         mocks.PROPERTIES.correlation_id)

    def test_exchange_property(self):
        self.assertEqual(self.obj.exchange, mocks.METHOD.exchange)

    def test_expiration_property(self):
        self.assertEqual(self.obj.expiration, mocks.PROPERTIES.expiration)

    def test_headers_property(self):
        self.assertDictEqual(self.obj.headers, mocks.PROPERTIES.headers)

    def test_message_id_property(self):
        self.assertEqual(self.obj.message_id, mocks.PROPERTIES.message_id)

    def test_name_property(self):
        self.assertEqual(self.obj.name, self.obj.__class__.__name__)

    def test_priority_property(self):
        self.assertEqual(self.obj.priority, mocks.PROPERTIES.priority)

    def test_properties_property(self):
        self.assertDictEqual(self.obj.properties,
                             dict(data.Properties(mocks.PROPERTIES)))

    def test_redelivered_property(self):
        self.assertEqual(self.obj.redelivered, mocks.METHOD.redelivered)

    def test_reply_to_property(self):
        self.assertEqual(self.obj.reply_to, mocks.PROPERTIES.reply_to)

    def test_routing_key_property(self):
        self.assertEqual(self.obj.routing_key, mocks.METHOD.routing_key)

    def test_message_type_property(self):
        self.assertEqual(self.obj.message_type, mocks.PROPERTIES.type)

    def test_timestamp_property(self):
        self.assertEqual(self.obj.timestamp, mocks.PROPERTIES.timestamp)

    def test_user_id_property(self):
        self.assertEqual(self.obj.user_id, mocks.PROPERTIES.user_id)


class TestSmartConsumer(consumer.SmartConsumer):
    def process(self):
        pass


class TestSmartConsumerWithJSON(unittest.TestCase):

    def setUp(self):
        self.body = {'foo': 'bar', 'baz': 1, 'qux': True}
        self.message = data.Message(mocks.CHANNEL, mocks.METHOD,
                                    mocks.PROPERTIES, json.dumps(self.body))
        self.obj = TestSmartConsumer({}, None)
        self.obj._execute(self.message)

    def test_message_body_property(self):
        self.assertDictEqual(self.obj.body, self.body)
