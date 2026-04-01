"""Tests for rejected.consumer"""

import json
import unittest
from unittest import mock

from rejected import consumer, data

from . import mocks


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
        obj.set_channel('mock', channel)
        self.assertEqual(obj._channels['mock'], channel)


class TestConsumer(consumer.Consumer):
    def process(self):
        pass


class ConsumerReceiveTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.obj = TestConsumer({}, None)
        self.message = data.Message(
            'mock',
            mocks.CHANNEL,
            mocks.METHOD,
            mocks.PROPERTIES,
            mocks.BODY,
            False,
        )
        self.measurement = data.Measurement()

    async def test_receive_assigns_message(self):
        await self.obj.execute(self.message, self.measurement)
        self.assertEqual(self.obj._message, self.message)

    async def test_receive_invokes_process(self):
        with mock.patch.object(self.obj, 'process') as process:
            await self.obj.execute(self.message, self.measurement)
            process.assert_called_once_with()

    async def test_receive_drops_invalid_message_type(self):
        obj = TestConsumer(
            {}, None, drop_invalid_messages=True, message_type='foo'
        )
        with mock.patch.object(obj, 'process') as process:
            await self.obj.execute(self.message, self.measurement)
            process.assert_not_called()

    async def test_raises_with_drop(self):
        obj = TestConsumer(
            {}, None, drop_invalid_messages=True, message_type='foo'
        )
        result = await obj.execute(self.message, self.measurement)
        self.assertEqual(result, data.MESSAGE_DROP)


class ConsumerPropertyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.config = {'foo': 'bar', 'baz': 1, 'qux': True}
        self.message = data.Message(
            'mock',
            mocks.CHANNEL,
            mocks.METHOD,
            mocks.PROPERTIES,
            mocks.BODY,
            False,
        )
        self.measurement = data.Measurement()

    async def run_consumer(self):
        self.obj = TestConsumer(self.config, None)
        await self.obj.execute(self.message, self.measurement)

    async def test_app_id_property(self):
        await self.run_consumer()
        self.assertEqual(self.obj.app_id, mocks.PROPERTIES.app_id)

    async def test_body_property(self):
        await self.run_consumer()
        self.assertEqual(self.obj.body, mocks.BODY)

    async def test_settings_property(self):
        await self.run_consumer()
        self.assertDictEqual(self.obj.settings, self.config)

    async def test_content_encoding_property(self):
        await self.run_consumer()
        self.assertEqual(
            self.obj.content_encoding, mocks.PROPERTIES.content_encoding
        )

    async def test_content_type_property(self):
        await self.run_consumer()
        self.assertEqual(self.obj.content_type, mocks.PROPERTIES.content_type)

    async def test_correlation_id_property(self):
        await self.run_consumer()
        self.assertEqual(
            self.obj.correlation_id, mocks.PROPERTIES.correlation_id
        )

    async def test_exchange_property(self):
        await self.run_consumer()
        self.assertEqual(self.obj.exchange, mocks.METHOD.exchange)

    async def test_expiration_property(self):
        await self.run_consumer()
        self.assertEqual(self.obj.expiration, mocks.PROPERTIES.expiration)

    async def test_headers_property(self):
        await self.run_consumer()
        self.assertDictEqual(self.obj.headers, mocks.PROPERTIES.headers)

    async def test_message_id_property(self):
        await self.run_consumer()
        self.assertEqual(self.obj.message_id, mocks.PROPERTIES.message_id)

    async def test_name_property(self):
        await self.run_consumer()
        self.assertEqual(self.obj.name, self.obj.__class__.__name__)

    async def test_priority_property(self):
        await self.run_consumer()
        self.assertEqual(self.obj.priority, mocks.PROPERTIES.priority)

    async def test_properties_property(self):
        await self.run_consumer()
        self.assertDictEqual(
            self.obj.properties, dict(data.Properties(mocks.PROPERTIES))
        )

    async def test_redelivered_property(self):
        await self.run_consumer()
        self.assertEqual(self.obj.redelivered, mocks.METHOD.redelivered)

    async def test_reply_to_property(self):
        await self.run_consumer()
        self.assertEqual(self.obj.reply_to, mocks.PROPERTIES.reply_to)

    async def test_routing_key_property(self):
        await self.run_consumer()
        self.assertEqual(self.obj.routing_key, mocks.METHOD.routing_key)

    async def test_message_type_property(self):
        await self.run_consumer()
        self.assertEqual(self.obj.message_type, mocks.PROPERTIES.type)

    async def test_timestamp_property(self):
        await self.run_consumer()
        self.assertEqual(self.obj.timestamp, mocks.PROPERTIES.timestamp)

    async def test_user_id_property(self):
        await self.run_consumer()
        self.assertEqual(self.obj.user_id, mocks.PROPERTIES.user_id)


class TestSmartConsumer(consumer.SmartConsumer):
    def process(self):
        pass


class TestSmartConsumerWithJSON(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.body = {'foo': 'bar', 'baz': 1, 'qux': True}
        self.message = data.Message(
            'mock',
            mocks.CHANNEL,
            mocks.METHOD,
            mocks.PROPERTIES,
            json.dumps(self.body),
            False,
        )
        self.measurement = data.Measurement()

    async def test_message_body_property(self):
        self.obj = TestSmartConsumer({}, None)
        await self.obj.execute(self.message, self.measurement)
        self.assertDictEqual(self.obj.body, self.body)
