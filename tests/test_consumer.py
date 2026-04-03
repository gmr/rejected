"""Tests for rejected.consumer"""

import datetime
import typing
import unittest
from unittest import mock

from rejected import config as config_module
from rejected import connection as connection_mod
from rejected import consumer, exceptions, models


def _make_message(**kwargs: typing.Any) -> models.Message:
    defaults: dict[str, typing.Any] = {
        'delivery_tag': 1,
        'exchange': 'exchange',
        'routing_key': 'routing_key',
        'body': '{"foo": "bar"}',
        'app_id': 'bar',
        'content_encoding': None,
        'content_type': 'application/json',
        'correlation_id': 'c123',
        'delivery_mode': 2,
        'expiration': '32768',
        'headers': {'foo': 'bar'},
        'message_id': 'mid123',
        'type': 'test',
        'priority': 5,
        'redelivered': False,
        'reply_to': 'rtrk',
        'returned': False,
        'timestamp': datetime.datetime.now(tz=datetime.UTC),
        'user_id': 'foo',
    }
    defaults.update(kwargs)
    return models.Message(**defaults)


def _make_ctx(
    message: models.Message | None = None,
) -> models.ProcessingContext:
    mock_conn = mock.Mock(spec=connection_mod.Connection)
    mock_conn.is_running = True
    return models.ProcessingContext(
        connection=mock_conn,
        channel=mock.Mock(),
        message=message or _make_message(),
    )


class ConsumerInitializationTests(unittest.TestCase):
    def test_configuration_is_assigned(self):
        cfg = config_module.Settings({'foo': 'bar'})
        obj = consumer.Consumer(cfg, None)
        self.assertEqual(obj._settings, cfg)

    def test_initialized_flag_is_false(self):
        obj = consumer.Consumer(config_module.Settings({}), None)
        self.assertFalse(obj._initialized)

    def test_context_is_none(self):
        obj = consumer.Consumer(config_module.Settings({}), None)
        self.assertIsNone(obj._context)


class ConsumerDefaultProcessTests(unittest.IsolatedAsyncioTestCase):
    async def test_process_raises_exception(self):
        obj = consumer.Consumer(config_module.Settings({}), None)
        with self.assertRaises(NotImplementedError):
            await obj.process()


class ConsumerSetChannelTests(unittest.TestCase):
    def test_set_channel_assigns_to_channel(self):
        obj = consumer.Consumer(config_module.Settings({}), None)
        ch = mock.Mock()
        obj.set_channel('mock', ch)
        self.assertEqual(obj._channels['mock'], ch)


class TestConsumer(consumer.Consumer):
    async def process(self):
        pass


class ConsumerExecuteTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.obj = TestConsumer(config_module.Settings({}), None)

    async def test_execute_calls_process(self):
        ctx = _make_ctx()
        with mock.patch.object(
            self.obj, 'process', new_callable=mock.AsyncMock
        ) as process_mock:
            await self.obj.execute(ctx)
            process_mock.assert_called_once()
        self.assertEqual(ctx.result, models.Result.MESSAGE_ACK)

    async def test_execute_sets_context(self):
        """During processing, self._context is set."""
        captured = {}

        async def capture_process():
            captured['ctx'] = self.obj._context

        self.obj.process = capture_process
        ctx = _make_ctx()
        await self.obj.execute(ctx)
        self.assertIs(captured['ctx'], ctx)

    async def test_execute_clears_context_after(self):
        ctx = _make_ctx()
        await self.obj.execute(ctx)
        self.assertIsNone(self.obj._context)

    async def test_execute_drops_invalid_message_type(self):
        class TypedConsumer(consumer.Consumer):
            MESSAGE_TYPE = 'expected'
            DROP_INVALID_MESSAGES = True

            async def process(self):
                pass

        obj = TypedConsumer(config_module.Settings({}), None)
        ctx = _make_ctx(_make_message(type='wrong'))
        await obj.execute(ctx)
        self.assertEqual(ctx.result, models.Result.MESSAGE_DROP)

    async def test_execute_rejects_invalid_message_type(self):
        class TypedConsumer(consumer.Consumer):
            MESSAGE_TYPE = 'expected'

            async def process(self):
                pass

        obj = TypedConsumer(config_module.Settings({}), None)
        ctx = _make_ctx(_make_message(type='wrong'))
        await obj.execute(ctx)
        self.assertEqual(ctx.result, models.Result.MESSAGE_EXCEPTION)

    async def test_execute_sets_result_on_ctx(self):
        ctx = _make_ctx()
        await self.obj.execute(ctx)
        self.assertEqual(ctx.result, models.Result.MESSAGE_ACK)

    async def test_consumer_exception_result(self):
        async def raise_consumer_exc():
            raise exceptions.ConsumerException('boom')

        self.obj.process = raise_consumer_exc
        ctx = _make_ctx()
        await self.obj.execute(ctx)
        self.assertEqual(ctx.result, models.Result.CONSUMER_EXCEPTION)

    async def test_unhandled_exception_result(self):
        async def raise_value_error():
            raise ValueError('boom')

        self.obj.process = raise_value_error
        ctx = _make_ctx()
        await self.obj.execute(ctx)
        self.assertEqual(ctx.result, models.Result.UNHANDLED_EXCEPTION)


class ConsumerPropertyTests(unittest.IsolatedAsyncioTestCase):
    """Test that properties are accessible DURING processing."""

    async def test_properties_during_processing(self):
        """All message properties are accessible via self.* during
        process()."""
        msg = _make_message()
        captured: dict[str, typing.Any] = {}

        class PropConsumer(consumer.Consumer):
            async def process(self):
                captured['body'] = self.body
                captured['app_id'] = self.app_id
                captured['content_encoding'] = self.content_encoding
                captured['content_type'] = self.content_type
                captured['correlation_id'] = self.correlation_id
                captured['exchange'] = self.exchange
                captured['expiration'] = self.expiration
                captured['headers'] = self.headers
                captured['message_id'] = self.message_id
                captured['message_type'] = self.message_type
                captured['name'] = self.name
                captured['priority'] = self.priority
                captured['redelivered'] = self.redelivered
                captured['reply_to'] = self.reply_to
                captured['returned'] = self.returned
                captured['routing_key'] = self.routing_key
                captured['timestamp'] = self.timestamp
                captured['user_id'] = self.user_id

        obj = PropConsumer(config_module.Settings({'foo': 'bar'}), None)
        ctx = _make_ctx(msg)
        await obj.execute(ctx)

        self.assertEqual(captured['body'], msg.body)
        self.assertEqual(captured['app_id'], msg.app_id)
        self.assertEqual(captured['content_encoding'], msg.content_encoding)
        self.assertEqual(captured['content_type'], msg.content_type)
        self.assertEqual(captured['correlation_id'], msg.correlation_id)
        self.assertEqual(captured['exchange'], msg.exchange)
        self.assertEqual(captured['expiration'], msg.expiration)
        self.assertEqual(captured['headers'], msg.headers)
        self.assertEqual(captured['message_id'], msg.message_id)
        self.assertEqual(captured['message_type'], msg.type)
        self.assertEqual(captured['name'], 'PropConsumer')
        self.assertEqual(captured['priority'], msg.priority)
        self.assertEqual(captured['redelivered'], msg.redelivered)
        self.assertEqual(captured['reply_to'], msg.reply_to)
        self.assertEqual(captured['returned'], msg.returned)
        self.assertEqual(captured['routing_key'], msg.routing_key)
        self.assertEqual(captured['timestamp'], msg.timestamp)
        self.assertEqual(captured['user_id'], msg.user_id)

    async def test_properties_none_after_execute(self):
        """Properties return None after execute completes."""
        obj = TestConsumer(config_module.Settings({}), None)
        ctx = _make_ctx()
        await obj.execute(ctx)
        self.assertIsNone(obj.app_id)
        self.assertIsNone(obj.body)

    async def test_settings_accessible(self):
        obj = TestConsumer(config_module.Settings({'foo': 'bar'}), None)
        self.assertEqual(obj.settings.get('foo'), 'bar')
