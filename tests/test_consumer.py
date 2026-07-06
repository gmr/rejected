"""Tests for rejected.consumer"""

import asyncio
import datetime
import typing
import unittest
from unittest import mock

from rejected import config as config_module
from rejected import connection as connection_mod
from rejected import consumer, exceptions, log, models


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


class ConsumerLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_finish_is_called(self):
        """#68 finish() runs after process() in the standard lifecycle."""
        calls: list[str] = []

        class LC(consumer.Consumer):
            async def prepare(self):
                calls.append('prepare')

            async def process(self):
                calls.append('process')

            async def finish(self):
                calls.append('finish')

        obj = LC(config_module.Settings({}), None)
        await obj.execute(_make_ctx())
        self.assertEqual(calls, ['prepare', 'process', 'finish'])


class RepublishPropertyTests(unittest.IsolatedAsyncioTestCase):
    async def test_processing_error_preserves_properties(self):
        """#69 republished messages keep the original AMQP properties."""

        class PC(consumer.Consumer):
            async def process(self):
                raise exceptions.ProcessingException('boom')

        proc = mock.Mock()
        proc.queue_name = 'q'
        proc.sentry_client = None
        obj = PC(config_module.Settings({}), proc)
        msg = _make_message()
        ctx = _make_ctx(msg)
        await obj.execute(ctx)
        self.assertEqual(ctx.result, models.Result.PROCESSING_EXCEPTION)
        props = ctx.channel.basic_publish.call_args.kwargs['properties']
        self.assertEqual(props.content_type, msg.content_type)
        self.assertEqual(props.correlation_id, msg.correlation_id)
        self.assertEqual(props.type, msg.type)
        self.assertEqual(props.priority, msg.priority)
        self.assertEqual(props.expiration, msg.expiration)


class ProcessingExceptionsHeaderTests(unittest.IsolatedAsyncioTestCase):
    async def test_non_numeric_header_does_not_raise(self):
        """#80 a non-numeric X-Processing-Exceptions header is tolerated."""

        class MR(consumer.Consumer):
            ERROR_MAX_RETRY = 3

            async def process(self):
                pass

        obj = MR(config_module.Settings({}), None)
        msg = _make_message(headers={'X-Processing-Exceptions': 'not-a-num'})
        ctx = _make_ctx(msg)
        await obj.execute(ctx)
        self.assertEqual(ctx.result, models.Result.MESSAGE_ACK)


class KeyboardInterruptTests(unittest.IsolatedAsyncioTestCase):
    async def test_keyboard_interrupt_does_not_reject(self):
        """#82 the KeyboardInterrupt handler no longer double-nacks."""

        class KC(consumer.Consumer):
            async def process(self):
                raise KeyboardInterrupt()

        proc = mock.Mock()
        proc.sentry_client = None
        obj = KC(config_module.Settings({}), proc)
        ctx = _make_ctx()
        await obj.execute(ctx)
        proc.reject.assert_not_called()
        proc.stop.assert_called_once()
        self.assertEqual(ctx.result, models.Result.MESSAGE_REQUEUE)


class CorrelationTests(unittest.IsolatedAsyncioTestCase):
    async def test_correlation_id_set_on_message(self):
        """#81 a missing correlation id is generated and carried on ctx."""

        class C(consumer.Consumer):
            async def process(self):
                pass

        obj = C(config_module.Settings({}), None)
        msg = _make_message(correlation_id=None, message_id=None)
        ctx = _make_ctx(msg)
        await obj.execute(ctx)
        self.assertIsNotNone(ctx.message.correlation_id)

    async def test_correlation_isolated_per_message(self):
        """#81 concurrent messages do not clobber each other's id."""
        seen: dict[str, str | None] = {}

        class C(consumer.FunctionalConsumer):
            async def process(self, ctx):
                await asyncio.sleep(0.01)
                seen[ctx.message.correlation_id] = log.correlation_id.get()

        obj = C(config_module.Settings({}), None)
        ctx1 = _make_ctx(_make_message(correlation_id='a'))
        ctx2 = _make_ctx(_make_message(correlation_id='b'))
        await asyncio.gather(obj.execute(ctx1), obj.execute(ctx2))
        self.assertEqual(seen['a'], 'a')
        self.assertEqual(seen['b'], 'b')

    async def test_initialize_called_once_concurrently(self):
        """#81 initialize() runs exactly once under concurrency."""
        calls: list[int] = []

        class C(consumer.FunctionalConsumer):
            async def initialize(self):
                calls.append(1)
                await asyncio.sleep(0.01)

            async def process(self, ctx):
                pass

        obj = C(config_module.Settings({}), None)
        await asyncio.gather(
            obj.execute(_make_ctx()), obj.execute(_make_ctx())
        )
        self.assertEqual(len(calls), 1)
