"""Tests for rejected.process"""

import copy
import datetime
import signal
import typing
import unittest
from unittest import mock

from rejected import __version__, consumer, models, process
from rejected import config as config_module
from rejected import connection as connection_mod

from . import mocks, test_state

# Raw config dict for building pydantic Config objects
_CONFIG_RAW: typing.ClassVar[dict] = {
    'stats': {'statsd': {'enabled': False}},
    'Connections': {
        'MockConnection': {
            'host': 'localhost',
            'port': 5672,
            'user': 'guest',
            'pass': 'guest',
            'vhost': '/',
        },
        'MockRemoteConnection': {
            'host': 'remotehost',
            'port': 5672,
            'user': 'guest',
            'pass': 'guest',
            'vhost': '/',
        },
        'MockRemoteSSLConnection': {
            'host': 'remotehost',
            'port': 5672,
            'user': 'guest',
            'pass': 'guest',
            'vhost': '/',
            'ssl_options': {'protocol': 2},
        },
    },
    'Consumers': {
        'MockConsumer': {
            'consumer': 'tests.mocks.MockConsumer',
            'connections': ['MockConnection'],
            'config': {'test_value': True, 'num_value': 100},
            'max_errors': 10,
            'qos_prefetch': 5,
            'ack': True,
            'queue': 'mock_queue',
        },
        'MockConsumer2': {
            'consumer': 'mock_consumer.MockConsumer',
            'connections': ['MockConnection', 'MockRemoteConnection'],
            'config': {'num_value': 50},
            'queue': 'mock_you',
        },
        'MockConsumer3': {
            'consumer': 'mock_consumer.MockConsumer',
            'connections': ['MockRemoteSSLConnection'],
            'config': {'num_value': 50},
            'queue': 'mock_you2',
        },
    },
}


def _make_config(
    raw: dict[str, typing.Any] | None = None,
) -> config_module.Config:
    return config_module.Config.model_validate(raw or _CONFIG_RAW)


def _make_message(
    delivery_tag: int | None = 1,
    redelivered: bool = False,
    returned: bool = False,
) -> models.Message:
    return models.Message(
        delivery_tag=delivery_tag,
        exchange='exchange',
        routing_key='routing_key',
        body='{"qux": true}',
        app_id='bar',
        content_encoding=None,
        content_type='application/json',
        correlation_id='c123',
        delivery_mode=2,
        expiration='32768',
        headers={'foo': 'bar'},
        message_id='mid123',
        message_type='test',
        priority=5,
        redelivered=redelivered,
        reply_to='rtrk',
        returned=returned,
        timestamp=datetime.datetime.now(tz=datetime.UTC),
        user_id='foo',
    )


def _make_ctx(
    conn: typing.Any = None,
    channel: typing.Any = None,
    message: models.Message | None = None,
) -> models.ProcessingContext:
    return models.ProcessingContext(
        connection=conn or mock.Mock(spec=connection_mod.Connection),
        channel=channel or mocks.CHANNEL,
        message=message or _make_message(),
    )


class TestProcess(unittest.IsolatedAsyncioTestCase, test_state.TestState):
    config: typing.ClassVar[config_module.Config] = _make_config()

    mock_args: typing.ClassVar[dict[str, typing.Any]] = {
        'config': config,
        'consumer_name': 'MockConsumer',
        'stats_queue': 'StatsQueue',
        'logging_config': {},
    }

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self._obj = self.new_process()

    async def asyncTearDown(self) -> None:
        del self._obj

    def new_kwargs(
        self, kwargs: dict[str, typing.Any]
    ) -> dict[str, typing.Any]:
        return copy.copy(kwargs)

    def new_process(
        self, kwargs: dict[str, typing.Any] | None = None
    ) -> process.Process:
        with mock.patch('multiprocessing.Process'):
            return process.Process(
                group=None,
                name='MockProcess',
                kwargs=kwargs or self.new_kwargs(self.mock_args),
            )

    def test_app_id(self) -> None:
        self.assertEqual(self._obj.AMQP_APP_ID, f'rejected/{__version__}')

    def test_startup_state(self) -> None:
        p = self.new_process()
        self.assertEqual(p.state, process.Process.STATE_INITIALIZING)

    def test_startup_time(self) -> None:
        mock_time = 123456789.012345
        with mock.patch('time.time', return_value=mock_time):
            p = self.new_process()
            self.assertEqual(p.state_start, mock_time)

    def test_startup_consumer_is_none(self) -> None:
        self.assertIsNone(self.new_process().consumer)

    def test_get_consumer_with_invalid_consumer(self) -> None:
        cfg = _make_config().consumers['MockConsumer2']
        self.assertIsNone(self._obj.get_consumer(cfg))

    def test_get_consumer_version_output(self) -> None:
        cfg = config_module.ConsumerConfig(consumer='tests.mocks.MockConsumer')
        with mock.patch('logging.Logger.info') as info:
            self._obj.get_consumer(cfg)
            info.assert_called_with(
                'Creating consumer %s v%s', cfg.consumer, mocks.__version__
            )

    @mock.patch.object(consumer.Consumer, '__init__', side_effect=ImportError)
    def test_get_consumer_with_config_is_none(self, _mock: mock.Mock) -> None:
        cfg = config_module.ConsumerConfig(
            consumer='rejected.consumer.Consumer',
            config={'field': 'value', 'true': True},
        )
        self.assertIsNone(self.new_process().get_consumer(cfg))

    @mock.patch.object(consumer.Consumer, '__init__', side_effect=ImportError)
    def test_get_consumer_with_no_config_is_none(
        self, _mock: mock.Mock
    ) -> None:
        cfg = config_module.ConsumerConfig(
            consumer='rejected.consumer.Consumer'
        )
        self.assertIsNone(self.new_process().get_consumer(cfg))

    def test_setup_signal_handlers(self) -> None:
        signals = [
            mock.call(signal.SIGPROF, self._obj.on_sigprof),
            mock.call(signal.SIGABRT, self._obj.stop),
        ]
        with mock.patch('signal.signal') as signal_signal:
            self._obj.setup_sighandlers()
            signal_signal.assert_has_calls(signals, any_order=True)

    def mock_setup(
        self,
        new_process: process.Process | None = None,
        side_effect: typing.Any = None,
    ) -> process.Process:
        with mock.patch('signal.signal', side_effect=side_effect):
            with mock.patch(
                'rejected.utils.import_consumer',
                return_value=(mock.Mock, None),
            ):
                if not new_process:
                    new_process = self.new_process(self.mock_args)
                    new_process.setup()
                return new_process

    def test_setup_stats_queue(self) -> None:
        p = self.mock_setup()
        self.assertEqual(p.stats_queue, self.mock_args['stats_queue'])

    def test_setup_consumer_name(self) -> None:
        p = self.mock_setup()
        self.assertEqual(p.consumer_name, self.mock_args['consumer_name'])

    def test_setup_config(self) -> None:
        p = self.mock_setup()
        expected = _make_config().consumers['MockConsumer']
        self.assertEqual(p.consumer_config, expected)

    def test_setup_config_queue_name(self) -> None:
        p = self.mock_setup()
        self.assertEqual(
            p.queue_name, _CONFIG_RAW['Consumers']['MockConsumer']['queue']
        )

    def test_setup_config_no_ack(self) -> None:
        p = self.mock_setup()
        self.assertEqual(
            p.no_ack, not _CONFIG_RAW['Consumers']['MockConsumer']['ack']
        )

    def test_setup_max_error_count(self) -> None:
        p = self.mock_setup()
        self.assertEqual(
            p.max_error_count,
            _CONFIG_RAW['Consumers']['MockConsumer']['max_errors'],
        )

    def test_setup_prefetch_count_no_config(self) -> None:
        raw = copy.deepcopy(_CONFIG_RAW)
        del raw['Consumers']['MockConsumer']['qos_prefetch']
        cfg = _make_config(raw)
        args = {**self.mock_args, 'config': cfg}
        p = self.new_process(args)
        p.setup()
        self.assertEqual(p.qos_prefetch, process.Process.QOS_PREFETCH_COUNT)

    def test_setup_prefetch_count_with_config(self) -> None:
        p = self.mock_setup()
        self.assertEqual(
            p.qos_prefetch,
            _CONFIG_RAW['Consumers']['MockConsumer']['qos_prefetch'],
        )

    def test_is_idle_state_processing(self) -> None:
        self._obj.state = self._obj.STATE_PROCESSING
        self.assertFalse(self._obj.is_idle)

    def test_is_running_state_processing(self) -> None:
        self._obj.state = self._obj.STATE_PROCESSING
        self.assertTrue(self._obj.is_running)

    def test_is_shutting_down_state_processing(self) -> None:
        self._obj.state = self._obj.STATE_PROCESSING
        self.assertFalse(self._obj.is_shutting_down)

    def test_is_stopped_state_processing(self) -> None:
        self._obj.state = self._obj.STATE_PROCESSING
        self.assertFalse(self._obj.is_stopped)

    def test_state_processing_desc(self) -> None:
        self._obj.state = self._obj.STATE_PROCESSING
        self.assertEqual(
            self._obj.state_description,
            self._obj.STATES[self._obj.STATE_PROCESSING],
        )

    async def test_invoke_consumer_requeues_on_exception(self) -> None:
        p = self.mock_setup()
        p.consumer.execute.side_effect = Exception('blow up!')
        p.state = p.STATE_IDLE

        mock_conn = mock.Mock(spec=connection_mod.Connection)
        mock_conn.is_running = True
        p.connections[mock_conn] = mock_conn

        ctx = _make_ctx(conn=mock_conn)
        mocks.CHANNEL.basic_nack = mock.Mock()

        await p.invoke_consumer(ctx)

        mocks.CHANNEL.basic_nack.assert_called_once()

    async def test_invoke_consumer_shutdown_requeues(self) -> None:
        p = self.mock_setup()
        p.state = p.STATE_SHUTTING_DOWN

        mock_conn = mock.Mock(spec=connection_mod.Connection)
        mock_conn.is_running = True
        p.connections[mock_conn] = mock_conn

        ctx = _make_ctx(conn=mock_conn)
        mocks.CHANNEL.basic_nack = mock.Mock()

        await p.invoke_consumer(ctx)

        mocks.CHANNEL.basic_nack.assert_called_once()

    def test_ack_message_when_connected(self) -> None:
        p = self.mock_setup()
        mock_conn = mock.Mock(spec=connection_mod.Connection)
        mock_conn.is_running = True
        p.connections[mock_conn] = mock_conn

        mocks.CHANNEL.basic_ack = mock.Mock()
        ctx = _make_ctx(conn=mock_conn)

        p.ack_message(ctx)

        mocks.CHANNEL.basic_ack.assert_called_once()
        mock_conn.shutdown.assert_not_called()

    def test_ack_message_when_disconnected(self) -> None:
        p = self.mock_setup()
        mock_conn = mock.Mock(spec=connection_mod.Connection)
        mock_conn.is_running = False
        p.connections[mock_conn] = mock_conn

        mocks.CHANNEL.basic_ack = mock.Mock()
        ctx = _make_ctx(conn=mock_conn)

        p.ack_message(ctx)

        mocks.CHANNEL.basic_ack.assert_not_called()
        mock_conn.shutdown.assert_called_once()
        self.assertTrue(ctx.measurement.tags.get(p.CLOSED_ON_COMPLETE))

    def test_reject_message_when_connected(self) -> None:
        p = self.mock_setup()
        mock_conn = mock.Mock(spec=connection_mod.Connection)
        mock_conn.is_running = True
        p.connections[mock_conn] = mock_conn

        mocks.CHANNEL.basic_nack = mock.Mock()
        ctx = _make_ctx(conn=mock_conn)

        p.reject(ctx, requeue=True)

        mocks.CHANNEL.basic_nack.assert_called_once()
        self.assertTrue(ctx.measurement.tags.get(p.NACKED))
        self.assertTrue(ctx.measurement.tags.get(p.REQUEUED))

    def test_reject_message_when_disconnected(self) -> None:
        p = self.mock_setup()
        mock_conn = mock.Mock(spec=connection_mod.Connection)
        mock_conn.is_running = False
        p.connections[mock_conn] = mock_conn

        mocks.CHANNEL.basic_nack = mock.Mock()
        ctx = _make_ctx(conn=mock_conn)

        p.reject(ctx, requeue=True)

        mocks.CHANNEL.basic_nack.assert_not_called()
        mock_conn.shutdown.assert_called_once()
