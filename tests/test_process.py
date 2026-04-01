"""Tests for rejected.process"""

import asyncio
import copy
import signal
import typing
import unittest
from unittest import mock

from rejected import (
    __version__,
    consumer,
    data,
    process,
)
from rejected import (
    config as config_module,
)

from . import mocks, test_state

# Raw config dict for building pydantic Config objects
_CONFIG_RAW: typing.ClassVar[dict] = {
    'stats': {
        'influxdb': {'enabled': False},
        'statsd': {'enabled': False},
    },
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
            'ssl_options': {
                'prototcol': 2,
            },
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


def _make_config(raw=None) -> config_module.Config:
    """Build a Config pydantic object from the raw dict."""
    return config_module.Config.model_validate(raw or _CONFIG_RAW)


class TestProcess(unittest.IsolatedAsyncioTestCase, test_state.TestState):
    config: typing.ClassVar[config_module.Config] = _make_config()

    mock_args: typing.ClassVar[dict] = {
        'config': config,
        'consumer_name': 'MockConsumer',
        'stats_queue': 'StatsQueue',
        'logging_config': {},
    }

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self._obj = self.new_process()

    async def asyncTearDown(self):
        del self._obj

    def new_kwargs(self, kwargs):
        return copy.copy(kwargs)

    def new_process(self, kwargs=None):
        with mock.patch('multiprocessing.Process'):
            return process.Process(
                group=None,
                name='MockProcess',
                kwargs=kwargs or self.new_kwargs(self.mock_args),
            )

    def test_app_id(self):
        expectation = f'rejected/{__version__}'
        self.assertEqual(self._obj.AMQP_APP_ID, expectation)

    def test_startup_state(self):
        new_process = self.new_process()
        self.assertEqual(new_process.state, process.Process.STATE_INITIALIZING)

    def test_startup_time(self):
        mock_time = 123456789.012345
        with mock.patch('time.time', return_value=mock_time):
            new_process = self.new_process()
            self.assertEqual(new_process.state_start, mock_time)

    def test_startup_consumer_is_none(self):
        new_process = self.new_process()
        self.assertIsNone(new_process.consumer)

    def test_get_config(self):
        conn = 'MockConnection'
        name = 'MockConsumer'
        number = 5
        pid = 1234
        cfg = _make_config()
        expectation = {
            'connection': cfg.connections[conn],
            'consumer_name': name,
            'process_name': f'{name}_{pid}_tag_{number}',
        }
        with mock.patch('os.getpid', return_value=pid):
            self.assertEqual(
                self._obj.get_config(cfg, number, name, conn),
                expectation,
            )

    def test_get_consumer_with_invalid_consumer(self):
        cfg = _make_config().consumers['MockConsumer2']
        self.assertIsNone(self._obj.get_consumer(cfg))

    def test_get_consumer_version_output(self):
        cfg = config_module.ConsumerConfig(consumer='tests.mocks.MockConsumer')
        with mock.patch('logging.Logger.info') as info:
            self._obj.get_consumer(cfg)
            info.assert_called_with(
                'Creating consumer %s v%s',
                cfg.consumer,
                mocks.__version__,
            )

    @mock.patch.object(consumer.Consumer, '__init__', side_effect=ImportError)
    def test_get_consumer_with_config_is_none(self, mock_method):
        cfg = config_module.ConsumerConfig(
            consumer='rejected.consumer.Consumer',
            config={'field': 'value', 'true': True},
        )
        new_process = self.new_process()
        new_process.get_consumer(cfg)
        self.assertIsNone(new_process.get_consumer(cfg))

    @mock.patch.object(consumer.Consumer, '__init__', side_effect=ImportError)
    def test_get_consumer_with_no_config_is_none(self, mock_method):
        cfg = config_module.ConsumerConfig(
            consumer='rejected.consumer.Consumer'
        )
        new_process = self.new_process()
        self.assertIsNone(new_process.get_consumer(cfg))

    def test_setup_signal_handlers(self):
        signals = [
            mock.call(signal.SIGPROF, self._obj.on_sigprof),
            mock.call(signal.SIGABRT, self._obj.stop),
        ]
        with mock.patch('signal.signal') as signal_signal:
            self._obj.setup_sighandlers()
            signal_signal.assert_has_calls(signals, any_order=True)

    def mock_setup(self, new_process=None, side_effect=None):
        with mock.patch('signal.signal', side_effect=side_effect):
            with mock.patch(
                'rejected.utils.import_consumer',
                return_value=(mock.Mock, None),
            ):
                if not new_process:
                    new_process = self.new_process(self.mock_args)
                    new_process.setup()

                new_process.measurement = mock.Mock()
                return new_process

    def test_setup_stats_queue(self):
        mock_process = self.mock_setup()
        self.assertEqual(
            mock_process.stats_queue, self.mock_args['stats_queue']
        )

    def test_setup_consumer_name(self):
        mock_process = self.mock_setup()
        self.assertEqual(
            mock_process.stats_queue, self.mock_args['stats_queue']
        )

    def test_setup_config(self):
        mock_process = self.mock_setup()
        expected = _make_config().consumers['MockConsumer']
        self.assertEqual(mock_process.consumer_config, expected)

    def test_setup_config_queue_name(self):
        mock_process = self.mock_setup()
        self.assertEqual(
            mock_process.queue_name,
            _CONFIG_RAW['Consumers']['MockConsumer']['queue'],
        )

    def test_setup_config_no_ack(self):
        mock_process = self.mock_setup()
        self.assertEqual(
            mock_process.no_ack,
            not _CONFIG_RAW['Consumers']['MockConsumer']['ack'],
        )

    def test_setup_max_error_count(self):
        mock_process = self.mock_setup()
        self.assertEqual(
            mock_process.max_error_count,
            _CONFIG_RAW['Consumers']['MockConsumer']['max_errors'],
        )

    def test_setup_prefetch_count_no_config(self):
        raw = copy.deepcopy(_CONFIG_RAW)
        del raw['Consumers']['MockConsumer']['qos_prefetch']
        cfg = _make_config(raw)
        args = {**self.mock_args, 'config': cfg}
        mock_process = self.new_process(args)
        mock_process.setup()
        self.assertEqual(
            mock_process.qos_prefetch, process.Process.QOS_PREFETCH_COUNT
        )

    def test_setup_prefetch_count_with_config(self):
        mock_process = self.mock_setup()
        self.assertEqual(
            mock_process.qos_prefetch,
            _CONFIG_RAW['Consumers']['MockConsumer']['qos_prefetch'],
        )

    def test_setup_with_ssl_connection(self):
        cfg = _make_config()
        args = {
            **self.mock_args,
            'config': cfg,
            'consumer_name': 'MockConsumer3',
        }
        new_process = self.new_process(args)
        with mock.patch('signal.signal'):
            with mock.patch(
                'rejected.utils.import_consumer',
                return_value=(mock.Mock, None),
            ):
                new_process.setup()
        new_process.measurement = mock.Mock()

        conn = new_process.connections['MockRemoteSSLConnection'].connection
        self.assertTrue(bool(conn.params.ssl_options))

    def test_setup_with_non_ssl_connection(self):
        cfg = _make_config()
        args = {
            **self.mock_args,
            'config': cfg,
            'consumer_name': 'MockConsumer2',
        }
        new_process = self.new_process(args)
        with mock.patch('signal.signal'):
            with mock.patch(
                'rejected.utils.import_consumer',
                return_value=(mock.Mock, None),
            ):
                new_process.setup()
        new_process.measurement = mock.Mock()

        conn = new_process.connections['MockRemoteConnection'].connection
        self.assertFalse(bool(conn.params.ssl_options))

    def test_is_idle_state_processing(self):
        self._obj.state = self._obj.STATE_PROCESSING
        self.assertFalse(self._obj.is_idle)

    def test_is_running_state_processing(self):
        self._obj.state = self._obj.STATE_PROCESSING
        self.assertTrue(self._obj.is_running)

    def test_is_shutting_down_state_processing(self):
        self._obj.state = self._obj.STATE_PROCESSING
        self.assertFalse(self._obj.is_shutting_down)

    def test_is_stopped_state_processing(self):
        self._obj.state = self._obj.STATE_PROCESSING
        self.assertFalse(self._obj.is_stopped)

    def test_state_processing_desc(self):
        self._obj.state = self._obj.STATE_PROCESSING
        self.assertEqual(
            self._obj.state_description,
            self._obj.STATES[self._obj.STATE_PROCESSING],
        )

    async def test_invoke_consumer_when_amqp_conn_is_connected(self):
        mock_process = self.mock_setup()
        mock_process.counters[mock_process.CLOSED_ON_COMPLETE] = 5

        # force unhandled exception in "execute"
        mock_process.consumer.execute.side_effect = Exception('blow up!')

        # mimic running process
        mock_process.consumer_lock = asyncio.Lock()
        mock_process.state = mock_process.STATE_IDLE

        # configure mock conn
        mock_conn = mock.Mock(spec=process.Connection)
        mock_process.connections[mock_conn] = mock_conn
        mock_conn.is_running = True

        mocks.CHANNEL.basic_nack = mock.Mock()
        message = data.Message(
            mock_conn,
            mocks.CHANNEL,
            mocks.METHOD,
            mocks.PROPERTIES,
            mocks.BODY,
            False,
        )

        await mock_process.invoke_consumer(message)

        self.assertEqual(mock_conn.shutdown.call_count, 0)
        self.assertEqual(mocks.CHANNEL.basic_nack.call_count, 1)
        self.assertEqual(
            mock_process.counters[mock_process.CLOSED_ON_COMPLETE], 5
        )

    async def test_invoke_consumer_when_amqp_conn_is_not_connected(self):
        mock_process = self.mock_setup()
        mock_process.counters[mock_process.CLOSED_ON_COMPLETE] = 5

        # force unhandled exception in "execute"
        mock_process.consumer.execute.side_effect = Exception('blow up!')

        # mimic running process
        mock_process.consumer_lock = asyncio.Lock()
        mock_process.state = mock_process.STATE_IDLE

        # configure mock conn
        mock_conn = mock.Mock(spec=process.Connection)
        mock_process.connections[mock_conn] = mock_conn
        mock_conn.is_running = False

        mocks.CHANNEL.basic_ack = mock.Mock()
        message = data.Message(
            mock_conn,
            mocks.CHANNEL,
            mocks.METHOD,
            mocks.PROPERTIES,
            mocks.BODY,
            False,
        )

        await mock_process.invoke_consumer(message)

        self.assertEqual(mock_conn.shutdown.call_count, 1)
        self.assertEqual(mocks.CHANNEL.basic_ack.call_count, 0)
        self.assertEqual(
            mock_process.counters[mock_process.CLOSED_ON_COMPLETE], 6
        )

    def test_ack_message_when_amqp_conn_is_connected(self):
        mock_process = self.mock_setup()
        mock_process.counters[mock_process.CLOSED_ON_COMPLETE] = 5

        # configure mock conn
        mock_conn = mock.Mock(spec=process.Connection)
        mock_process.connections[mock_conn] = mock_conn
        mock_conn.is_running = True

        mocks.CHANNEL.basic_ack = mock.Mock()
        message = data.Message(
            mock_conn,
            mocks.CHANNEL,
            mocks.METHOD,
            mocks.PROPERTIES,
            mocks.BODY,
            False,
        )

        mock_process.ack_message(message)

        self.assertEqual(mock_conn.shutdown.call_count, 0)
        self.assertEqual(mocks.CHANNEL.basic_ack.call_count, 1)
        self.assertEqual(
            mock_process.counters[mock_process.CLOSED_ON_COMPLETE], 5
        )

    def test_ack_message_when_amqp_conn_is_not_connected(self):
        mock_process = self.mock_setup()
        mock_process.counters[mock_process.CLOSED_ON_COMPLETE] = 5

        # configure mock conn
        mock_conn = mock.Mock(spec=process.Connection)
        mock_process.connections[mock_conn] = mock_conn
        mock_conn.is_running = False

        mocks.CHANNEL.basic_ack = mock.Mock()
        message = data.Message(
            mock_conn,
            mocks.CHANNEL,
            mocks.METHOD,
            mocks.PROPERTIES,
            mocks.BODY,
            False,
        )

        mock_process.ack_message(message)

        self.assertEqual(mock_conn.shutdown.call_count, 1)
        self.assertEqual(mocks.CHANNEL.basic_ack.call_count, 0)
        self.assertEqual(
            mock_process.counters[mock_process.CLOSED_ON_COMPLETE], 6
        )
