"""Tests for rejected.process"""
import copy
import mock
from mock import patch
try:
    import unittest2 as unittest
except ImportError:
    import unittest

import pika
from pika import channel
from pika import connection
from pika import credentials
import signal

from helper import config as helper_config

from rejected import consumer
from rejected import process
from rejected import __version__

from . import test_state
from . import mocks



class TestProcess(test_state.TestState):

    config = {
        'stats': {
            'influxdb': {
                'enabled': False
            },
            'statsd': {
                'enabled': False
            }
        },
        'Connections': {
            'MockConnection': {
                'host': 'localhost',
                'port': 5672,
                'user': 'guest',
                'pass': 'guest',
                'vhost': '/'
            },
            'MockRemoteConnection': {
                'host': 'remotehost',
                'port': 5672,
                'user': 'guest',
                'pass': 'guest',
                'vhost': '/'
            }
        },
        'Consumers': {
            'MockConsumer': {
                'consumer': 'tests.mocks.MockConsumer',
                'connections': ['MockConnection'],
                'config': {'test_value': True,
                           'num_value': 100},
                'min': 2,
                'max': 5,
                'max_errors': 10,
                'qos_prefetch': 5,
                'ack': True,
                'queue': 'mock_queue'
            },
            'MockConsumer2': {
                'consumer': 'mock_consumer.MockConsumer',
                'connections': ['MockConnection', 'MockRemoteConnection'],
                'config': {'num_value': 50},
                'min': 1,
                'max': 2,
                'queue': 'mock_you'
            }
        }
    }
    logging_config = helper_config.LoggingConfig(helper_config.Config.LOGGING)

    mock_args = {
        'config': config,
        'consumer_name': 'MockConsumer',
        'stats_queue': 'StatsQueue'
    }

    def setUp(self):
        self._obj = self.new_process()

    def tearDown(self):
        del self._obj

    def new_kwargs(self, kwargs):
        return copy.deepcopy(kwargs)

    def new_process(self, kwargs=None):
        with patch('multiprocessing.Process'):
            return process.Process(
                group=None,
                name='MockProcess',
                kwargs=kwargs or self.new_kwargs(self.mock_args))

    def test_app_id(self):
        expectation = 'rejected/%s' % __version__
        self.assertEqual(self._obj.AMQP_APP_ID, expectation)

    def test_startup_state(self):
        new_process = self.new_process()
        self.assertEqual(new_process.state, process.Process.STATE_INITIALIZING)

    def test_startup_time(self):
        mock_time = 123456789.012345
        with patch('time.time', return_value=mock_time):
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
        expectation = {
            'connection': self.config['Connections'][conn],
            'consumer_name': name,
            'process_name': '%s_%i_tag_%i' % (name, pid, number)
        }
        with patch('os.getpid', return_value=pid):
            self.assertEqual(self._obj.get_config(self.config, number, name,
                                                  conn), expectation)

    def test_get_consumer_with_invalid_consumer(self):
        cfg = self.config['Consumers']['MockConsumer2']
        self.assertIsNone(self._obj.get_consumer(cfg))

    def test_get_consumer_version_output(self):
        config = {'consumer': 'tests.mocks.MockConsumer'}
        with patch('logging.Logger.info') as info:
            self._obj.get_consumer(config)
            info.assert_called_with('Creating consumer %s v%s',
                                    config['consumer'],
                                    mocks.__version__)

    def test_get_consumer_no_version_output(self):
        config = {'consumer': 'rejected.consumer.Consumer'}
        with patch('logging.Logger.info') as info:
            self._obj.get_consumer(config)
            info.assert_called_with('Creating consumer %s v%s',
                                    config['consumer'], __version__)

    @patch.object(consumer.Consumer, '__init__', side_effect=ImportError)
    def test_get_consumer_with_config_is_none(self, mock_method):
        config = {
            'consumer': 'rejected.consumer.Consumer',
            'config': {'field': 'value',
                       'true': True}
        }
        new_process = self.new_process()
        new_process.get_consumer(config)
        self.assertIsNone(new_process.get_consumer(config))

    @patch.object(consumer.Consumer, '__init__', side_effect=ImportError)
    def test_get_consumer_with_no_config_is_none(self, mock_method):
        config = {'consumer': 'rejected.consumer.Consumer'}
        new_process = self.new_process()
        self.assertIsNone(new_process.get_consumer(config))

    def test_setup_signal_handlers(self):
        signals = [mock.call(signal.SIGPROF, self._obj.on_sigprof),
                   mock.call(signal.SIGABRT, self._obj.stop)]
        with patch('signal.signal') as signal_signal:
            self._obj.setup_sighandlers()
            signal_signal.assert_has_calls(signals, any_order=True)

    def mock_setup(self, new_process=None, side_effect=None):
        with patch('signal.signal', side_effect=side_effect):
            with patch('rejected.utils.import_consumer',
                       return_value=(mock.Mock, None)):
                if not new_process:
                    new_process = self.new_process(self.mock_args)
                    new_process.setup()
                return new_process

    def test_setup_stats_queue(self):
        mock_process = self.mock_setup()
        self.assertEqual(mock_process.stats_queue,
                         self.mock_args['stats_queue'])

    def test_setup_consumer_name(self):
        mock_process = self.mock_setup()
        self.assertEqual(mock_process.stats_queue,
                         self.mock_args['stats_queue'])

    def test_setup_config(self):
        mock_process = self.mock_setup()
        config = self.config['Consumers']['MockConsumer']
        self.assertEqual(mock_process.consumer_config, config)

    def test_setup_config_queue_name(self):
        mock_process = self.mock_setup()
        self.assertEqual(mock_process.queue_name,
                         self.config['Consumers']['MockConsumer']['queue'])

    def test_setup_config_no_ack(self):
        mock_process = self.mock_setup()
        self.assertEqual(mock_process.no_ack,
                         not self.config['Consumers']['MockConsumer']['ack'])

    def test_setup_max_error_count(self):
        mock_process = self.mock_setup()
        self.assertEqual(mock_process.max_error_count,
                         self.config['Consumers']['MockConsumer']['max_errors'])

    def test_setup_prefetch_count_no_config(self):
        args = copy.deepcopy(self.mock_args)
        del args['config']['Consumers']['MockConsumer']['qos_prefetch']
        mock_process = self.new_process(args)
        mock_process.setup()
        self.assertEqual(mock_process.qos_prefetch,
                         process.Process.QOS_PREFETCH_COUNT)

    def test_setup_prefetch_count_with_config(self):
        mock_process = self.mock_setup()
        self.assertEqual(
            mock_process.qos_prefetch,
            self.config['Consumers']['MockConsumer']['qos_prefetch'])

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
        self.assertEqual(self._obj.state_description,
                         self._obj.STATES[self._obj.STATE_PROCESSING])
