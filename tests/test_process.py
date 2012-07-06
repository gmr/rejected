"""Tests for rejected.process"""
from pika import channel
from pika import connection
from pika import credentials
from pika import exceptions
import mock
import multiprocessing
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from rejected import consumer
from rejected import process
from rejected import __version__


class TestImportNamspacedClass(unittest.TestCase):

    def test_import_namespaced_class(self):
        import logging
        (result_class,
         result_version) = process.import_namespaced_class('logging.Logger')
        self.assertEqual(result_class, logging.Logger)

    def test_import_namespaced_class_version(self):
        import logging
        (result_class,
         result_version) = process.import_namespaced_class('logging.Logger')
        self.assertEqual(result_version, logging.__version__)

    def test_import_namespaced_class_no_version(self):
        (result_class,
         result_version) = process.import_namespaced_class('signal.ItimerError')
        self.assertIsNone(result_version)

    def test_import_namespaced_class_failure(self):
        self.assertRaises(ImportError, process.import_namespaced_class,
                          'rejected.fake_module.Classname')


class TestProcess(unittest.TestCase):

    config = {'Connections':
                      {'MockConnection':
                               {'host': 'localhost',
                                'port': 5672,
                                'user': 'guest',
                                'pass': 'guest',
                                'vhost': '/'},
                       'MockRemoteConnection':
                               {'host': 'remotehost',
                                'port': 5672,
                                'user': 'guest',
                                'pass': 'guest',
                                'vhost': '/'}
                      },
              'Consumers':
                      {'MockConsumer':
                               {'consumer': 'mock_consumer.MockConsumer',
                                'connections': ['MockConnection'],
                                'config': {'test_value': True,
                                           'num_value': 100},
                                'min': 2,
                                'max': 5,
                                'queue': 'mock_queue'},
                       'MockConsumer2':
                               {'consumer': 'mock_consumer.MockConsumerTwo',
                                'connections': ['MockConnection',
                                                'MockRemoteConnection'],
                                'config': {'num_value': 50},
                                'min': 1,
                                'max': 2,
                                'queue': 'mock_you'}}}
    mock_args= {'config': config,
                'connection_name': 'MockConnection',
                'consumer_name': 'MockConsumer',
                'stats_queue': mock.Mock(spec=multiprocessing.Queue)}

    def setUp(self):
        self._process = self.new_process()

    def tearDown(self):
        del self._process

    def new_counter(self):
        return {process.Process.ERROR: 0,
                process.Process.PROCESSED: 0,
                process.Process.REDELIVERED: 0,
                process.Process.TIME_SPENT: 0,
                process.Process.TIME_WAITED: 0}

    def new_process(self):
        with mock.patch('multiprocessing.Process'):
            return process.Process(group=None,
                                   name='MockProcess',
                                   kwargs=self.mock_args)

    def new_mock_channel(self):
        return mock.Mock(spec=channel.Channel)

    def new_mock_connection(self):
        return mock.Mock(spec=connection.Connection)

    def new_mock_parameters(self, host, port, vhost, user, password):
        mock_credentials = mock.Mock(spec=credentials.PlainCredentials)
        mock_credentials.username = user
        mock_credentials.password = password
        mock_parameters = mock.Mock(spec=connection.ConnectionParameters)
        mock_parameters.credentials = mock_credentials
        mock_parameters.host = host
        mock_parameters.port = port
        mock_parameters.virtual_host = vhost
        return mock_parameters

    def test_app_id(self):
        expectation = 'rejected/%s' % __version__
        self.assertEqual(self._process._AMQP_APP_ID, expectation)

    def test_counter_data_structure(self):
        self.assertEqual(self._process._new_counter_dict(), self.new_counter())

    def test_startup_state(self):
        new_process = self.new_process()
        self.assertEqual(new_process._state, process.Process.STATE_INITIALIZING)

    def test_startup_time(self):
        mock_time = 123456789.012345
        with mock.patch('time.time', return_value=mock_time):
            new_process = self.new_process()
            self.assertEqual(new_process._state_start, mock_time)

    def test_startup_counts(self):
        new_process = self.new_process()
        self.assertEqual(new_process._counts, self.new_counter())

    def test_startup_channel_is_none(self):
        new_process = self.new_process()
        self.assertIsNone(new_process._channel)

    def test_startup_consumer_is_none(self):
        new_process = self.new_process()
        self.assertIsNone(new_process._consumer)

    def test_ack_message(self):
        delivery_tag = 'MockDeliveryTag-1'
        mock_ack = mock.Mock()
        self._process._channel = self.new_mock_channel()
        self._process._channel.basic_ack = mock_ack
        self._process._ack_message(delivery_tag)
        mock_ack.assert_called_once_with(delivery_tag=delivery_tag)

    def test_count(self):
        expectation = 10
        self._process._counts[process.Process.REDELIVERED] = expectation
        self.assertEqual(self._process._count(process.Process.REDELIVERED),
                                              expectation)

    def test_get_config(self):
        connection = 'MockConnection'
        name = 'MockConsumer'
        number = 5
        pid = 1234
        expectation = {'connection': self.config['Connections'][connection],
                       'connection_name': connection,
                       'consumer_name': name,
                       'name': '%s_%i_tag_%i' % (name, pid, number)}
        with mock.patch('os.getpid', return_value=pid):
            self.assertEqual(self._process._get_config(self.config, number,
                                                       name, connection),
                             expectation)

    def get_connection_parameters(self, host, port, vhost, user, password):
        with mock.patch('pika.credentials.PlainCredentials'):
            with mock.patch('pika.connection.ConnectionParameters'):
                return self._process._get_connection_parameters(host,
                                                                port,
                                                                vhost,
                                                                user,
                                                                password)

    def default_connection_parameters(self):
        return {'host': 'rabbitmq',
                'port': 5672,
                'user': 'nose',
                'password': 'mock',
                'vhost': 'unittest'}

    def test_get_connection_parameters_host(self):
        kwargs = self.default_connection_parameters()
        result = self.get_connection_parameters(**kwargs)
        self.assertEqual(result.host, kwargs['host'])

    def test_get_connection_parameters_port(self):
        kwargs = self.default_connection_parameters()
        result = self.get_connection_parameters(**kwargs)
        self.assertEqual(result.port, kwargs['port'])

    def test_get_connection_parameters_vhost(self):
        kwargs = self.default_connection_parameters()
        result = self.get_connection_parameters(**kwargs)
        self.assertEqual(result.virtual_host, kwargs['vhost'])

    def test_get_connection_parameters_user(self):
        kwargs = self.default_connection_parameters()
        result = self.get_connection_parameters(**kwargs)
        self.assertEqual(result.credentials.username, kwargs['user'])

    def test_get_connection_parameters_password(self):
        kwargs = self.default_connection_parameters()
        result = self.get_connection_parameters(**kwargs)
        self.assertEqual(result.credentials.password, kwargs['password'])

    def test_get_connection_called_get_parameters(self):
        config = self.config['Connections']['MockConnection']
        kwargs = {'host': config['host'],
                  'port': config['port'],
                  'user': config['user'],
                  'password': config['pass'],
                  'vhost': config['vhost']}
        parameters = self.get_connection_parameters(**kwargs)
        with mock.patch.object(process.Process, '_get_connection_parameters',
                               return_value=parameters) as mock_method:
            with mock.patch('pika.adapters.tornado_connection.TornadoConnection'):
                new_process = self.new_process()
                new_process._get_connection(self.config['Connections'],
                                            'MockConnection')
                mock_method.assert_called_once_with(config['host'],
                                                    config['port'],
                                                    config['vhost'],
                                                    config['user'],
                                                    config['pass'])

    def test_get_connection_with_exception_raised(self):
        config = self.config['Connections']['MockConnection']
        kwargs = {'host': config['host'],
                  'port': config['port'],
                  'user': config['user'],
                  'password': config['pass'],
                  'vhost': config['vhost']}
        parameters = self.get_connection_parameters(**kwargs)
        with mock.patch.object(process.Process, '_get_connection_parameters',
                               return_value=parameters):
            with mock.patch('pika.adapters.tornado_connection.TornadoConnection',
                            side_effect=exceptions.AMQPConnectionError):
                new_process = self.new_process()
                new_process._get_connection(self.config['Connections'],
                                            'MockConnection')
                self.assertEqual(new_process._state,
                                 process.Process.STATE_STOPPED)

    def test_get_consumer_raises_import_error(self):
        self.assertRaises(ImportError, self._process._get_consumer,
                          self.config['Consumers']['MockConsumer'])

    def test_get_consumer_version_output(self):
        config = {'consumer': 'optparse.OptionParser'}
        with mock.patch('logging.Logger.info') as info:
            import optparse
            self._process._get_consumer(config)
            info.assert_called_with('Creating processor %s v%s',
                                    config['consumer'],
                                    optparse.__version__)

    def test_get_consumer_no_version_output(self):
        config = {'consumer': 'StringIO.StringIO'}
        with mock.patch('logging.Logger.info') as info:
            self._process._get_consumer(config)
            info.assert_called_with('Creating processor %s', config['consumer'])

    @mock.patch.object(consumer.Consumer, '__init__')
    def test_get_consumer_with_config(self, mock_method):
        config = {'consumer': 'rejected.consumer.Consumer',
                  'config': {'field': 'value', 'true': True}}
        self._process._get_consumer(config)
        mock_method.assert_called_once_with(config['config'])

    @mock.patch.object(consumer.Consumer, '__init__', side_effect=ImportError)
    def test_get_consumer_with_config_is_none(self, mock_method):
        config = {'consumer': 'rejected.consumer.Consumer',
                  'config': {'field': 'value', 'true': True}}
        self.assertIsNone(self._process._get_consumer(config))

    @mock.patch.object(consumer.Consumer, '__init__')
    def test_get_consumer_with_no_config(self, mock_method):
        config = {'consumer': 'rejected.consumer.Consumer'}
        self._process._get_consumer(config)
        mock_method.assert_called_once_with()

    @mock.patch.object(consumer.Consumer, '__init__', side_effect=ImportError)
    def test_get_consumer_with_no_config_is_none(self, mock_method):
        config = {'consumer': 'rejected.consumer.Consumer'}
        self.assertIsNone(self._process._get_consumer(config))

    def test__process(self):
        mock_consumer = mock.Mock(consumer.Consumer)
        new_process = self.new_process()
        new_process._processor = mock_consumer
        message = 'Hello World'
        new_process._process(message)
        mock_consumer.process.assert_called_once_with(message)

    def test__process_consumer_exception(self):
        mock_consumer = mock.Mock(consumer.Consumer)
        mock_consumer.process.side_effect = consumer.ConsumerException
        new_process = self.new_process()
        new_process._processor = mock_consumer
        message = 'Hello World'
        self.assertRaises(consumer.ConsumerException, new_process._process,
                          message)

    def test__process_other_exception(self):
        mock_consumer = mock.Mock(consumer.Consumer)
        mock_consumer.process.side_effect = AttributeError
        new_process = self.new_process()
        new_process._processor = mock_consumer
        message = 'Hello World'
        self.assertRaises(consumer.ConsumerException, new_process._process,
                          message)

    def test_reject_message(self):
        delivery_tag = 'MockDeliveryTag-1'
        mock_reject = mock.Mock()
        self._process._channel = self.new_mock_channel()
        self._process._channel.basic_reject = mock_reject
        self._process._reject(delivery_tag)
        mock_reject.assert_called_once_with(delivery_tag=delivery_tag)
