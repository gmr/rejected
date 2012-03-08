"""
Test For rejected.cli

"""
__author__ = 'Gavin M. Roy'
__email__ = 'gmr@myyearbook.com'
__since__ = '2011-03-07'

import daemon
import grp
import logging_config
import mock
import optparse
import pwd
import signal
import sys

try:     # Python 2.7 and previous support
    import unittest2 as unittest
except ImportError:
    import unittest

# Append the parent directory
sys.path.insert(0, '..')

# Import the cli module
from rejected import cli
from rejected import mcp

_BAD_YAML = "%yaml443.\n====HAHA: {4400"
_TEST_YAML = """\
%YAML 1.2
---
user: nobody
group: wheel
Logging:
    loggers:
      rejected:
        level: DEBUG
        propagate: True
      pika:
        level: INFO
        propagate: True
    filters:
    formatters:
      verbose: "%(levelname) -10s %(asctime)s %(name) -30s %(funcName) -25s: %(message)s"
    handlers:
      console:
        class: logging.StreamHandler
        formatter: verbose
        debug_only: True
        level: DEBUG

Monitor:
    enabled: False
    interval: 30

Connections:
    localhost:
        host: localhost
        port: 5672
        user: guest
        pass: guest
        ssl: False
        vhost: /
        monitor_port: 55672

Consumers:
    Test:
        import: rejected.consumer
        processor: Consumer
        connections: [localhost]
        queue: test_queue
        max_errors: 5
        no_ack: False
        requeue_on_error: False
"""

_TEST_CONFIG = {'user': 'nobody', 'group': 'wheel',
                'Connections': {'localhost': {'monitor_port': 55672,
                                              'vhost': '/',
                                              'host': 'localhost',
                                              'user': 'guest',
                                              'ssl': False,
                                              'pass': 'guest',
                                              'port': 5672}},
                'Logging': {'loggers': {'pika': {'propagate': True,
                                                 'level': 'INFO'},
                                        'rejected': {'propagate': True,
                                                     'level': 'DEBUG'}},
                            'formatters': {'verbose': '%(levelname) -10s '
                                                      '%(asctime)s %(name) '
                                                      '-30s %(funcName) -25s: '
                                                      '%(message)s'},
                            'filters': None,
                            'handlers': {'console': {'formatter': 'verbose',
                                                     'debug_only': True,
                                                     'class': 'logging.StreamHa'
                                                              'ndler',
                                                     'level': 'DEBUG'}}},
                'Monitor': {'interval': 30, 'enabled': False},
                'Consumers': {'Test': {'queue': 'test_queue',
                                       'connections': ['localhost'],
                                       'max_errors': 5,
                                       'processor': 'Consumer',
                                       'import': 'rejected.consumer',
                                       'no_ack': False,
                                       'requeue_on_error': False}}}


class MyTestCase(unittest.TestCase):

    def setUp(self):
        self._object = TestCLI()

    def tearDown(self):
        del self._object

    def test_parse_yaml(self):
        self.assertEqual(self._object._parse_yaml(_TEST_YAML), _TEST_CONFIG)

    def test_parse_yaml_error(self):
        self.assertEqual(0, len(self._object._parse_yaml(_BAD_YAML)))

    def test_load_configuration(self):
        self.assertEqual(self._object._load_configuration(), _TEST_CONFIG)

    def test_load_configuration_failure(self):
        def read_file_bad(filename):
            return _BAD_YAML
        self._object._read_file = read_file_bad
        self.assertRaises(RuntimeError, self._object._load_configuration)
        def read_file_none(filename):
            return None
        self._object._read_file = read_file_none
        self.assertRaises(RuntimeError, self._object._load_configuration)

    def test_logging_config(self):
        self.assertIsInstance(self._object._logging_config(),
                              logging_config.Logging)

    def test_option_parser(self):
        self.assertIsInstance(cli.CLI._option_parser(self._object),
                              optparse.OptionParser)

    def test_validate_config(self):
        filename = '/dev/null'
        self._object._parser = optparse.OptionParser()
        self._object._add_parser_options()
        options, args = self._object._parser.parse_args(['--config', filename])
        self.assertEqual(options.config, filename)

    def test_validate_config_error(self):

        # Test an empty file
        self.assertRaises(optparse.OptionValueError,
                          self._object._validate_config,
                          optparse.Option('--test'),
                          '--test',
                          None,
                          self._object._parser)

        # Test for a non-existent file
        self.assertRaises(optparse.OptionValueError,
                          self._object._validate_config,
                          optparse.Option('--test'),
                          '--test',
                          '/rejected-tests/ack!/pirates/zomg',
                          self._object._parser)

    def test_change_group(self):
        expectation = 100
        name = 'foobar'
        self._object._group_id = mock.Mock(return_value=expectation)
        self._object._context = mock.Mock()
        self._object._change_group(name)
        self.assertEqual(self._object._context.gid, expectation)
        self._object._group_id.assert_called_with(name)

    def test_change_user(self):
        expectation = 100
        name = 'foobar'
        self._object._user_id = mock.Mock(return_value=expectation)
        self._object._context = mock.Mock()
        self._object._change_user(name)
        self.assertEqual(self._object._context.uid, expectation)
        self._object._user_id.assert_called_with(name)

    def test_daemon_context(self):
        self.assertIsInstance(self._object._daemon_context(),
                              daemon.DaemonContext)

    def test_group_id(self):
        name = 'wheel'
        expectation = grp.getgrnam(name).gr_gid
        self.assertEqual(self._object._group_id(name), expectation)

    def test_user_id(self):
        name = 'nobody'
        expectation = pwd.getpwnam(name).pw_uid
        self.assertEqual(self._object._user_id(name), expectation)

    def test_setup_signalmap(self):
        self._object._context = mock.Mock()
        self._object._setup_signal_map()
        self.assertEqual(self._object._context.signal_map,
                         self._object._signal_map)

    def test_setup_signals(self):
        self._object._setup_signals()
        for signal_num in self._object._signal_map:
            self.assertEqual(signal.getsignal(signal_num),
                             self._object._signal_map[signal_num])

    def test_logging_set(self):
        self._object._logging = mock.Mock()
        self._object._setup_logging()
        self.assertTrue(self._object._logging.setup.called)

    def test_master_control_program(self):
        self.assertIsInstance(self._object._master_control_program(),
                              mcp.MasterControlProgram)

    def test_master_control_program_run_and_cli__run(self):
        mcp = mock.Mock()
        self._object._master_control_program = mock.Mock(return_value=mcp)
        self._object._run()
        self.assertTrue(mcp.run.called)

    def test_daemon_run(self):
        mcp = mock.Mock()
        self._object._master_control_program = mock.Mock(return_value=mcp)
        context = mock.MagicMock()
        self._object._daemon_context = mock.Mock(return_value=context)

        # Invoke the method we are testing
        self._object._run_daemon()

        # Assert the context map was setup
        self.assertEqual(context.signal_map, self._object._signal_map)

        # Assert the group was changed
        expectation = grp.getgrnam(_TEST_CONFIG['group']).gr_gid
        self.assertEqual(context.gid, expectation)

        # Assert the user was changed
        expectation = pwd.getpwnam(_TEST_CONFIG['user']).pw_uid
        self.assertEqual(context.uid, expectation)

        # Assert MasterControlProgram.run was called
        self.assertTrue(mcp.run.called)

        # Assert __exit__ was called
        self.assertTrue(context.__exit__.called)

    def _setup_test_run_stubs(self):

        _run_daemon = mock.Mock()
        self._object._run_daemon = _run_daemon

        _run = mock.Mock()
        self._object._run = _run

        _setup_signals = mock.Mock()
        self._object._setup_signals = _setup_signals

        return _run_daemon, _run, _setup_signals

    def _test_run(self):
        # Re-process the arguments
        self._object._config = self._object._load_configuration()

        # The main run function
        self._object.run()

    def test_run(self):
        # Get mocks for checking if methods were run
        _run_daemon, _run, _setup_signals = self._setup_test_run_stubs()

        # Test that prepend is processed if set
        prepend = mock.Mock()
        self._object._prepend_python_path = prepend
        path_expectation = '/foo'
        setattr(self._object._options, 'prepend_path', path_expectation)

        self._test_run()

        # Make sure the prepend was called
        prepend.assert_called_with(path_expectation)

        # Make sure we called run daemon
        self.assertTrue(_run_daemon.called)
        self.assertFalse(_setup_signals.called)
        self.assertFalse(_run.called)

        # Now run again with foreground = True
        setattr(self._object._options, 'foreground', True)

        # Reset the mock objects with new ones
        _run_daemon, _run, _setup_signals = self._setup_test_run_stubs()

        # Re-process the arguments
        self._object._config = self._object._load_configuration()

        # The main run function
        self._object.run()

        # Make sure we called run daemon
        self.assertFalse(_run_daemon.called)
        self.assertTrue(_setup_signals.called)
        self.assertTrue(_run.called)

    def test_stop(self):
        self._object._mcp = mock.Mock()
        self._object.stop()
        self.assertTrue(self._object._mcp.shutdown.called)

    def test_main(self):
        setattr(self._object._options, 'foreground', True)
        cli._get_cli = mock.Mock(return_value=self._object)
        mcp = mock.Mock()
        self._object._master_control_program = mock.Mock(return_value=mcp)
        self._object._change_group = mock.Mock()
        self._object._change_user = mock.Mock()
        cli.main()
        self.assertTrue(cli._get_cli.called)

        # Test that keyboard interrupt calls CLI.stop()
        self._object.run = mock.Mock(side_effect=KeyboardInterrupt)
        stop_obj = mock.Mock()
        self._object.stop = stop_obj
        cli.main()
        self.assertTrue(stop_obj.called)

        # Test that Runtime error does not call stop
        self._object.run = mock.Mock(side_effect=RuntimeError)
        stop_obj = mock.Mock()
        _error = mock.Mock()
        cli._error = _error
        self._object.stop = stop_obj
        cli.main()
        self.assertFalse(stop_obj.called)
        self.assertTrue(_error.called)


class TestCLI(cli.CLI):

    def _option_parser(self):
        self._options = optparse.Values()
        setattr(self._options, 'config', '/dev/null')
        setattr(self._options, 'foreground', False)
        setattr(self._options, 'prepend_path', None)
        parser = mock.Mock()
        parser.parse_args.return_value=(self._options, list())
        return parser

    def _read_file(self, filename):
        return _TEST_YAML


if __name__ == '__main__':
    unittest.main()
