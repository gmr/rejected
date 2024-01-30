import pathlib
import tomllib
import unittest
from unittest import mock

import yaml

from rejected import config


class TestCase(unittest.TestCase):

    def test_load_configuration(self):
        for extension in ['json', 'toml', 'yaml']:
            path = pathlib.Path(f'tests/data/good_config.{extension}')
            with path.open('rb') as handle:
                if extension == 'toml':
                    expectation = tomllib.load(handle)
                else:
                    expectation = yaml.safe_load(handle)
            expectation['application']['connections']['rabbitmq'][
                'password'] = None
            configuration = config.load_configuration(str(path))
            configuration.application.connections['rabbitmq'].password = None
            self.assertDictEqual(
                configuration.application.connections['rabbitmq'].dict(),
                expectation['application']['connections']['rabbitmq'])

    @mock.patch('sys.stderr.write')
    @mock.patch('sys.exit')
    def test_load_configuration_error(self, sys_exit, write):
        for extension in ['json', 'toml', 'yaml']:
            path = pathlib.Path(f'tests/data/bad_config.{extension}')
            config.load_configuration(str(path))
        sys_exit.assert_has_calls([mock.call(3), mock.call(3)])
        self.assertTrue(write.called)

    @mock.patch('sys.stderr.write')
    @mock.patch('sys.exit')
    def test_load_configuration_file_not_found(self, sys_exit, write):
        config.load_configuration('foo.json')
        sys_exit.assert_called_once_with(2)
        self.assertTrue(write.called)

    def test_read_configuration(self):
        for extension in ['json', 'toml', 'yaml']:
            path = pathlib.Path(f'tests/data/good_config.{extension}')
            with path.open('rb') as handle:
                if extension == 'toml':
                    expectation = tomllib.load(handle)
                else:
                    expectation = yaml.safe_load(handle)
            with config.read_configuration(str(path)) as result:
                self.assertDictEqual(result, expectation)

    @mock.patch('sys.exit')
    def test_read_configuration_invalid_extension(self, sys_exit):
        with self.assertRaises(RuntimeError):
            with config.read_configuration('tests/__init__.py'):
                pass

    @mock.patch('sys.stderr.write')
    @mock.patch('sys.exit')
    def test_load_invalid_config_file(self, sys_exit, write):
        for extension in ['json', 'toml', 'yaml']:
            path = pathlib.Path(f'tests/data/invalid.{extension}')
            config.load_configuration(str(path))
        sys_exit.assert_has_calls([mock.call(2), mock.call(2), mock.call(2)])
        self.assertTrue(write.called)
