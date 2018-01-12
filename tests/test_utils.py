import unittest
import uuid

from rejected import utils


class TestImportNamspacedClass(unittest.TestCase):

    def test_import_consumer(self):
        import logging
        (result_class,
         result_version) = utils.import_consumer('logging.Logger')
        self.assertEqual(result_class, logging.Logger)

    def test_import_consumer_version(self):
        import logging
        (result_class,
         result_version) = utils.import_consumer('logging.Logger')
        self.assertEqual(result_version, logging.__version__)

    def test_import_consumer_no_version(self):
        (result_class,
         result_version) = utils.import_consumer('signal.ItimerError')
        self.assertIsNone(result_version)

    def test_import_consumer_failure(self):
        self.assertRaises(ImportError, utils.import_consumer,
                          'rejected.fake_module.Classname')


class Decorated(object):

    def __init__(self):
        self._message = {}

    @utils.message_property
    def foo(self):
        return self._message['foo']

    def _get_bar(self):
        return self._message.get('bar')

    bar = utils.message_property(getter=_get_bar)


class MessagePropertyDecoratorTestCase(unittest.TestCase):

    def test_empty_foo(self):
        obj = Decorated()
        self.assertIsNone(obj.foo)

    def test_set_foo(self):
        expectation = str(uuid.uuid4())
        obj = Decorated()
        obj._message['foo'] = expectation
        self.assertEqual(obj.foo, expectation)

    def test_foo_assignment(self):
        obj = Decorated()
        with self.assertRaises(AttributeError):
            obj.foo = 'bar'

    def test_empty_bar(self):
        obj = Decorated()
        self.assertIsNone(obj.bar)

    def test_set_bar(self):
        expectation = str(uuid.uuid4())
        obj = Decorated()
        obj._message['bar'] = expectation
        self.assertEqual(obj.bar, expectation)

    def test_bar_assignment(self):
        obj = Decorated()
        with self.assertRaises(AttributeError):
            obj.bar = 'foo'
