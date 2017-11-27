import logging
from os import path
import unittest
import uuid

import mock

from rejected import consumer, log, testing


class FilterTestCase(unittest.TestCase):

    def setUp(self):
        self.log_record = logging.LogRecord(
            'test', logging.INFO, path.realpath(__file__), 13,
            'Example logged message', [], None, 'setUp')


class CorrelationIDFilterTestCase(FilterTestCase):

    def setUp(self):
        super(CorrelationIDFilterTestCase, self).setUp()
        self.log_filter = log.CorrelationIDFilter()

    def test_that_filter_passes(self):
        setattr(self.log_record, 'correlation_id', str(uuid.uuid4()))
        self.assertEqual(self.log_filter.filter(self.log_record), 1)

    def test_that_filter_does_not_pass(self):
        self.assertEqual(self.log_filter.filter(self.log_record), 0)


class NoCorrelationIDFilterTestCase(FilterTestCase):

    def setUp(self):
        super(NoCorrelationIDFilterTestCase, self).setUp()
        self.log_filter = log.NoCorrelationIDFilter()

    def test_that_filter_passes(self):
        self.assertEqual(self.log_filter.filter(self.log_record), 1)

    def test_that_filter_does_not_pass(self):
        setattr(self.log_record, 'correlation_id', str(uuid.uuid4()))
        self.assertEqual(self.log_filter.filter(self.log_record), 0)


class CorrelationIDAdapterTestCase(testing.AsyncTestCase):

    def test_that_correlation_adapter_is_assigned(self):
        self.assertIsInstance(self.consumer.logger, log.CorrelationIDAdapter)

    def test_correlation_adapter_process(self):
        self.consumer._correlation_id = str(uuid.uuid4())
        msg, kwargs = self.consumer.logger.process('Test', {})
        self.assertEqual(msg, 'Test')
        self.assertDictEqual(
            kwargs['extra'],
            {'parent': self.consumer.name,
             'correlation_id': self.consumer.correlation_id})


# Tests for deprecated classes


class CorrelationAdapterTestCase(unittest.TestCase):

    def test_that_warnings_warn_is_invoked(self):
        with mock.patch('warnings.warn') as warn:
            log.CorrelationAdapter(
                logging.getLogger(__name__),
                {'parent': mock.Mock(spec=consumer.Consumer)})
            warn.assert_called_once()


class CorrelationFilterDeprecationTestCase(unittest.TestCase):

    def test_that_warnings_warn_is_invoked(self):
        with mock.patch('warnings.warn') as warn:
            log.CorrelationFilter()
            warn.assert_called_once()


class CorrelationFilterExistsTestCase(FilterTestCase):

    def setUp(self):
        super(CorrelationFilterExistsTestCase, self).setUp()
        self.log_filter = log.CorrelationFilter(exists=True)

    def test_that_filter_passes(self):
        setattr(self.log_record, 'correlation_id', str(uuid.uuid4()))
        self.assertEqual(self.log_filter.filter(self.log_record), 1)

    def test_that_filter_does_not_pass(self):
        self.assertEqual(self.log_filter.filter(self.log_record), 0)


class CorrelationFilterExistsFalseTestCase(FilterTestCase):

    def setUp(self):
        super(CorrelationFilterExistsFalseTestCase, self).setUp()
        self.log_filter = log.CorrelationFilter(exists=False)

    def test_that_filter_passes(self):
        self.assertEqual(self.log_filter.filter(self.log_record), 1)

    def test_that_filter_does_not_pass(self):
        setattr(self.log_record, 'correlation_id', str(uuid.uuid4()))
        self.assertEqual(self.log_filter.filter(self.log_record), 0)
