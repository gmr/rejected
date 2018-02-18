import unittest
import uuid

from pika import spec

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


class MessageInfoTestCase(unittest.TestCase):
    def test_message_info_output(self):
        correlation_id = str(uuid.uuid4())
        message_id = str(uuid.uuid4())
        exchange = str(uuid.uuid4())
        routing_key = str(uuid.uuid4())
        expectation = ('{} [correlation_id="{}"] '
                       'published to "{}" using "{}"').format(
                           message_id, correlation_id, exchange, routing_key)

        properties = spec.BasicProperties(
            'application/json',
            correlation_id=correlation_id,
            message_id=message_id)

        self.assertEqual(
            utils.message_info(exchange, routing_key, properties), expectation)

    def test_message_info_output_no_correlation_id(self):
        message_id = str(uuid.uuid4())
        exchange = str(uuid.uuid4())
        routing_key = str(uuid.uuid4())
        expectation = '{} published to "{}" using "{}"'.format(
            message_id, exchange, routing_key)

        properties = spec.BasicProperties(
            'application/json', message_id=message_id)

        self.assertEqual(
            utils.message_info(exchange, routing_key, properties), expectation)

    def test_message_id_only(self):
        message_id = str(uuid.uuid4())

        properties = spec.BasicProperties(
            'application/json', message_id=message_id)

        self.assertEqual(utils.message_info('', '', properties), message_id)

    def test_no_identifiable_info(self):
        properties = spec.BasicProperties('application/json')
        self.assertEqual(utils.message_info('', '', properties), '')
