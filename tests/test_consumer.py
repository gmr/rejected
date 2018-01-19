# coding=utf-8
"""Tests for rejected.consumer"""
import logging
import random
import time
import unittest
import uuid

from tornado import gen
import mock

from rejected import consumer, connection, data, errors, testing

from . import common, mocks

LOGGER = logging.getLogger(__name__)


class ConsumerInitializationTests(unittest.TestCase):

    def test_configuration_is_assigned(self):
        cfg = {'foo': 'bar'}
        obj = consumer.Consumer(settings=cfg, process=None)
        self.assertDictEqual(obj._settings, cfg)

    def test_channel_is_none(self):
        obj = consumer.Consumer(settings={}, process=None)
        self.assertIsNone(obj._channel)

    def test_message_is_none(self):
        obj = consumer.Consumer(settings={}, process=None)
        self.assertIsNone(obj._message)

    @staticmethod
    def test_initialize_is_invoked():
        with mock.patch('rejected.consumer.Consumer.initialize') as init:
            consumer.Consumer(settings={}, process=None)
            init.assert_called_once_with()


class ConsumerDefaultProcessTests(testing.AsyncTestCase):

    def get_consumer(self):
        return consumer.Consumer

    @testing.gen_test
    def test_process_raises_exception_in_testing(self):
        with self.assertRaises(NotImplementedError):
            yield self.consumer.process()

    @testing.gen_test
    def test_process_raises_exception_in_testing(self):
        with self.assertRaises(AssertionError):
            yield self.process_message(mocks.BODY, 'application/json')


class ConsumerSetConnectionTests(unittest.TestCase):

    def test_set_channel_assigns_to_channel(self):
        obj = consumer.Consumer(settings={}, process=None)
        conn = mock.Mock(spec=connection.Connection)
        conn.name = 'mock'
        obj.set_connection(conn)
        self.assertEqual(obj._connections['mock'], conn)


class ConsumerLifecycleTests(testing.AsyncTestCase):

    def get_consumer(self):
        return common.TestConsumer

    @testing.gen_test
    def test_contract_met(self):
        result = yield self.process_message(str(uuid.uuid4()))
        self.assertIsInstance(result, data.Measurement)
        self.assertEqual(result, self.measurement)
        self.assertTrue(self.consumer.called_initialize)
        self.assertTrue(self.consumer.called_prepare)
        self.assertTrue(self.consumer.called_process)
        self.assertTrue(self.consumer.called_on_finish)

    @testing.gen_test
    def test_consumer_body_passthrough(self):
        body = str(uuid.uuid4())
        yield self.process_message(body)
        self.assertEqual(self.consumer.body, body)

    @testing.gen_test
    def test_double_finish_logs(self):
        yield self.process_message(str(uuid.uuid4()))
        with mock.patch.object(self.consumer.logger, 'warning') as warning:
            self.consumer.finish()
            warning.assert_called_once()


class TestFinishedConsumer(consumer.Consumer):

    def __init__(self, *args, **kwargs):
        self.called_prepare = False
        self.called_process = False
        self.called_on_finish = False
        super(TestFinishedConsumer, self).__init__(*args, **kwargs)

    @gen.coroutine
    def prepare(self):
        self.called_prepare = True
        self.finish()
        return super(TestFinishedConsumer, self).prepare()

    def process(self):
        self.called_process = True

    def on_finish(self):
        self.called_on_finish = True


class FinishedInPrepareTestCase(testing.AsyncTestCase):

    def get_consumer(self):
        return TestFinishedConsumer

    @testing.gen_test
    def test_contract_met(self):
        result = yield self.process_message(str(uuid.uuid4()))
        self.assertIsInstance(result, data.Measurement)
        self.assertEqual(result, self.measurement)
        self.assertTrue(self.consumer.called_prepare)
        self.assertFalse(self.consumer.called_process)
        self.assertTrue(self.consumer.called_on_finish)


class TestOnFinishConsumer(consumer.Consumer):

    def __init__(self, *args, **kwargs):
        self.called_on_finish = False
        self.raise_in_prepare = None
        self.raise_in_process = None
        super(TestOnFinishConsumer, self).__init__(*args, **kwargs)

    def prepare(self):
        if self.raise_in_prepare:
            raise self.raise_in_prepare
        return super(TestOnFinishConsumer, self).prepare()

    def process(self):
        if self.raise_in_process:
            raise self.raise_in_process
        pass

    def on_finish(self):
        self.called_on_finish = True


class OnFinishAfterExceptionTests(testing.AsyncTestCase):

    def get_consumer(self):
        return TestOnFinishConsumer

    @testing.gen_test
    def test_on_finished_called_case_01(self):
        self.consumer.raise_in_prepare = consumer.ConsumerException
        with self.assertRaises(consumer.ConsumerException):
            yield self.process_message(str(uuid.uuid4()))
        self.assertTrue(self.consumer.called_on_finish)

    @testing.gen_test
    def test_on_finished_called_case_02(self):
        self.consumer.raise_in_prepare = consumer.ConfigurationException
        with self.assertRaises(consumer.ConfigurationException):
            yield self.process_message(str(uuid.uuid4()))
        self.assertTrue(self.consumer.called_on_finish)

    @testing.gen_test
    def test_on_finished_called_case_03(self):
        self.consumer.raise_in_prepare = consumer.MessageException
        with self.assertRaises(consumer.MessageException):
            yield self.process_message(str(uuid.uuid4()))
        self.assertTrue(self.consumer.called_on_finish)

    @testing.gen_test
    def test_on_finished_called_case_04(self):
        self.consumer.raise_in_prepare = consumer.ProcessingException
        with self.assertRaises(consumer.ProcessingException):
            yield self.process_message(str(uuid.uuid4()))
        self.assertTrue(self.consumer.called_on_finish)

    @testing.gen_test
    def test_on_finished_called_case_05(self):
        self.consumer.raise_in_prepare = ValueError
        with self.assertRaises(AssertionError):
            yield self.process_message(str(uuid.uuid4()))
        self.assertTrue(self.consumer.called_on_finish)

    @testing.gen_test
    def test_on_finished_called_case_06(self):
        self.consumer.raise_in_process = consumer.ConsumerException
        with self.assertRaises(consumer.ConsumerException):
            yield self.process_message(str(uuid.uuid4()))
        self.assertTrue(self.consumer.called_on_finish)

    @testing.gen_test
    def test_on_finished_called_case_07(self):
        self.consumer.raise_in_process = consumer.ConfigurationException
        with self.assertRaises(consumer.ConfigurationException):
            yield self.process_message(str(uuid.uuid4()))
        self.assertTrue(self.consumer.called_on_finish)

    @testing.gen_test
    def test_on_finished_called_case_08(self):
        self.consumer.raise_in_process = consumer.MessageException
        with self.assertRaises(consumer.MessageException):
            yield self.process_message(str(uuid.uuid4()))
        self.assertTrue(self.consumer.called_on_finish)

    @testing.gen_test
    def test_on_finished_called_case_09(self):
        self.consumer.raise_in_process = consumer.ProcessingException
        with self.assertRaises(consumer.ProcessingException):
            yield self.process_message(str(uuid.uuid4()))
        self.assertTrue(self.consumer.called_on_finish)

    @testing.gen_test
    def test_on_finished_called_case_10(self):
        self.consumer.raise_in_process = ValueError
        with self.assertRaises(AssertionError):
            yield self.process_message(str(uuid.uuid4()))
        self.assertTrue(self.consumer.called_on_finish)

    @testing.gen_test
    def test_on_finished_called_case_11(self):
        self.consumer.raise_in_process = errors.RabbitMQException(
            self.process.connections['mock'], 999, 'test-message')
        with self.assertRaises(errors.RabbitMQException):
            yield self.process_message(str(uuid.uuid4()))
        self.assertTrue(self.consumer.called_on_finish)


class TestPublisher(consumer.Consumer):

    errors = []

    @gen.coroutine
    def process(self):
        if self.reply_to or self.message_type == 'reply_to_test':
            properties = self.headers.get('properties', {})
            self.rpc_reply(self.settings['body'], properties)
        else:
            self.publish_message(self.settings['exchange'],
                                 self.settings['routing_key'],
                                 {'content_type': 'text/plain'},
                                 self.settings['body'])


class ConsumerPublishingTests(testing.AsyncTestCase):

    def get_settings(self):
        return {
            'exchange': str(uuid.uuid4()),
            'routing_key': str(uuid.uuid4()),
            'body': str(uuid.uuid4())
        }

    def get_consumer(self):
        return TestPublisher

    @testing.gen_test
    def test_publish_message(self):
        yield self.process_message(self.consumer.settings['body'],
                                   'text/plain')
        self.assertEqual(self.published_messages[0].exchange,
                         self.consumer.settings['exchange'])
        self.assertEqual(self.published_messages[0].routing_key,
                         self.consumer.settings['routing_key'])
        self.assertEqual(self.published_messages[0].body,
                         self.consumer.settings['body'])

    @testing.gen_test
    def test_rpc_reply(self):
        properties = {
            'content_type': 'text/plain',
            'message_id': str(uuid.uuid4()),
            'reply_to': str(uuid.uuid4())
        }

        yield self.process_message(self.consumer.settings['body'],
                                   properties=properties,
                                   exchange=self.consumer.settings['exchange'])

        self.assertEqual(self.published_messages[0].properties.correlation_id,
                         properties['message_id'])
        self.assertEqual(self.published_messages[0].exchange,
                         self.consumer.settings['exchange'])
        self.assertEqual(self.published_messages[0].routing_key,
                         properties['reply_to'])
        self.assertEqual(self.published_messages[0].body,
                         self.consumer.settings['body'])

    @testing.gen_test
    def test_rpc_reply_with_properties(self):
        properties = {
            'content_type': 'text/plain',
            'message_id': str(uuid.uuid4()),
            'headers': {
                'properties': {
                    'app_id': 'test-app',
                    'message_id': str(uuid.uuid4()),
                    'correlation_id': str(uuid.uuid4()),
                    'timestamp': 10
                }
            },
            'reply_to': str(uuid.uuid4())
        }

        yield self.process_message(self.consumer.settings['body'],
                                   properties=properties,
                                   exchange=self.consumer.settings['exchange'])

        self.assertEqual(self.published_messages[0].exchange,
                         self.consumer.settings['exchange'])
        self.assertEqual(self.published_messages[0].routing_key,
                         properties['reply_to'])
        self.assertEqual(self.published_messages[0].body,
                         self.consumer.settings['body'])

        self.assertEqual(self.published_messages[0].properties.app_id,
                         properties['headers']['properties']['app_id'])
        self.assertEqual(self.published_messages[0].properties.correlation_id,
                         properties['headers']['properties']['correlation_id'])
        self.assertEqual(self.published_messages[0].properties.message_id,
                         properties['headers']['properties']['message_id'])
        self.assertEqual(self.published_messages[0].properties.timestamp,
                         properties['headers']['properties']['timestamp'])

    @testing.gen_test
    def test_rpc_reply_no_reply_to(self):
        with self.assertRaises(AssertionError):
            yield self.process_message(
                self.consumer.settings['body'],
                properties={
                    'content_type': 'text/plain',
                    'message_id': str(uuid.uuid4()),
                    'type': 'reply_to_test'
                },
                exchange=self.consumer.settings['exchange'])


class TestConfirmingPublisher(consumer.Consumer):

    def initialize(self):
        self.confirmations = []

    @gen.coroutine
    def process(self):
        for iteration in range(0, 3):
            self.logger.info('Publishing message %i', iteration)
            confirmation = yield self.publish_message(
                self.settings['exchange'],
                self.settings['routing_key'],
                self.properties,
                self.settings['body'])
            self.confirmations.append(confirmation)
        self.logger.info('Confirmations: %r', self.confirmations)


class ConfirmingPublishingTests(testing.AsyncTestCase):

    PUBLISHER_CONFIRMATIONS = True

    def get_settings(self):
        return {
            'exchange': str(uuid.uuid4()),
            'routing_key': str(uuid.uuid4()),
            'body': str(uuid.uuid4())
        }

    def get_consumer(self):
        return TestConfirmingPublisher

    @testing.gen_test
    def test_confirmation_success_case(self):
        yield self.process_message(self.consumer.settings['body'],
                                   'text/plain')
        self.assertEqual(self.published_messages[0].exchange,
                         self.consumer.settings['exchange'])
        self.assertEqual(self.published_messages[0].routing_key,
                         self.consumer.settings['routing_key'])
        self.assertEqual(self.published_messages[0].body,
                         self.consumer.settings['body'])
        self.assertTrue(all(self.consumer.confirmations))

    @testing.gen_test
    def test_confirmation_undelivered_case(self):
        def raise_undelivered(*args, **kwargs):
            raise testing.UndeliveredMessage()

        with self.publishing_side_effect(raise_undelivered):
            yield self.process_message(self.consumer.settings['body'],
                                       'text/plain')
        self.assertFalse(all(self.consumer.confirmations))

    @testing.gen_test
    def test_confirmation_unroutable_case(self):
        def raise_unroutable(*args, **kwargs):
            raise testing.UnroutableMessage()

        with self.publishing_side_effect(raise_unroutable):
            yield self.process_message(self.consumer.settings['body'],
                                       'text/plain')
        self.assertFalse(all(self.consumer.confirmations))

    @testing.gen_test
    def test_confirmation_branch_case(self):
        def raise_undelivered(
                _exchange, _routing_key, properties, _body, _mandatory):
            if properties.type == 'raise':
                LOGGER.debug('Raising testing.UndeliveredMessage()')
                raise testing.UndeliveredMessage()

        with self.publishing_side_effect(raise_undelivered):
            yield self.process_message(self.consumer.settings['body'],
                                       'text/plain')
            yield self.process_message(self.consumer.settings['body'],
                                       'text/plain',
                                       properties={'type': 'raise'})
            yield self.process_message(self.consumer.settings['body'],
                                       'text/plain')

        self.assertListEqual(
            self.consumer.confirmations,
            [True, True, True, False, False, False, True, True, True])


class ConsumerPropertyTestCase(testing.AsyncTestCase):

    def get_consumer(self):
        return common.TestConsumer

    @testing.gen_test
    def test_consumer_app_id(self):
        app_id = str(uuid.uuid4())
        yield self.process_message(mocks.BODY, properties={'app_id': app_id})
        self.assertEqual(self.consumer.app_id, app_id)

    def test_consumer_app_id_none(self):
        self.consumer._message = None
        self.assertIsNone(self.consumer.app_id)

    @testing.gen_test
    def test_consumer_content_encoding(self):
        value = str(uuid.uuid4())
        yield self.process_message(
            mocks.BODY, properties={'content_encoding': value})
        self.assertEqual(self.consumer.content_encoding, value)

    @testing.gen_test
    def test_consumer_content_type(self):
        value = str(uuid.uuid4())
        yield self.process_message(
            mocks.BODY, properties={'content_type': value})
        self.assertEqual(self.consumer.content_type, value)

    @testing.gen_test
    def test_consumer_exchange(self):
        value = str(uuid.uuid4())
        yield self.process_message(mocks.BODY, exchange=value)
        self.assertEqual(self.consumer.exchange, value)

    @testing.gen_test
    def test_consumer_expiration(self):
        value = str(uuid.uuid4())
        yield self.process_message(
            mocks.BODY,  properties={'expiration': value})
        self.assertEqual(self.consumer.expiration, value)

    @testing.gen_test
    def test_consumer_headers(self):
        value = {str(uuid.uuid4()): str(uuid.uuid4())}
        yield self.process_message(mocks.BODY,  properties={'headers': value})
        self.assertDictEqual(self.consumer.headers, value)

    @testing.gen_test
    def test_consumer_ioloop(self):
        yield self.process_message(mocks.BODY)
        self.assertEqual(self.consumer.io_loop, self.io_loop)

    @testing.gen_test
    def test_consumer_measurement(self):
        result = yield self.process_message(mocks.BODY)
        self.assertEqual(result, self.consumer.measurement)

    @testing.gen_test
    def test_consumer_message_type(self):
        value = str(uuid.uuid4())
        yield self.process_message(mocks.BODY, properties={'type': value})
        self.assertEqual(self.consumer.message_type, value)

    @testing.gen_test
    def test_consumer_priority(self):
        value = random.randint(0, 10)
        yield self.process_message(mocks.BODY, properties={'priority': value})
        self.assertEqual(self.consumer.priority, value)

    @testing.gen_test
    def test_consumer_redelivered(self):
        value = str(uuid.uuid4())
        yield self.process_message(mocks.BODY)
        self.assertFalse(self.consumer.redelivered)

    @testing.gen_test
    def test_consumer_reply_to(self):
        value = str(uuid.uuid4())
        yield self.process_message(mocks.BODY, properties={'reply_to': value})
        self.assertEqual(self.consumer.reply_to, value)

    @testing.gen_test
    def test_consumer_routing_key(self):
        value = str(uuid.uuid4())
        yield self.process_message(mocks.BODY, routing_key=value)
        self.assertEqual(self.consumer.routing_key, value)

    @testing.gen_test
    def test_consumer_timestamp(self):
        value = int(time.time())
        yield self.process_message(
            mocks.BODY, 'text/plain', properties={'timestamp': value})
        self.assertEqual(self.consumer.timestamp, value)

    @testing.gen_test
    def test_consumer_user_id(self):
        value = str(uuid.uuid4())
        yield self.process_message(mocks.BODY, properties={'user_id': value})
        self.assertEqual(self.consumer.user_id, value)


class RequireSettingsConsumer(consumer.Consumer):

    def __init__(self, *args, **kwargs):
        super(RequireSettingsConsumer, self).__init__(*args, **kwargs)
        self.setting_key = None
        self.processed = False

    def prepare(self):
        self.require_setting(self.setting_key)

    def process(self):
        self.processed = True


class RequireSettingTestCase(testing.AsyncTestCase):

    KEY = str(uuid.uuid4())
    VALUE = str(uuid.uuid4())

    def get_consumer(self):
        return RequireSettingsConsumer

    def get_settings(self):
        return {self.KEY: self.VALUE}

    @testing.gen_test
    def test_no_exception_raised(self):
        self.consumer.setting_key = self.KEY
        yield self.process_message(mocks.BODY)
        self.assertTrue(self.consumer.processed)

    @testing.gen_test
    def test_exception_raised(self):
        self.consumer.setting_key = str(uuid.uuid4())
        with self.assertRaises(consumer.ConfigurationException):
            yield self.process_message(mocks.BODY)
        self.assertFalse(self.consumer.processed)
