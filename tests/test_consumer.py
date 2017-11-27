# coding=utf-8
"""Tests for rejected.consumer"""
import logging
import unittest
import uuid

from tornado import gen
import mock

from rejected import consumer, connection, process, testing

from . import mocks

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

    def test_initialize_is_invoked(self):
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


class TestConsumer(consumer.Consumer):

    def __init__(self, *args, **kwargs):
        self.called_initialize = False
        self.called_prepare = False
        self.called_process = False
        self.called_on_finish = False
        super(TestConsumer, self).__init__(*args, **kwargs)

    def initialize(self):
        self.called_initialize = True

    def prepare(self):
        self.called_prepare = True
        return super(TestConsumer, self).prepare()

    def process(self):
        self.called_process = True

    def on_finish(self):
        self.called_on_finish = True


class ConsumerLifecycleTests(testing.AsyncTestCase):

    def get_consumer(self):
        return TestConsumer

    @testing.gen_test
    def test_contract_met(self):
        yield self.process_message(str(uuid.uuid4()))
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
