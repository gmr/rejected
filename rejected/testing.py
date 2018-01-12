"""
Consumer Testing
================

"""
import contextlib
import json
import logging
import time
import uuid

from helper import config
import mock
from pika import channel, frame, spec
from pika.adapters import tornado_connection
from tornado import gen, testing

try:
    import raven
except ImportError:
    raven = None

from rejected import connection, consumer, data, errors, process

LOGGER = logging.getLogger(__name__)

gen_test = testing.gen_test
"""Add gen_test to rejected.testing's namespace"""


class AsyncTestCase(testing.AsyncTestCase):
    """:class:`tornado.testing.AsyncTestCase` subclass for testing
    :class:`~rejected.consumer.Consumer` classes.
    :class:`tornado.testing.AsyncTestCase` is a subclass of
    :class:`unittest.TestCase`

    .. rubric:: Notes

    The unittest framework is synchronous, so the test must be complete by the
    time the test method returns. This means that asynchronous code cannot be
    used in quite the same way as usual.

    To write test functions that use the
    same yield-based patterns used with the :py:mod:`tornado.gen` module,
    decorate your test methods with
    :func:`@rejected.testing.gen_test <rejected.testing.gen_test>`
    instead of :func:`@tornado.gen.coroutine <tornado.gen.coroutine>`.
    This class also provides the :meth:`~rejected.testing.AsyncTestCase.stop`
    and :meth:`~rejected.testing.AsyncTestCase.wait` methods for a more manual
    style of testing. The test method itself must call ``self.wait()``, and
    asynchronous callbacks should call ``self.stop()`` to signal completion.

    By default, a new :class:`~tornado.ioloop.IOLoop` is constructed for each
    test and is available as ``self.io_loop``. This ``IOLoop`` should be used
    in the construction of HTTP clients/servers, etc. If the code being tested
    requires a global ``IOLoop``, subclasses should override
    :meth:`~rejected.testing.AsyncTestCase.get_new_ioloop` to return it.

    The ``IOLoop``'s start and stop methods should not be called directly.
    Instead, use ``self.stop`` and ``self.wait``. Arguments passed to
    ``self.stop`` are returned from ``self.wait``. It is possible to have
    multiple wait/stop cycles in the same test.

    """
    _consumer = None
    PUBLISHER_CONFIRMATIONS = False

    def __init__(self, *args, **kwargs):
        super(AsyncTestCase, self).__init__(*args, **kwargs)
        self.correlation_id = None
        self.logger = logging.getLogger(self.__class__.__name__)
        self.process = None
        self.consumer = None
        self.channel = None
        self.publish_callable = None
        self.publish_calls = []

    def setUp(self):
        """Method called to prepare the test fixture. This is called
        immediately before calling the test method; other than
        :exc:`AssertionError` or :exc:`unittest.SkipTest`, any exception
        raised by this method will be considered an error rather than a test
        failure.

        .. warning:: If you extend this method, you **MUST** invoke
            ``super(YourAsyncTestCase, self).setUp()`` to properly setup
            the test case.

        """
        super(AsyncTestCase, self).setUp()
        self.correlation_id = str(uuid.uuid4())
        self.process = self._create_process()
        self.consumer = self._create_consumer()
        self.channel = self.process.connections['mock'].channel
        print(self.channel.is_closed)
        self.publish_callable = None
        self.publish_calls = []
        self.channel.basic_publish.side_effect = self._on_publish

    def tearDown(self):
        """Method called immediately after the test method has been called and
        the result recorded. This is called even if the test method raised an
        exception, so the implementation in subclasses may need to be
        particularly careful about checking internal state. Any exception,
        other than :exc:`AssertionError` or :exc:`unittest.SkipTest`, raised
        by this method will be considered an additional error rather than a
        test failure (thus increasing the total number of reported errors).

        This method will only be called if the
        :meth:`~rejected.testing.AsyncTestCase.setUp` succeeds, regardless of
        the outcome of the test method.

        .. warning:: If you extend this method, you **MUST** invoke
            ``super(YourAsyncTestCase, self).tearDown()`` to properly tear down
            the test case.

        """
        super(AsyncTestCase, self).tearDown()
        if not self.consumer.is_finished:
            self.consumer.finish()

    def get_consumer(self):
        """Override to return the consumer class for testing.

        :rtype: class

        """
        return consumer.Consumer

    def get_settings(self):
        """Override this method to provide settings to the consumer during
        construction. These settings should be from the `config` stanza
        of the Consumer configuration.

        :rtype: dict

        """
        return {}

    def create_message(self, message, properties=None,
                       exchange='rejected', routing_key='test'):
        """Create a message instance for use with the consumer in testing.

        :param any message: the body of the message to create
        :param dict properties: AMQP message properties
        :param str exchange: The exchange the message should appear to be from
        :param str routing_key: The message's routing key
        :rtype: :class:`rejected.data.Message`

        """
        if not properties:
            properties = {}
        if isinstance(message, dict) and \
                properties.get('content_type') == 'application/json':
            message = json.dumps(message)
        return data.Message(
            connection='mock',
            channel=self.process.connections['mock'].channel,
            method=spec.Basic.Deliver(
                'ctag0', 1, False, exchange, routing_key),
            properties=spec.BasicProperties(
                app_id=properties.get('app_id', 'rejected.testing'),
                content_encoding=properties.get('content_encoding'),
                content_type=properties.get('content_type'),
                correlation_id=properties.get(
                    'correlation_id', self.correlation_id),
                delivery_mode=properties.get('delivery_mode', 1),
                expiration=properties.get('expiration'),
                headers=properties.get('headers'),
                message_id=properties.get('message_id', str(uuid.uuid4())),
                priority=properties.get('priority'),
                reply_to=properties.get('reply_to'),
                timestamp=properties.get('timestamp', int(time.time())),
                type=properties.get('type'),
                user_id=properties.get('user_id')
            ), body=message)

    @property
    def measurement(self):
        """Return the :py:class:`rejected.data.Measurement` for the currently
        assigned measurement object to the consumer.

        :rtype: :class:`rejected.data.Measurement`

        """
        return self.consumer.measurement

    @gen.coroutine
    def process_message(self,
                        message_body=None,
                        content_type='application/json',
                        message_type=None,
                        properties=None,
                        exchange='rejected',
                        routing_key='routing-key'):
        """Process a message as if it were being delivered by RabbitMQ. When
        invoked, an AMQP message will be locally created and passed into the
        consumer. With using the default values for the method, if you pass in
        a JSON serializable object, the message body will automatically be
        JSON serialized.

        If an exception is not raised, a :class:`~rejected.data.Measurement`
        instance is returned that will contain all of the measurements
        collected during the processing of the message.

        Example:

        .. code-block:: python

            class ConsumerTestCase(testing.AsyncTestCase):

                @testing.gen_test
                def test_consumer_raises_message_exception(self):
                    with self.assertRaises(consumer.MessageException):
                        yield self.process_message({'foo': 'bar'})

        .. note:: This method is a co-routine and must be yielded to ensure
                  that your tests are functioning properly.

        :param any message_body: the body of the message to create
        :param str content_type: The mime type
        :param str message_type: identifies the type of message to create
        :param dict properties: AMQP message properties
        :param str exchange: The exchange the message should appear to be from
        :param str routing_key: The message's routing key
        :raises: :exc:`AssertionError` when an unhandled exception is raised
        :raises: :exc:`~rejected.consumer.ConsumerException`
        :raises: :exc:`~rejected.consumer.MessageException`
        :raises: :exc:`~rejected.consumer.ProcessingException`
        :rtype: :class:`rejected.data.Measurement`

        """
        properties = properties or {}
        properties.setdefault('content_type', content_type)
        properties.setdefault('correlation_id', self.correlation_id)
        properties.setdefault('timestamp', int(time.time()))
        properties.setdefault('type', message_type)

        measurement = data.Measurement()

        result = yield self.consumer.execute(
            self.create_message(
                message_body, properties, exchange, routing_key),
            measurement)
        self.logger.info('execute returned %r', result)
        if result == data.CONSUMER_EXCEPTION:
            raise consumer.ConsumerException()
        elif result == data.MESSAGE_EXCEPTION:
            raise consumer.MessageException()
        elif result == data.PROCESSING_EXCEPTION:
            raise consumer.ProcessingException()
        elif result == data.CONFIGURATION_EXCEPTION:
            raise consumer.ConfigurationException()
        elif result == data.RABBITMQ_EXCEPTION:
            raise errors.RabbitMQException(
                self.process.connections['mock'],
                999, 'test-exception')
        elif result == data.UNHANDLED_EXCEPTION:
            raise AssertionError('UNHANDLED_EXCEPTION')
        raise gen.Return(measurement)

    @property
    def published_messages(self):
        """Return a list of :class:`~rejected.testing.PublishedMessage`
        that are extracted from all calls to
        :meth:`~pika.channel.Channel.basic_publish` that are invoked during the
        test. The properties attribute is the
        :class:`pika.spec.BasicProperties`
        instance that was created during publishing.

        .. versionadded:: 3.18.9

        :returns: list([:class:`~rejected.testing.PublishedMessage`])

        """
        return self.publish_calls

    @contextlib.contextmanager
    def publishing_side_effect(self, func=None):
        """Assign a callable (lambda function, method, etc) to invoke for each
        published message. Will be invoked with 4 arguments: Exchange (str),
        Routing Key (str), AMQP Message
        Properties (:class:`pika.spec.BasicProperties`), and Body (bytes).

        Raise a :exc:`rejected.testing.UnroutableMessage` exception
        to trigger the message to be returned to the Consumer as unroutable.

        Raise a :exc:`rejected.testing.UndeliveredMessage` exception
        to trigger an undelivered confirmation message when publisher
        confirmations are enabled.

        .. versionadded:: 4.0.0

        :param callable func: The function / method to execute

        """
        self.publish_callable = func
        yield
        self.publish_callable = None

    def _create_connection(self, name, callbacks):
        conn = mock.Mock(spec=tornado_connection.TornadoConnection)
        with mock.patch('rejected.connection.Connection.connect') as connect:
            connect.return_value = conn
            obj = connection.Connection(name, {}, 'test-consumer', True,
                                        self.PUBLISHER_CONFIRMATIONS,
                                        self.io_loop, callbacks)
            obj.set_state(obj.STATE_ACTIVE)
            obj.channel = mock.Mock(spec=channel.Channel)
            obj.channel._state = obj.channel.OPEN
            obj.channel.connection = obj
            obj.channel.is_closed = False
            obj.channel.is_closing = False
            obj.channel.is_open = True
            return obj

    def _create_consumer(self):
        """Creates the per-test instance of the consumer that is going to be
        tested.

        :rtype: rejected.consumer.Consumer

        """
        obj = self.get_consumer()(
            process=self.process, settings=config.Data(self.get_settings()))
        obj._message = self.create_message('dummy')
        obj.set_connection(self.process.connections['mock'])
        return obj

    def _create_process(self):
        obj = mock.Mock(spec=process.Process)
        callbacks = connection.Callbacks(obj.on_connection_ready,
                                         obj.on_connection_failure,
                                         obj.on_connection_closed,
                                         obj.on_connection_blocked,
                                         obj.on_connection_unblocked,
                                         obj.on_confirmation,
                                         obj.on_delivery)
        obj.connections = {'mock': self._create_connection('mock', callbacks)}
        obj.sentry_client = mock.Mock(spec=raven.Client) if raven else None
        return obj

    def _on_publish(self, exchange, routing_key, properties, body,
                    mandatory=True):
        LOGGER.debug('on_publish to %s using %s (pc: %s)',
                     exchange, routing_key, self.PUBLISHER_CONFIRMATIONS)
        msg = PublishedMessage(exchange, routing_key, properties, body)
        self.publish_calls.append(msg)
        error = None

        if self.publish_callable:
            try:
                self.publish_callable(
                    exchange, routing_key, properties, body, mandatory)
            except (UndeliveredMessage, UnroutableMessage) as err:
                error = err
                LOGGER.debug('Message side effect is %r', error)

        if self.PUBLISHER_CONFIRMATIONS:
            method = spec.Basic.Ack
            msg.delivered = True
            if isinstance(error, UndeliveredMessage):
                method = spec.Basic.Nack
                msg.delivered = False
            elif isinstance(error, UnroutableMessage):
                msg.delivered = False
                self.io_loop.add_callback(
                    self.process.connections['mock'].on_return, 1,
                    spec.Basic.Return(
                        312, 'NO ROUTE', exchange, routing_key),
                    properties,
                    body)
            self.io_loop.add_callback(
                self.process.connections['mock'].on_confirmation,
                frame.Method(1, method(len(self.publish_calls), False)))


class PublishedMessage(object):
    """Contains information about messages published during a test when
    using :class:`rejected.testing.AsyncTestCase`.

    :param str exchange: The exchange the message was published to
    :param str routing_key: The routing key the message was published with
    :param pika.spec.BasicProperties properties: AMQP message properties
    :param bytes body: AMQP message body
    :param bool delivered: Indicates if the message was delivered when
        Publisher Confirmations are enabled

    .. versionadded:: 3.18.9

    """
    __slots__ = ['exchange', 'routing_key', 'properties', 'body', 'delivered']

    def __init__(self, exchange, routing_key, properties, body,
                 delivered=None):
        """Create a new instance of the object.

        :param str exchange: The exchange the message was published to
        :param str routing_key: The routing key the message was published with
        :param pika.spec.BasicProperties properties: AMQP message properties
        :param bytes body: AMQP message body
        :param bool delivered: Indicates if the message was delivered when
            Publisher Confirmations are enabled

        """
        self.exchange = exchange
        self.routing_key = routing_key
        self.properties = properties
        self.body = body
        self.delivered = delivered

    def __repr__(self):  # pragma: nocover
        """Return the string representation of the object.

        :rtype: str

        """
        return '<PublishedMessage exchange="{}" routing_key="{}">'.format(
            self.exchange, self.routing_key)


class UndeliveredMessage(Exception):
    """Raise as a side effect of :attr:`rejected.testing.AsyncTestCase` with
    :meth:`~rejected.testing.AsyncTestCase.publishing_side_effect` to test
    negative acknowledgements when using publisher confirmations.

    """


class UnroutableMessage(Exception):
    """Raise as a side effect of :attr:`rejected.testing.AsyncTestCase` with
    :meth:`~rejected.testing.AsyncTestCase.publishing_side_effect` to test
    branches dealing with messages returned by RabbitMQ as unroutable when
    using publisher confirmations.

    """
