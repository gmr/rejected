"""
The :class:`rejected.testing.AsyncTestCase` provides a based class for the
easy creation of tests for your consumers. The test cases exposes multiple
methods to make it easy to setup a consumer and process messages. It is
built on top of :class:`unittest.IsolatedAsyncioTestCase`.

To get started, override the
:meth:`rejected.testing.AsyncTestCase.get_consumer` method.

Next, the :meth:`rejected.testing.AsyncTestCase.get_settings` method can be
overridden to define the settings that are passed into the consumer.

Finally, to invoke your Consumer as if it were receiving a message, the
:meth:`~rejected.testing.AsyncTestCase.process_message` method should be
invoked.

.. note:: Tests are asynchronous; define test methods as ``async def``.

Example
-------
The following example expects that when the message is processed by the
consumer, the consumer will raise a :exc:`~rejected.consumer.MessageException`.

.. code:: python

    from rejected import consumer, testing

    import my_package


    class ConsumerTestCase(testing.AsyncTestCase):

        def get_consumer(self):
            return my_package.Consumer

        def get_settings(self):
            return {'remote_url': 'http://foo'}

        async def test_consumer_raises_message_exception(self):
            with self.assertRaises(consumer.MessageException):
                await self.process_message({'foo': 'bar'})

"""

import json
import logging
import time
import unittest
import uuid
from unittest import mock

from helper import config
from pika import channel, spec
from pika.adapters import asyncio_connection

try:
    import sentry_sdk
except ImportError:
    sentry_sdk = None

from . import consumer, data, process

LOGGER = logging.getLogger(__name__)


class AsyncTestCase(unittest.IsolatedAsyncioTestCase):
    """:class:`unittest.IsolatedAsyncioTestCase` subclass for testing
    :class:`~rejected.consumer.Consumer` classes.

    """

    _consumer = None

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.correlation_id = str(uuid.uuid4())
        self.process = self._create_process()
        self.consumer = self._create_consumer()
        self.channel = self.process.connections['mock'].channel
        self.exc_info = None

    async def asyncTearDown(self):
        await super().asyncTearDown()
        if not self.consumer._finished:
            await self.consumer.finish()

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
        return [
            PublishedMessage(
                body=c[2]['body'],
                exchange=c[2]['exchange'],
                properties=c[2]['properties'],
                routing_key=c[2]['routing_key'],
            )
            for c in self.channel.basic_publish.mock_calls
        ]

    def get_consumer(self):
        """Override to return the consumer class for testing.

        :rtype: :class:`rejected.consumer.Consumer`

        """
        return consumer.Consumer

    def get_settings(self):
        """Override this method to provide settings to the consumer during
        construction. These settings should be from the `config` stanza
        of the Consumer configuration.

        :rtype: dict

        """
        return {}

    def create_message(
        self, message, properties=None, exchange='rejected', routing_key='test'
    ):
        """Create a message instance for use with the consumer in testing.

        :param any message: the body of the message to create
        :param dict properties: AMQP message properties
        :param str exchange: The exchange the message should appear to be from
        :param str routing_key: The message's routing key
        :rtype: :class:`rejected.data.Message`

        """
        if not properties:
            properties = {}
        if (
            isinstance(message, dict)
            and properties.get('content_type') == 'application/json'
        ):
            message = json.dumps(message)
        return data.Message(
            connection='mock',
            channel=self.process.connections['mock'].channel,
            method=spec.Basic.Deliver(
                'ctag0', 1, False, exchange, routing_key
            ),
            properties=spec.BasicProperties(
                app_id=properties.get('app_id', 'rejected.testing'),
                content_encoding=properties.get('content_encoding'),
                content_type=properties.get('content_type'),
                correlation_id=properties.get(
                    'correlation_id', self.correlation_id
                ),
                delivery_mode=properties.get('delivery_mode', 1),
                expiration=properties.get('expiration'),
                headers=properties.get('headers'),
                message_id=properties.get('message_id', str(uuid.uuid4())),
                priority=properties.get('priority'),
                reply_to=properties.get('reply_to'),
                timestamp=properties.get('timestamp', int(time.time())),
                type=properties.get('type'),
                user_id=properties.get('user_id'),
            ),
            body=message,
            returned=False,
        )

    def log_exception(self, msg_format, *args, exc_info):
        """Customize the logging of uncaught exceptions.

        :param str msg_format: format of msg to log with ``self.logger.error``
        :param args: positional arguments to pass to ``self.logger.error``
        :param exc_info: The exc_info for the exception

        This for internal use and should not be extended or used directly.

        By default, this method will log the message using
        :meth:`logging.Logger.error` and send the exception to Sentry.
        If an exception is currently active, then the traceback will be
        logged at the debug level.

        """
        LOGGER.exception(msg_format, *args, exc_info=exc_info)
        self.exc_info = exc_info

    @property
    def measurement(self):
        """Return the :py:class:`rejected.data.Measurement` for the currently
        assigned measurement object to the consumer.

        :rtype: :class:`rejected.data.Measurement`

        """
        return self.consumer._measurement

    async def process_message(
        self,
        message_body=None,
        content_type='application/json',
        message_type=None,
        properties=None,
        exchange='rejected',
        routing_key='routing-key',
    ):
        """Process a message as if it were being delivered by RabbitMQ. When
        invoked, an AMQP message will be locally created and passed into the
        consumer. With using the default values for the method, if you pass in
        a JSON serializable object, the message body will automatically be
        JSON serialized.

        If an exception is not raised, a :class:`~rejected.data.Measurement`
        instance is returned that will contain all of the measurements
        collected during the processing of the message.

        Example:

        .. code:: python

            class ConsumerTestCase(testing.AsyncTestCase):

                async def test_consumer_raises_message_exception(self):
                    with self.assertRaises(consumer.MessageException):
                        result = await self.process_message({'foo': 'bar'})


        .. note:: This method is a coroutine and must be awaited to ensure
                  that your tests are functioning properly.

        :param any message_body: the body of the message to create
        :param str content_type: The mime type
        :param str message_type: identifies the type of message to create
        :param dict properties: AMQP message properties
        :param str exchange: The exchange the message should appear to be from
        :param str routing_key: The message's routing key
        :raises: :exc:`rejected.consumer.ConsumerException`
        :raises: :exc:`rejected.consumer.MessageException`
        :raises: :exc:`rejected.consumer.ProcessingException`
        :rtype: :class:`rejected.data.Measurement`

        """
        properties = properties or {}
        properties.setdefault('content_type', content_type)
        properties.setdefault('correlation_id', self.correlation_id)
        properties.setdefault('timestamp', int(time.time()))
        properties.setdefault('type', message_type)

        measurement = data.Measurement()

        self.consumer.log_exception = self.log_exception
        result = await self.consumer.execute(
            self.create_message(
                message_body, properties, exchange, routing_key
            ),
            measurement,
        )
        if result == data.CONSUMER_EXCEPTION:
            raise consumer.ConsumerException()
        elif result == data.MESSAGE_EXCEPTION:
            raise consumer.MessageException()
        elif result == data.PROCESSING_EXCEPTION:
            raise consumer.ProcessingException()
        elif result == data.UNHANDLED_EXCEPTION:
            if self.exc_info:
                raise self.exc_info[1]
            raise AssertionError('UNHANDLED_EXCEPTION')
        return measurement

    @staticmethod
    def _create_channel():
        return mock.Mock(spec=channel.Channel)

    def _create_connection(self):
        obj = mock.Mock(spec=asyncio_connection.AsyncioConnection)
        obj.channel = self._create_channel()
        obj.channel.connection = obj
        return obj

    def _create_consumer(self):
        """Creates the per-test instance of the consumer that is going to be
        tested.

        :rtype: rejected.consumer.Consumer

        """
        cls = self.get_consumer()
        obj = cls(config.Data(self.get_settings()), self.process)
        obj._message = self.create_message('dummy')
        obj.set_channel('mock', self.process.connections['mock'].channel)
        return obj

    def _create_process(self):
        obj = mock.Mock(spec=process.Process)
        obj.connections = {'mock': self._create_connection()}
        obj.sentry_client = True if sentry_sdk else None
        return obj


class PublishedMessage:
    """Contains information about messages published during a test when
    using :class:`rejected.testing.AsyncTestCase`.

    :param str exchange: The exchange the message was published to
    :param str routing_key: The routing key the message was published with
    :param pika.spec.BasicProperties properties: AMQP message properties
    :param bytes body: AMQP message body

    .. versionadded:: 3.18.9

    """

    __slots__ = ['body', 'exchange', 'properties', 'routing_key']

    def __init__(self, exchange, routing_key, properties, body):
        """Create a new instance of the object.

        :param str exchange: The exchange the message was published to
        :param str routing_key: The routing key the message was published with
        :param pika.spec.BasicProperties properties: AMQP message properties
        :param bytes body: AMQP message body

        """
        self.exchange = exchange
        self.routing_key = routing_key
        self.properties = properties
        self.body = body

    def __repr__(self):
        """Return the string representation of the object.

        :rtype: str

        """
        return (
            f'<PublishedMessage exchange="{self.exchange}"'
            f' routing_key="{self.routing_key}">'
        )
