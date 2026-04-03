"""
The :class:`rejected.testing.AsyncTestCase` provides a base class for the
easy creation of tests for your consumers. It is built on top of
:class:`unittest.IsolatedAsyncioTestCase`.

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
consumer, the consumer will raise a
:exc:`~rejected.exceptions.MessageException`.

.. code:: python

    from rejected import exceptions, testing

    import my_package


    class ConsumerTestCase(testing.AsyncTestCase):

        def get_consumer(self):
            return my_package.Consumer

        def get_settings(self):
            return {'remote_url': 'http://foo'}

        async def test_consumer_raises_message_exception(self):
            with self.assertRaises(exceptions.MessageException):
                await self.process_message({'foo': 'bar'})

"""

import datetime
import json
import logging
import typing
import unittest
import uuid
from unittest import mock

from pika import channel, spec
from pika.adapters import asyncio_connection

try:
    import sentry_sdk
except ImportError:
    sentry_sdk = None

from . import codecs, connection, consumer, exceptions, models, process
from . import config as config_module
from . import measurement as measurement_mod

LOGGER = logging.getLogger(__name__)


class AsyncTestCase(unittest.IsolatedAsyncioTestCase):
    """:class:`unittest.IsolatedAsyncioTestCase` subclass for testing
    :class:`~rejected.consumer.Consumer` and
    :class:`~rejected.consumer.TransactionConsumer` classes.

    """

    _consumer: consumer._Consumer | None = None
    _last_ctx: models.ProcessingContext | None = None

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.correlation_id = str(uuid.uuid4())
        self.process = self._create_process()
        self.consumer = self._create_consumer()
        self.channel = self.process.connections['mock'].channel
        self.exc_info: (
            tuple[type[BaseException], BaseException, typing.Any] | None
        ) = None

    @property
    def published_messages(self) -> list['PublishedMessage']:
        """Return a list of :class:`PublishedMessage` extracted from
        all calls to :meth:`pika.channel.Channel.basic_publish` during
        the test.

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

    def get_consumer(self) -> type[consumer._Consumer]:
        """Override to return the consumer class for testing."""
        return consumer.Consumer

    def get_settings(self) -> dict[str, typing.Any]:
        """Override to provide settings to the consumer during
        construction.

        """
        return {}

    def create_context(
        self,
        message_body: typing.Any = None,
        content_type: str = 'application/json',
        message_type: str | None = None,
        properties: dict[str, typing.Any] | None = None,
        exchange: str = 'rejected',
        routing_key: str = 'test',
    ) -> models.ProcessingContext:
        """Create a :class:`~rejected.models.ProcessingContext` for
        testing.

        If ``message_body`` is a dict and ``content_type`` is
        ``application/json``, the body is JSON-serialized.

        """
        properties = properties or {}
        properties.setdefault('content_type', content_type)
        properties.setdefault('correlation_id', self.correlation_id)
        properties.setdefault(
            'timestamp',
            int(datetime.datetime.now(tz=datetime.UTC).timestamp()),
        )
        properties.setdefault('type', message_type)

        if (
            isinstance(message_body, dict)
            and properties.get('content_type') == 'application/json'
        ):
            message_body = json.dumps(message_body)

        mock_conn = mock.Mock(spec=connection.Connection)
        mock_conn.is_running = True

        msg = models.Message(
            delivery_tag=1,
            exchange=exchange,
            routing_key=routing_key,
            body=message_body,
            app_id=properties.get('app_id', 'rejected.testing'),
            content_encoding=properties.get('content_encoding'),
            content_type=properties.get('content_type'),
            correlation_id=properties.get(
                'correlation_id', self.correlation_id
            ),
            delivery_mode=properties.get('delivery_mode', 1),
            expiration=properties.get('expiration'),
            headers=properties.get('headers', {}),
            message_id=properties.get('message_id', str(uuid.uuid4())),
            type=properties.get('type'),
            priority=properties.get('priority'),
            redelivered=False,
            reply_to=properties.get('reply_to'),
            returned=False,
            timestamp=(
                datetime.datetime.fromtimestamp(
                    properties['timestamp'], tz=datetime.UTC
                )
                if properties.get('timestamp')
                else None
            ),
            user_id=properties.get('user_id'),
        )
        return models.ProcessingContext(
            connection=mock_conn,
            channel=self.process.connections['mock'].channel,
            message=msg,
        )

    @property
    def measurement(self) -> measurement_mod.Measurement | None:
        """Return the Measurement for the last processed message."""
        if self._last_ctx:
            return self._last_ctx.measurement
        return None

    async def process_message(
        self,
        message_body: typing.Any = None,
        content_type: str = 'application/json',
        message_type: str | None = None,
        properties: dict[str, typing.Any] | None = None,
        exchange: str = 'rejected',
        routing_key: str = 'routing-key',
    ) -> measurement_mod.Measurement:
        """Process a message as if it were being delivered by RabbitMQ.

        Builds a :class:`~rejected.models.ProcessingContext` and
        passes it through the consumer's ``execute`` method.

        If an exception is not raised, returns the
        :class:`~rejected.measurement.Measurement` collected during
        processing.

        :raises: :exc:`rejected.exceptions.ConsumerException`
        :raises: :exc:`rejected.exceptions.MessageException`
        :raises: :exc:`rejected.exceptions.ProcessingException`

        """
        ctx = self.create_context(
            message_body,
            content_type,
            message_type,
            properties,
            exchange,
            routing_key,
        )
        self._last_ctx = ctx

        # Patch _log_exception to capture exc_info for re-raising
        original_log = self.consumer._log_exception

        def _capture_log(
            ctx_: models.ProcessingContext, msg_format: str, *args: typing.Any
        ) -> None:
            import sys

            self.exc_info = sys.exc_info()  # type: ignore[assignment]
            original_log(ctx_, msg_format, *args)

        self.consumer._log_exception = _capture_log  # type: ignore[assignment]

        await self.consumer.execute(ctx)

        match ctx.result:
            case models.Result.CONSUMER_EXCEPTION:
                raise exceptions.ConsumerException()
            case models.Result.MESSAGE_EXCEPTION:
                raise exceptions.MessageException()
            case models.Result.PROCESSING_EXCEPTION:
                raise exceptions.ProcessingException()
            case models.Result.UNHANDLED_EXCEPTION:
                if self.exc_info:
                    raise self.exc_info[1]
                raise AssertionError('UNHANDLED_EXCEPTION')
            case models.Result.MESSAGE_REQUEUE:
                raise AssertionError(
                    'Message was requeued — consumer returned MESSAGE_REQUEUE'
                )
        return ctx.measurement

    @staticmethod
    def _create_channel() -> mock.Mock:
        return mock.Mock(spec=channel.Channel)

    def _create_connection(self) -> mock.Mock:
        obj = mock.Mock(spec=asyncio_connection.AsyncioConnection)
        obj.channel = self._create_channel()
        obj.channel.connection = obj
        return obj

    def _create_consumer(self) -> consumer._Consumer:
        cls = self.get_consumer()
        obj = cls(config_module.Settings(self.get_settings()), self.process)
        obj.set_channel('mock', self.process.connections['mock'].channel)
        return obj

    def _create_process(self) -> mock.Mock:
        obj = mock.Mock(spec=process.Process)
        obj.connections = {'mock': self._create_connection()}
        obj.sentry_client = True if sentry_sdk else None
        obj.codec = codecs.Codec()
        return obj


class PublishedMessage:
    """Contains information about messages published during a test.

    :param str exchange: The exchange the message was published to
    :param str routing_key: The routing key used
    :param pika.spec.BasicProperties properties: AMQP message properties
    :param bytes body: AMQP message body

    .. versionadded:: 3.18.9

    """

    __slots__ = ['body', 'exchange', 'properties', 'routing_key']

    def __init__(
        self,
        exchange: str,
        routing_key: str,
        properties: spec.BasicProperties,
        body: bytes,
    ) -> None:
        self.exchange = exchange
        self.routing_key = routing_key
        self.properties = properties
        self.body = body

    def __repr__(self) -> str:
        return (
            f'<PublishedMessage exchange="{self.exchange}"'
            f' routing_key="{self.routing_key}">'
        )
