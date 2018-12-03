"""
The :py:class:`Consumer`, and :py:class:`SmartConsumer` provide base
classes to extend for consumer applications.

While the :py:class:`Consumer` class provides all the structure required for
implementing a rejected consumer, the :py:class:`SmartConsumer` adds
functionality designed to make writing consumers even easier. When messages
are received by consumers extending :py:class:`SmartConsumer`, if the message's
``content_type`` property contains one of the supported mime-types, the message
body will automatically be deserialized, making the deserialized message body
available via the ``body`` attribute. Additionally, should one of the supported
``content_encoding`` types (``gzip`` or ``bzip2``) be specified in the
message's property, it will automatically be decoded.

Supported `SmartConsumer` MIME types are:

 - application/msgpack (with u-msgpack-python installed)
 - application/json
 - application/pickle
 - application/x-pickle
 - application/x-plist
 - application/x-vnd.python.pickle
 - application/vnd.python.pickle
 - text/csv
 - text/html (with beautifulsoup4 installed)
 - text/xml (with beautifulsoup4 installed)
 - text/yaml
 - text/x-yaml

"""
import bz2
import contextlib
import csv
import datetime
import io
import json
import logging
import pickle
import plistlib
import sys
import time
import uuid
import warnings
import zlib

import pika
from pika import exceptions
from tornado import concurrent, gen, locks
import yaml

from rejected import data, log

LOGGER = logging.getLogger(__name__)

# Optional imports
try:
    import bs4
except ImportError:
    LOGGER.warning('BeautifulSoup not found, disabling html and xml support')
    bs4 = None

try:
    import umsgpack
except ImportError:
    LOGGER.warning('umsgpack not found, disabling msgpack support')
    umsgpack = None

# Python3 Support
try:
    unicode()
except NameError:
    unicode = str


DEFAULT_CHANNEL = 'default'
_DROPPED_MESSAGE = 'X-Rejected-Dropped'
_PROCESSING_EXCEPTIONS = 'X-Processing-Exceptions'
_EXCEPTION_FROM = 'X-Exception-From'

BS4_MIME_TYPES = ('text/html', 'text/xml')
PICKLE_MIME_TYPES = ('application/pickle', 'application/x-pickle',
                     'application/x-vnd.python.pickle',
                     'application/vnd.python.pickle')
YAML_MIME_TYPES = ('text/yaml', 'text/x-yaml')


class Consumer(object):
    """Base consumer class that defines the contract between rejected and
    consumer applications.

    In any of the consumer base classes, if the ``message_type`` is specified
    in the configuration (or set with the ``MESSAGE_TYPE`` attribute), the
    ``type`` property of incoming messages will be validated against when a
    message is received. If there is no match, the consumer will not
    process the message and will drop the message without an exception if the
    ``drop_invalid_messages`` setting is set to ``True`` in the configuration
    (or if the ``DROP_INVALID_MESSAGES`` attribute is set to ``True``).
    If it is ``False``, a :exc:`~rejected.consumer.MessageException` is raised.

    If ``DROP_EXCHANGE`` is specified either as an attribute of the consumer
    class or in the consumer configuration, if a message is dropped, it is
    published to the that exchange prior to rejecting the message in RabbitMQ.
    When the message is republished, four new values are added to the AMQP
    ``headers`` message property: ``X-Dropped-By``, ``X-Dropped-Reason``,
    ``X-Dropped-Timestamp``, ``X-Original-Exchange``.

    The ``X-Dropped-By`` header value contains the configured name of the
    consumer that dropped the message. ``X-Dropped-Reason`` contains the
    reason the message was dropped (eg invalid message type or maximum error
    count). ``X-Dropped-Timestamp`` value contains the ISO-8601 formatted
    timestamp of when the message was dropped. Finally, the
    ``X-Original-Exchange`` value contains the original exchange that the
    message was published to.

    If a consumer raises a :exc:`~rejected.consumer.ProcessingException`, the
    message that was being processed will be republished to the exchange
    specified by the ``error`` exchange configuration value or the
    ``ERROR_EXCHANGE`` attribute of the consumer's class. The message will be
    published using the routing key that was last used for the message. The
    original message body and properties will be used and two additional
    header property values may be added:

        - ``X-Processing-Exception`` contains the string value of the
            exception that was raised, if specified.
        - ``X-Processing-Exceptions`` contains the quantity of processing
            exceptions that have been raised for the message.

    In combination with a queue that has ``x-message-ttl`` set
    and ``x-dead-letter-exchange`` that points to the original exchange for the
    queue the consumer is consuming off of, you can implement a delayed retry
    cycle for messages that are failing to process due to external resource or
    service issues.

    If ``error_max_retry`` is specified in the configuration or
    ``ERROR_MAX_RETRY`` is set on the class, the headers for each method
    will be inspected and if the value of ``X-Processing-Exceptions`` is
    greater than or equal to the specified value, the message will
    be dropped.

    As of 3.18.6, the ``MESSAGE_AGE_KEY`` class level attribute contains the
    default key part to used when recording stats for the message age. You can
    also override the :py:meth:`~rejected.consumer.Consumer.message_age_key`
    method to create compound keys. For example, to create a key that includes
    the message priority:

    .. code:: python

        class Consumer(consumer.Consumer):

            def message_age_key(self):
                return 'priority-{}.message_age'.format(self.priority or 0)

    .. note:: Since 3.17, :class:`~rejected.consumer.Consumer` and
        :class:`~rejected.consumer.PublishingConsumer` have been combined
        into the same class.

    As of 3.19.13, the ``ACK_PROCESSING_EXCEPTIONS`` class level attribute
    allows you to ack messages that raise a
    :exc:`~rejected.consumer.ProcessingException` instead of rejecting them,
    allowing for dead-lettered messages to be constrained to
    :exc:`~rejected.consumer.MessageException`s only. Defaults to `False`.

    """
    DROP_EXCHANGE = None
    DROP_INVALID_MESSAGES = False
    MESSAGE_TYPE = None
    ERROR_EXCHANGE = 'errors'
    ERROR_MAX_RETRY = None
    MESSAGE_AGE_KEY = 'message_age'
    ACK_PROCESSING_EXCEPTIONS = False

    def __init__(self, settings, process,
                 drop_invalid_messages=None,
                 message_type=None,
                 error_exchange=None,
                 error_max_retry=None,
                 drop_exchange=None):
        """Creates a new instance of the :class:`~rejected.consumer.Consumer`
        class. To perform initialization tasks, extend
        :meth:`~rejected.consumer.Consumer.initialize`.

        """
        self._channels = {}
        self._correlation_id = None
        self._drop_exchange = drop_exchange or self.DROP_EXCHANGE
        self._drop_invalid = drop_invalid_messages or \
            self.DROP_INVALID_MESSAGES
        self._error_exchange = error_exchange or self.ERROR_EXCHANGE
        self._error_max_retry = error_max_retry or self.ERROR_MAX_RETRY
        self._finished = False
        self._message = None
        self._message_type = message_type or self.MESSAGE_TYPE
        self._measurement = None
        self._message_body = None
        self._process = process
        self._settings = settings
        self._yield_condition = locks.Condition()

        # Create a logger that attaches correlation ID to the record
        self._logger = logging.getLogger(
            settings.get('_import_module', __name__))
        self.logger = log.CorrelationAdapter(self._logger, self)

        # Set a Sentry context for the consumer
        self.set_sentry_context('consumer', self.name)

        # Run any child object specified initialization
        self.initialize()

    def initialize(self):
        """Extend this method for any initialization tasks that occur only when
        the :class:`~rejected.consumer.Consumer` class is created.

        """
        pass

    def prepare(self):
        """Called when a message is received before
        :meth:`~rejected.consumer.Consumer.process`.

        .. note:: Asynchronous support: Decorate this method with
            :func:`tornado.gen.coroutine` to make it asynchronous.

        If this method returns a :class:`~tornado.concurrent.Future`, execution
        will not proceed until the Future has completed.

        """
        pass

    def process(self):
        """Extend this method for implementing your Consumer logic.

        If the message can not be processed and the Consumer should stop after
        n failures to process messages, raise the
        :exc:`~rejected.consumer.ConsumerException`.

        .. note:: Asynchronous support: Decorate this method with
            :func:`tornado.gen.coroutine` to make it asynchronous.

        :raises: :exc:`rejected.consumer.ConsumerException`
        :raises: :exc:`rejected.consumer.MessageException`
        :raises: :exc:`rejected.consumer.ProcessingException`

        """
        raise NotImplementedError

    def message_age_key(self):
        """Return the key part that is used in submitting message age stats.
        Override this method to change the key part. This could be used to
        include message priority in the key, for example.

        .. versionadded:: 3.18.6

        :rtype: str

        """
        return self.MESSAGE_AGE_KEY

    def on_finish(self):
        """Called after a message has been processed.

        Override this method to perform cleanup, logging, etc.
        This method is a counterpart to
        :meth:`~rejected.consumer.Consumer.prepare`.  ``on_finish`` may
        not produce any output, as it is called after all processing has
        taken place.

        If an exception is raised during the processing of a message,
        :meth:`~rejected.consumer.Consumer.prepare` is not invoked.

        .. note:: Asynchronous support: Decorate this method with
            :func:`tornado.gen.coroutine` to make it asynchronous.

        """
        self.logger.debug('on_finished invoked')

    def on_blocked(self, name):
        """Called when a connection for this consumer is blocked.

        Override this method to respond to being blocked.

        .. versionadded:: 3.17

        :param str name: The connection name that is blocked

        """
        self.logger.debug('Connection %s has been blocked', name)

    def on_unblocked(self, name):
        """Called when a connection for this consumer is unblocked.

        Override this method to respond to being blocked.

        .. versionadded:: 3.17

        :param str name: The connection name that is blocked

        """
        self.logger.debug('Connection %s has been unblocked', name)

    def shutdown(self):
        """Override to cleanly shutdown when rejected is stopping the consumer.

        This could be used for closing database connections or other such
        activities.

        """
        self.logger.debug('shutdown invoked')

    """Utility Methods for use by Consumer Code"""

    def finish(self):
        """Finishes message processing for the current message. If this is
        called in :meth:`~rejected.consumer.Consumer.prepare`, the
        :meth:`~rejected.consumer.Consumer.process` method is not invoked
        for the current message.

        """
        if self._finished:
            self.logger.warning('Finished called when already finished')
            return
        self._finished = True
        self.on_finish()

    def publish_message(self, exchange, routing_key, properties, body,
                        channel=None):
        """Publish a message to RabbitMQ on the same channel the original
        message was received on.

        :param str exchange: The exchange to publish to
        :param str routing_key: The routing key to publish with
        :param dict properties: The message properties
        :param str body: The message body
        :param str channel: The channel/connection name to use. If it is not
            specified, the channel that the message was delivered on is used.

        """
        self.logger.debug('Publishing message to %s:%s (%s)',
                          exchange, routing_key, channel)
        with self._measurement.track_duration(
                'publish.{}.{}'.format(exchange, routing_key)):
            self._publish_channel(channel).basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                properties=self._get_pika_properties(properties),
                body=body)

    def reply(self, response_body, properties,
              auto_id=True,
              exchange=None,
              reply_to=None):
        """Reply to the received message.

        If ``auto_id`` is :data:`True`, a new UUIDv4 value will be generated
        for the ``message_id`` AMQP message property. The ``correlation_id``
        AMQP message property will be set to the ``message_id`` of the
        original message. In addition, the ``timestamp`` will be assigned the
        current time of the message. If ``auto_id`` is :data:`False`, neither
        the ``message_id`` and the ``correlation_id`` AMQP properties will be
        changed in the properties.

        If ``exchange`` is not set, the exchange the message was received on
        will be used.

        If ``reply_to`` is set in the original properties,
        it will be used as the routing key. If the ``reply_to`` is not set
        in the properties and it is not passed in, a :exc:`ValueError` will be
        raised. If reply to is set in the properties, it will be cleared out
        prior to the message being republished.

        :param any response_body: The message body to send
        :param properties: Message properties to use
        :type properties: :class:`rejected.data.Properties`
        :param bool auto_id: Automatically shuffle ``message_id`` &
            ``correlation_id``
        :param str exchange: Override the exchange to publish to
        :param str reply_to: Override the ``reply_to`` AMQP property
        :raises: :exc:`ValueError`

        """
        if not properties.reply_to and not reply_to:
            raise ValueError('Missing reply_to in properties or as argument')

        if auto_id and properties.message_id:
            properties.app_id = __name__
            properties.correlation_id = properties.message_id
            properties.message_id = str(uuid.uuid4())
            properties.timestamp = int(time.time())
            self.logger.debug('New message_id: %s', properties.message_id)
            self.logger.debug('Correlation_id: %s', properties.correlation_id)

        # Redefine the reply to if needed
        reply_to = reply_to or properties.reply_to

        # Wipe out reply_to if it's set
        if properties.reply_to:
            properties.reply_to = None

        self.publish_message(exchange or self._message.exchange, reply_to,
                             dict(properties), response_body)

    def send_exception_to_sentry(self, exc_info):
        """Send an exception to Sentry if enabled.

        :param tuple exc_info: exception information as returned from
            :func:`sys.exc_info`

        """
        self._process.send_exception_to_sentry(exc_info)

    def set_sentry_context(self, tag, value):
        """Set a context tag in Sentry for the given key and value.

        :param str tag: The context tag name
        :param str value: The context value

        """
        if self.sentry_client:
            self.logger.debug(
                'Setting sentry context for %s to %s', tag, value)
            self.sentry_client.tags_context({tag: value})

    def stats_add_duration(self, key, duration):
        """Add a duration to the per-message measurements

        .. versionadded:: 3.19.0

        :param str key: The key to add the timing to
        :param int|float duration: The timing value in seconds

        """
        if not self._measurement:
            LOGGER.warning('stats_add_timing invoked outside execution')
            return
        self._measurement.add_duration(key, duration)

    def stats_add_timing(self, key, duration):
        """Add a timing to the per-message measurements

        .. versionadded:: 3.13.0
        .. deprecated:: 3.19.0

        :param str key: The key to add the timing to
        :param int|float duration: The timing value in seconds

        """
        warnings.warn('Deprecated, use Consumer.stats_add_duration',
                      DeprecationWarning)
        self.stats_add_duration(key, duration)

    def statsd_add_timing(self, key, duration):
        """Add a timing to the per-message measurements

        :param str key: The key to add the timing to
        :param int|float duration: The timing value in seconds

        .. deprecated:: 3.13.0

        """
        warnings.warn('Deprecated, use Consumer.stats_add_duration',
                      DeprecationWarning)
        self.stats_add_duration(key, duration)

    def stats_incr(self, key, value=1):
        """Increment the specified key in the per-message measurements

        .. versionadded:: 3.13.0

        :param str key: The key to increment
        :param int value: The value to increment the key by

        """
        if not self._measurement:
            LOGGER.warning('stats_incr invoked outside execution')
            return
        self._measurement.incr(key, value)

    def statsd_incr(self, key, value=1):
        """Increment the specified key in the per-message measurements

        :param str key: The key to increment
        :param int value: The value to increment the key by

        .. deprecated:: 3.13.0

        """
        warnings.warn('Deprecated, use Consumer.stats_incr',
                      DeprecationWarning)
        self.stats_incr(key, value)

    def stats_set_tag(self, key, value=1):
        """Set the specified tag/value in the per-message measurements

        .. versionadded:: 3.13.0

        :param str key: The key to increment
        :param int value: The value to increment the key by

        """
        if not self._measurement:
            LOGGER.warning('stats_set_tag invoked outside execution')
            return
        self._measurement.set_tag(key, value)

    def stats_set_value(self, key, value=1):
        """Set the specified key/value in the per-message measurements

        .. versionadded:: 3.13.0

        :param str key: The key to increment
        :param int value: The value to increment the key by

        """
        if not self._measurement:
            LOGGER.warning('stats_set_value invoked outside execution')
            return
        self._measurement.set_value(key, value)

    @contextlib.contextmanager
    def stats_track_duration(self, key):
        """Time around a context and add to the the per-message measurements

        .. versionadded:: 3.13.0
        .. deprecated:: 3.19.0

        :param str key: The key for the timing to track

        """
        start_time = time.time()
        try:
            yield
        finally:
            self.stats_add_duration(
                key, max(start_time, time.time()) - start_time)

    def statsd_track_duration(self, key):
        """Time around a context and add to the the per-message measurements

        :param str key: The key for the timing to track

        .. deprecated:: 3.13.0

        """
        warnings.warn('Deprecated, use Consumer.stats_track_duration',
                      DeprecationWarning)
        return self.stats_track_duration(key)

    def unset_sentry_context(self, tag):
        """Remove a context tag from sentry

        :param str tag: The context tag to remove

        """
        if self.sentry_client:
            self.sentry_client.tags.pop(tag, None)

    @gen.coroutine
    def yield_to_ioloop(self):
        """Function that will allow Rejected to process IOLoop events while
        in a tight-loop inside an asynchronous consumer.

        """
        try:
            yield self._yield_condition.wait(
                self._message.channel.connection.ioloop.time() + 0.001)
        except gen.TimeoutError:
            pass

    """Quick-access properties"""

    @property
    def app_id(self):
        """Access the current message's ``app-id`` property as an attribute of
        the consumer class.

        :rtype: str

        """
        if not self._message:
            return None
        return self._message.properties.app_id

    @property
    def body(self):
        """Access the opaque body from the current message.

        :rtype: str

        """
        if not self._message:
            return None
        return self._message.body

    @property
    def content_encoding(self):
        """Access the current message's ``content-encoding`` AMQP message
        property as an attribute of the consumer class.

        :rtype: str

        """
        if not self._message:
            return None
        return (self._message.properties.content_encoding or
                '').lower() or None

    @property
    def content_type(self):
        """Access the current message's ``content-type`` AMQP message property
        as an attribute of the consumer class.

        :rtype: str

        """
        if not self._message:
            return None
        return (self._message.properties.content_type or '').lower() or None

    @property
    def correlation_id(self):
        """Access the current message's ``correlation-id`` AMAP message
        property as an attribute of the consumer class. If the message does not
        have a ``correlation-id`` then, each message is assigned a new UUIDv4
        based ``correlation-id`` value.

        :rtype: str

        """
        return self._correlation_id

    @property
    def exchange(self):
        """Access the AMQP exchange the message was published to as an
        attribute of the consumer class.

        :rtype: str

        """
        if not self._message:
            return None
        return self._message.exchange

    @property
    def expiration(self):
        """Access the current message's ``expiration`` AMQP message property as
        an attribute of the consumer class.

        :rtype: str

        """
        if not self._message:
            return None
        return self._message.properties.expiration

    @property
    def headers(self):
        """Access the current message's ``headers`` AMQP message property as an
        attribute of the consumer class.

        :rtype: dict

        """
        if not self._message:
            return None
        return self._message.properties.headers or dict()

    @property
    def io_loop(self):
        """Access the :py:class:`tornado.ioloop.IOLoop` instance for the
        current message.

        .. versionadded:: 3.18.4

        :rtype: tornado.ioloop.IOLoop

        """
        return self._message.channel.connection.ioloop

    @property
    def message_id(self):
        """Access the current message's ``message-id`` AMQP message property as
        an attribute of the consumer class.

        :rtype: str

        """
        if not self._message:
            return None
        return self._message.properties.message_id

    @property
    def name(self):
        """Property returning the name of the consumer class.

        :rtype: str

        """
        return self.__class__.__name__

    @property
    def priority(self):
        """Access the current message's ``priority`` AMQP message property as
        an attribute of the consumer class.

        :rtype: int

        """
        if not self._message:
            return None
        return self._message.properties.priority

    @property
    def properties(self):
        """Access the current message's AMQP message properties in dict form as
        an attribute of the consumer class.

        :rtype: dict

        """
        if not self._message:
            return None
        return dict(self._message.properties)

    @property
    def redelivered(self):
        """Indicates if the current message has been redelivered.

        :rtype: bool

        """
        if not self._message:
            return None
        return self._message.redelivered

    @property
    def reply_to(self):
        """Access the current message's ``reply-to`` AMQP message property as
        an attribute of the consumer class.

        :rtype: str

        """
        if not self._message:
            return None
        return self._message.properties.reply_to

    @property
    def returned(self):
        """Indicates if the message was delivered by consumer previously and
        returned from RabbitMQ.

        .. versionadded:: 3.17

        :rtype: bool

        """
        if not self._message:
            return None
        return self._message.redelivered

    @property
    def routing_key(self):
        """Access the routing key for the current message.

        :rtype: str

        """
        if not self._message:
            return None
        return self._message.routing_key

    @property
    def message_type(self):
        """Access the current message's ``type`` AMQP message property as an
        attribute of the consumer class.

        :rtype: str

        """
        if not self._message:
            return None
        return self._message.properties.type

    @property
    def sentry_client(self):
        """Access the Sentry raven ``Client`` instance or ``None``

        Use this object to add tags or additional context to Sentry
        error reports (see :meth:`raven.base.Client.tags_context`) or
        to report messages (via :meth:`raven.base.Client.captureMessage`)
        directly to Sentry.

        :rtype: :class:`raven.base.Client`

        """
        if hasattr(self._process, 'sentry_client'):
            return self._process.sentry_client

    @property
    def settings(self):
        """Access the consumer settings as specified by the ``config`` section
        for the consumer in the rejected configuration.

        :rtype: dict

        """
        return self._settings

    @property
    def timestamp(self):
        """Access the unix epoch timestamp value from the AMQP message
        properties of the current message.

        :rtype: int

        """
        if not self._message:
            return None
        return self._message.properties.timestamp

    @property
    def user_id(self):
        """Access the ``user-id`` AMQP message property from the current
        message's properties.

        :rtype: str

        """
        if not self._message:
            return None
        return self._message.properties.user_id

    """Internal Methods"""

    @gen.coroutine
    def execute(self, message_in, measurement):
        """Process the message from RabbitMQ. To implement logic for processing
        a message, extend Consumer._process, not this method.

        This for internal use and should not be extended or used directly.

        :param message_in: The message to process
        :type message_in: :class:`rejected.data.Message`
        :param measurement: For collecting per-message instrumentation
        :type measurement: :class:`rejected.data.Measurement`
        :rtype: bool

        """
        LOGGER.debug('Received: %r', message_in)
        self._clear()
        self._message = message_in
        self._measurement = measurement

        # If timestamp is set, record age of the message coming in
        if message_in.properties.timestamp:
            message_age = float(
                    max(message_in.properties.timestamp, time.time()) -
                    message_in.properties.timestamp)
            if message_age > 0:
                measurement.add_duration(self.message_age_key(), message_age)

        # Ensure there is a correlation ID
        self._correlation_id = message_in.properties.correlation_id or \
            message_in.properties.message_id or \
            str(uuid.uuid4())

        if self.message_type:
            self.set_sentry_context('type', self.message_type)

        # Validate the message type if the child sets MESSAGE_TYPE
        if self._message_type:
            if isinstance(self._message_type, (tuple, list, set)):
                message_supported = self.message_type in self._message_type
            else:
                message_supported = self.message_type == self._message_type

            if not message_supported:
                self.logger.warning('Received unsupported message type: %s',
                                    self.message_type)
                # Should the message be dropped or returned to the broker?
                if self._drop_invalid:
                    if self._drop_exchange:
                        self._republish_dropped_message('invalid type')
                    raise gen.Return(data.MESSAGE_DROP)
                raise gen.Return(data.MESSAGE_EXCEPTION)

        # Check the number of ProcessingErrors and possibly drop the message
        if (self._error_max_retry and
                _PROCESSING_EXCEPTIONS in self.headers):
            if self.headers[_PROCESSING_EXCEPTIONS] >= self._error_max_retry:
                self.logger.warning('Dropping message with %i deaths due to '
                                    'ERROR_MAX_RETRY',
                                    self.headers[_PROCESSING_EXCEPTIONS])
                if self._drop_exchange:
                    self._republish_dropped_message(
                        'max retries ({})'.format(
                            self.headers[_PROCESSING_EXCEPTIONS]))
                raise gen.Return(data.MESSAGE_DROP)

        result = None
        try:
            result = self.prepare()
            if concurrent.is_future(result):
                yield result
            if not self._finished:
                result = self.process()
                if concurrent.is_future(result):
                    yield result
                    self.logger.debug('Post yield of future process')
        except KeyboardInterrupt:
            self.logger.debug('CTRL-C')
            self._process.reject(message_in.delivery_tag, True)
            self._process.stop()
            raise gen.Return(data.MESSAGE_REQUEUE)

        except exceptions.ChannelClosed as error:
            self.logger.critical('Channel closed while processing %s: %s',
                                 message_in.delivery_tag, error)
            self._measurement.set_tag('exception', error.__class__.__name__)
            raise gen.Return(None)

        except exceptions.ConnectionClosed as error:
            self.logger.critical('Connection closed while processing %s: %s',
                                 message_in.delivery_tag, str(error))
            self._measurement.set_tag('exception', error.__class__.__name__)
            raise gen.Return(None)

        except ConsumerException as error:
            self.logger.error('ConsumerException processing delivery %s: %s',
                              message_in.delivery_tag, str(error))
            self._measurement.set_tag('exception', error.__class__.__name__)
            if error.metric:
                self._measurement.set_tag('error', error.metric)
            raise gen.Return(data.CONSUMER_EXCEPTION)

        except MessageException as error:
            self.logger.info('MessageException processing delivery %s: %s',
                             message_in.delivery_tag, str(error))
            self._measurement.set_tag('exception', error.__class__.__name__)
            if error.metric:
                self._measurement.set_tag('error', error.metric)
            raise gen.Return(data.MESSAGE_EXCEPTION)

        except ProcessingException as error:
            self.logger.warning(
                'ProcessingException processing delivery %s: %s',
                message_in.delivery_tag, str(error))
            self._measurement.set_tag('exception', error.__class__.__name__)
            if error.metric:
                self._measurement.set_tag('error', error.metric)
            self._republish_processing_error(
                error.metric or error.__class__.__name__)
            raise gen.Return(data.PROCESSING_EXCEPTION)

        except NotImplementedError as error:
            self.log_exception('NotImplementedError processing delivery'
                               ' %s: %s', message_in.delivery_tag, error)
            self._measurement.set_tag('exception', 'UnhandledException')
            raise gen.Return(data.UNHANDLED_EXCEPTION)

        except Exception as error:
            exc_info = sys.exc_info()
            if concurrent.is_future(result):
                error = result.exception()
                exc_info = result.exc_info()
            self.log_exception('Exception processing delivery %s: %s',
                               message_in.delivery_tag, str(error),
                               exc_info=exc_info)
            self._measurement.set_tag('exception', 'UnhandledException')
            raise gen.Return(data.UNHANDLED_EXCEPTION)

        if not self._finished:
            self.finish()
        self.logger.debug('Post finish')
        raise gen.Return(data.MESSAGE_ACK)

    def log_exception(self, msg_format, *args, **kwargs):
        """Customize the logging of uncaught exceptions.

        :param str msg_format: format of msg to log with ``self.logger.error``
        :param args: positional arguments to pass to ``self.logger.error``
        :param kwargs: keyword args to pass into ``self.logger.error``
        :keyword bool send_to_sentry: if omitted or *truthy*, this keyword
            will send the captured exception to Sentry (if enabled).

        This for internal use and should not be extended or used directly.

        By default, this method will log the message using
        :meth:`logging.Logger.error` and send the exception to Sentry.
        If an exception is currently active, then the traceback will be
        logged at the debug level.

        """
        self.logger.error(msg_format, *args)
        exc_info = kwargs.get('exc_info', sys.exc_info())
        if all(exc_info):
            exc_type, exc_value, tb = exc_info
            exc_name = exc_type.__name__
            self.logger.exception('Processor handled %s: %s', exc_name,
                                  exc_value, exc_info=exc_info)
        self._process.send_exception_to_sentry(exc_info)

    def on_confirmation(self, name, delivered, delivery_tag):
        """Called when a message is confirmed by RabbitMQ.

        This for internal use and should not be extended or used directly.

        .. todo:: integrate this with message publishing

        :param str name: The RabbitMQ connection that confirmed the delivery
        :param bool delivered: Was the message was successfully delivered
        :param str delivery_tag: The delivery tag for the message

        """
        pass

    def require_setting(self, name, feature='this feature'):
        """Raises an exception if the given app setting is not defined.

        This for internal use and should not be extended or used directly.

        :param str name: The parameter name
        :param str feature: A friendly name for the setting feature

        """
        if name not in self.settings:
            raise Exception("You must define the '%s' setting in your "
                            "application to use %s" % (name, feature))

    def set_channel(self, name, channel):
        """Assign the _channel attribute to the channel that was passed in.

        This for internal use and should not be extended or used directly.

        :param str name: The channel connection name
        :param channel: The channel to assign
        :type channel: :class:`pika.channel.Channel`

        """
        self._channels[name] = channel

    @property
    def _channel(self):
        """Return the channel of the message that is currently being processed.

        :rtype: :class:`pika.channel.Channel`

        """
        if not self._message:
            return None
        return self._message.channel

    def _clear(self):
        """Resets all assigned data for the current message."""
        self._finished = False
        self._message = None
        self._message_body = None

    @staticmethod
    def _get_pika_properties(properties_in):
        """Return a :class:`pika.spec.BasicProperties` object for a
        :class:`rejected.data.Properties` object.

        :param dict properties_in: Properties to convert
        :rtype: :class:`pika.spec.BasicProperties`

        """
        properties = pika.BasicProperties()
        for key in properties_in or {}:
            if properties_in.get(key) is not None:
                setattr(properties, key, properties_in.get(key))
        return properties

    def _publish_channel(self, name=None):
        """Return the channel to publish onm optionally specifying the channel
        name to use.

        :param str name:
        :rtype: pika.channel.Channel

        """
        if not name:
            return self._message.channel
        try:
            return self._channels[name]
        except KeyError:
            raise ValueError('Channel {} not found'.format(name))

    def _republish_dropped_message(self, reason):
        """Republish the original message that was received it is being dropped
        by the consumer.

        This for internal use and should not be extended or used directly.

        :param str reason: The reason the message was dropped

        """
        self.logger.debug('Republishing due to ProcessingException')
        properties = dict(self._message.properties) or {}
        if 'headers' not in properties or not properties['headers']:
            properties['headers'] = {}
        properties['headers']['X-Dropped-By'] = self.name
        properties['headers']['X-Dropped-Reason'] = reason
        properties['headers']['X-Dropped-Timestamp'] = \
            datetime.datetime.utcnow().isoformat()
        properties['headers']['X-Original-Exchange'] = self._message.exchange

        self._message.channel.basic_publish(
            self._drop_exchange,
            self._message.routing_key,
            self._message.body,
            pika.BasicProperties(**properties))

    def _republish_processing_error(self, error):
        """Republish the original message that was received because a
        :exc:`~rejected.consumer.ProcessingException` was raised.

        This for internal use and should not be extended or used directly.

        Add a header that keeps track of how many times this has happened
        for this message.

        :param str error: The string value for the exception

        """
        self.logger.debug('Republishing due to ProcessingException')
        properties = dict(self._message.properties) or {}
        if 'headers' not in properties or not properties['headers']:
            properties['headers'] = {}

        if error:
            properties['headers']['X-Processing-Exception'] = error

        if _PROCESSING_EXCEPTIONS not in properties['headers']:
            properties['headers'][_PROCESSING_EXCEPTIONS] = 1
        else:
            try:
                properties['headers'][_PROCESSING_EXCEPTIONS] += 1
            except TypeError:
                properties['headers'][_PROCESSING_EXCEPTIONS] = 1

        self._message.channel.basic_publish(
            self._error_exchange,
            self._message.routing_key,
            self._message.body,
            pika.BasicProperties(**properties))


class PublishingConsumer(Consumer):
    """Deprecated, functionality moved to :class:`rejected.consumer.Consumer`

    .. deprecated:: 3.17.0

    """
    def __init__(self, *args, **kwargs):
        warnings.warn('PublishingConsumer deprecated, all functionality moved'
                      'to Consumer', category=DeprecationWarning)
        super(PublishingConsumer, self).__init__(*args, **kwargs)


class SmartConsumer(Consumer):
    """Base class to ease the implementation of strongly typed message
    consumers that validate and automatically decode and deserialize the
    inbound message body based upon the message properties. Additionally,
    should one of the supported ``content_encoding`` types (``gzip`` or
    ``bzip2``) be specified in the message's property, it will automatically
    be decoded.

    When publishing a message, the message can be automatically serialized
    and encoded. If the ``content_type`` property is specified, the consumer
    will attempt to automatically serialize the message body. If the
    ``content_encoding`` property is specified using a supported encoding
    (``gzip`` or ``bzip2``), it will automatically be encoded as well.

    *Supported MIME types for automatic serialization and deserialization are:*

     - application/json
     - application/pickle
     - application/x-pickle
     - application/x-plist
     - application/x-vnd.python.pickle
     - application/vnd.python.pickle
     - text/csv
     - text/html (with beautifulsoup4 installed)
     - text/xml (with beautifulsoup4 installed)
     - text/yaml
     - text/x-yaml

    In any of the consumer base classes, if the ``MESSAGE_TYPE`` attribute is
    set, the ``type`` property of incoming messages will be validated against
    when a message is received, checking for string equality against the
    ``MESSAGE_TYPE`` attribute. If they are not matched, the consumer will not
    process the message and will drop the message without an exception if the
    ``DROP_INVALID_MESSAGES`` attribute is set to ``True``. If it is ``False``,
    a :py:class:`ConsumerException` is raised.

    .. note:: Since 3.17, :class:`~rejected.consumer.SmartConsumer` and
        :class:`~rejected.consumer.SmartPublishingConsumer` have been combined
        into the same class.

    """
    def publish_message(self, exchange, routing_key, properties, body,
                        no_serialization=False,
                        no_encoding=False,
                        channel=None):
        """Publish a message to RabbitMQ on the same channel the original
        message was received on.

        By default, if you pass a non-string object to the body and the
        properties have a supported content-type set, the body will be
        auto-serialized in the specified content-type.

        If the properties do not have a timestamp set, it will be set to the
        current time.

        If you specify a content-encoding in the properties and the encoding is
        supported, the body will be auto-encoded.

        Both of these behaviors can be disabled by setting no_serialization or
        no_encoding to True.

        :param str exchange: The exchange to publish to
        :param str routing_key: The routing key to publish with
        :param dict properties: The message properties
        :param mixed body: The message body to publish
        :param bool no_serialization: Turn off auto-serialization of the body
        :param bool no_encoding: Turn off auto-encoding of the body
        :param str channel: The channel/connection name to use. If it is not
            specified, the channel that the message was delivered on is used.

        """
        # Auto-serialize the content if needed
        is_string = (isinstance(body, str) or
                     isinstance(body, bytes) or
                     isinstance(body, unicode))
        if (not no_serialization and not is_string and
                properties.get('content_type')):
            self.logger.debug('Auto-serializing message body')
            body = self._auto_serialize(properties.get('content_type'), body)

        # Auto-encode the message body if needed
        if not no_encoding and properties.get('content_encoding'):
            self.logger.debug('Auto-encoding message body')
            body = self._auto_encode(properties.get('content_encoding'), body)

        # Publish the message
        self.logger.debug('Publishing message to %s:%s', exchange, routing_key)
        self._publish_channel(channel).basic_publish(
            exchange=exchange, routing_key=routing_key,
            properties=self._get_pika_properties(properties), body=body)

    @property
    def body(self):
        """Return the message body, unencoded if needed,
        deserialized if possible.

        :rtype: any

        """
        # Return a materialized view of the body if it has been previously set
        if self._message_body:
            return self._message_body

        # Handle bzip2 compressed content
        elif self.content_encoding == 'bzip2':
            self._message_body = self._decode_bz2(self._message.body)

        # Handle zlib compressed content
        elif self.content_encoding == 'gzip':
            self._message_body = self._decode_gzip(self._message.body)

        # Else we want to assign self._message.body to self._message_body
        else:
            self._message_body = self._message.body

        # Handle the auto-deserialization
        if self.content_type == 'application/json':
            self._message_body = self._load_json_value(self._message_body)

        elif umsgpack and self.content_type == 'application/msgpack':
            self._message_body = self._load_msgpack_value(self._message_body)

        elif self.content_type in PICKLE_MIME_TYPES:
            self._message_body = self._load_pickle_value(self._message_body)

        elif self.content_type == 'application/x-plist':
            self._message_body = self._load_plist_value(self._message_body)

        elif self.content_type == 'text/csv':
            self._message_body = self._load_csv_value(self._message_body)

        elif bs4 and self.content_type in BS4_MIME_TYPES:
            self._message_body = self._load_bs4_value(self._message_body)

        elif self.content_type in YAML_MIME_TYPES:
            self._message_body = self._load_yaml_value(self._message_body)

        # Return the message body
        return self._message_body

    def _auto_encode(self, content_encoding, value):
        """Based upon the value of the content_encoding, encode the value.

        :param str content_encoding: The content encoding type (gzip, bzip2)
        :param str value: The value to encode
        :rtype: value

        """
        if content_encoding == 'gzip':
            return self._encode_gzip(value)

        if content_encoding == 'bzip2':
            return self._encode_bz2(value)

        self.logger.warning(
            'Invalid content-encoding specified for auto-encoding')
        return value

    def _auto_serialize(self, content_type, value):
        """Auto-serialization of the value based upon the content-type value.

        :param str content_type: The content type to serialize
        :param any value: The value to serialize
        :rtype: str

        """
        if content_type == 'application/json':
            self.logger.debug('Auto-serializing content as JSON')
            return self._dump_json_value(value)

        elif umsgpack and content_type == 'application/msgpack':
            self.logger.debug('Auto-serializing content as msgpack')
            return self._dump_msgpack_value(value)

        elif content_type in PICKLE_MIME_TYPES:
            self.logger.debug('Auto-serializing content as Pickle')
            return self._dump_pickle_value(value)

        elif content_type == 'application/x-plist':
            self.logger.debug('Auto-serializing content as plist')
            return self._dump_plist_value(value)

        elif content_type == 'text/csv':
            self.logger.debug('Auto-serializing content as csv')
            return self._dump_csv_value(value)

        # If it's XML or HTML auto
        elif (bs4 and isinstance(value, bs4.BeautifulSoup) and
              content_type in ('text/html', 'text/xml')):
            self.logger.debug('Dumping BS4 object into HTML or XML')
            return self._dump_bs4_value(value)

        # If it's YAML, load the content via pyyaml into a dict
        elif self.content_type in YAML_MIME_TYPES:
            self.logger.debug('Auto-serializing content as YAML')
            return self._dump_yaml_value(value)

        self.logger.warning(
            'Invalid content-type specified for auto-serialization')
        return value

    @staticmethod
    def _decode_bz2(value):
        """Return a bz2 decompressed value

        :param bytes value: Compressed value
        :rtype: str

        """
        return bz2.decompress(value)

    @staticmethod
    def _decode_gzip(value):
        """Return a zlib decompressed value

        :param bytes value: Compressed value
        :rtype: str

        """
        return zlib.decompress(value)

    @staticmethod
    def _dump_bs4_value(value):
        """Return a BeautifulSoup object as a string

        :param bs4.BeautifulSoup value: The object to return a string from
        :rtype: str

        """
        return str(value)

    @staticmethod
    def _dump_csv_value(value):
        """Take a list of lists and return it as a CSV value

        :param list value: A list of lists to return as a CSV
        :rtype: str

        """
        buff = io.StringIO()
        writer = csv.writer(buff, quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerows(value)
        buff.seek(0)
        value = buff.read()
        buff.close()
        return value

    @staticmethod
    def _dump_json_value(value):
        """Serialize a value into JSON

        :param object value: The value to serialize
        :rtype: bytes

        """
        return json.dumps(value, ensure_ascii=True).encode('utf-8')

    @staticmethod
    def _dump_msgpack_value(value):
        """Serialize a value into MessagePack

        :param object value: The value to serialize
        :type value: str or dict or list
        :rtype: bytes

        """
        return umsgpack.packb(value)

    @staticmethod
    def _dump_pickle_value(value):
        """Serialize a value into the pickle format

        :param object value: The object to pickle
        :rtype: bytes

        """
        return pickle.dumps(value)

    @staticmethod
    def _dump_plist_value(value):
        """Create a plist value from a dictionary

        :param dict value: The value to make the plist from
        :rtype: bytes

        """
        if hasattr(plistlib, 'dumps'):
            return plistlib.dumps(value)
        try:
            return plistlib.writePlistToString(value).encode('utf-8')
        except AttributeError:
            return plistlib.writePlistToBytes(value)

    @staticmethod
    def _dump_yaml_value(value):
        """Dump an object into a YAML string

        :param object value: The value to dump as a YAML string
        :rtype: str

        """
        return yaml.dump(value)

    @staticmethod
    def _encode_bz2(value):
        """Return a bzip2 compressed value

        :param str value: Uncompressed value
        :rtype: bytes

        """
        if not isinstance(value, bytes):
            value = value.encode('utf-8')
        return bz2.compress(value)

    @staticmethod
    def _encode_gzip(value):
        """Return zlib compressed value

        :param str value: Uncompressed value
        :rtype: bytes

        """
        if not isinstance(value, bytes):
            value = value.encode('utf-8')
        return zlib.compress(value)

    @staticmethod
    def _load_bs4_value(value):
        """Load an HTML or XML string into an lxml etree object.

        :param str value: The HTML or XML string
        :rtype: bs4.BeautifulSoup
        :raises: ConsumerException

        """
        if not bs4:
            raise ConsumerException('BeautifulSoup4 is not enabled')
        if isinstance(value, bytes):
            value = value.decode('utf-8')
        return bs4.BeautifulSoup(value)

    @staticmethod
    def _load_csv_value(value):
        """Create a csv.DictReader instance for the sniffed dialect for the
        value passed in.

        :param str value: The CSV value
        :rtype: csv.DictReader

        """
        if isinstance(value, bytes):
            value = value.decode('utf-8')
        csv_buffer = io.StringIO(value)
        dialect = csv.Sniffer().sniff(csv_buffer.read(1024))
        csv_buffer.seek(0)
        return csv.DictReader(csv_buffer, dialect=dialect)

    def _load_json_value(self, value):
        """Deserialize a JSON string returning the native Python data type
        for the value.

        :param str value: The JSON string
        :rtype: object

        """
        if isinstance(value, bytes):
            value = value.decode('utf-8')
        try:
            return json.loads(value, encoding='utf-8')
        except ValueError as error:
            self.logger.exception('Could not decode message body: %s', error)
            raise MessageException(error)

    def _load_msgpack_value(self, value):
        """Deserialize a msgpack string returning the native Python data type
        for the value.

        :param str value: The msgpack string
        :rtype: object

        """
        try:
            return umsgpack.unpackb(value)
        except ValueError as error:
            self.logger.exception('Could not decode message body: %s', error)
            raise MessageException(error)

    @staticmethod
    def _load_pickle_value(value):
        """Deserialize a pickle string returning the native Python data type
        for the value.

        :param bytes value: The pickle string
        :rtype: object

        """
        return pickle.loads(value)

    @staticmethod
    def _load_plist_value(value):
        """Deserialize a plist string returning the native Python data type
        for the value.

        :param bytes value: The pickle string
        :rtype: dict

        """
        if hasattr(plistlib, 'loads'):
            return plistlib.loads(value)
        try:
            return plistlib.readPlistFromString(value)
        except AttributeError:
            return plistlib.readPlistFromBytes(value)

    @staticmethod
    def _load_yaml_value(value):
        """Load an YAML string into an dict object.

        :param str value: The YAML string
        :rtype: any
        :raises: ConsumerException

        """
        return yaml.load(value)


class SmartPublishingConsumer(SmartConsumer):
    """Deprecated, functionality moved to
    :class:`rejected.consumer.SmartConsumer`

        .. deprecated:: 3.17.0

    """

    def __init__(self, *args, **kwargs):
        warnings.warn('SmartPublishingConsumer deprecated, all functionality '
                      'moved to SmartConsumer', category=DeprecationWarning)
        super(SmartPublishingConsumer, self).__init__(*args, **kwargs)


class RejectedException(Exception):
    """Base exception for :py:class:`~rejected.consumer.Consumer` related
    exceptions.

    If provided, the metric will be used to automatically record exception
    metric counts using the path
    `[prefix].[consumer-name].exceptions.[exception-type].[metric]`.

    Positional and keyword arguments are used to format the value that is
    passed in when providing the string value of the exception.

    :param str value: An optional value used in string representation
    :param str metric: An optional value for auto-instrumentation of exceptions

    .. versionadded:: 3.19.0

    """
    METRIC_NAME = 'rejected-exception'

    def __init__(self, *args, **kwargs):
        if len(args) > 1:
            self.args = args[1:] if 'value' not in kwargs else args
        else:
            self.args = args
        self.metric = kwargs.pop('metric', None)
        self.value = kwargs.pop('value', '{!r} {!r}' if not args else args[0])
        self.kwargs = kwargs

    def __str__(self):
        if not self.args and not self.kwargs:
            return repr(self)
        return self.value.format(*self.args, **self.kwargs)

    def __repr__(self):
        if not self.args and not self.kwargs:
            return '{}()'.format(self.__class__.__name__)
        return '{}({})'.format(self.__class__.__name__, str(self))


class ConsumerException(RejectedException):
    """May be called when processing a message to indicate a problem that the
    Consumer may be experiencing that should cause it to stop.

    :param str value: An optional value used in string representation
    :param str metric: An optional value for auto-instrumentation of exceptions

    """
    def __init__(self, *args, **kwargs):
        super(ConsumerException, self).__init__(*args, **kwargs)


class MessageException(RejectedException):
    """Invoke when a message should be rejected and not re-queued, but not due
    to a processing error that should cause the consumer to stop.

    :param str value: An optional value used in string representation
    :param str metric: An optional value for auto-instrumentation of exceptions

    """
    def __init__(self, *args, **kwargs):
        super(MessageException, self).__init__(*args, **kwargs)


class ProcessingException(RejectedException):
    """Invoke when a message should be rejected and not re-queued, but not due
    to a processing error that should cause the consumer to stop. This should
    be used for when you want to reject a message which will be republished to
    a retry queue, without anything being stated about the exception.

    :param str value: An optional value used in string representation
    :param str metric: An optional value for auto-instrumentation of exceptions

    """
    def __init__(self, *args, **kwargs):
        super(ProcessingException, self).__init__(*args, **kwargs)
