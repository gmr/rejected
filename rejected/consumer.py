"""
The :class:`Consumer` class provides all the structure required for
implementing a rejected consumer.

"""
import contextlib
import datetime
import logging
import sys
import time
import uuid

import pika
from tornado import concurrent, gen, locks

from rejected import data, log, utils
from rejected.errors import *

_DEFAULT_CHANNEL = 'default'
_DROPPED_MESSAGE = 'X-Rejected-Dropped'
_PROCESSING_EXCEPTIONS = 'X-Processing-Exceptions'
_EXCEPTION_FROM = 'X-Exception-From'


class Consumer(object):
    """Base consumer class that defines the contract between rejected and
    consumer applications. You must extend the
    :meth:`~rejected.consumer.Consumer.process` method in your child class
    to properly implement a consumer application. All other methods are
    optional.

    """
    MESSAGE_TYPE = None
    """A value or list of values that are compared against the AMQP `type`
    message property to determine if the consumer can support the
    processing of the received message. If not specified, type checking is
    not performed. Use in conjunction with
    :const:`~rejected.consumer.Consumer.DROP_INVALID_MESSAGES`.

    :default: :data:`None`
    :type: :class:`str` or 
           :class:`list` ( :class:`str` ) or 
           :class:`set` ( :class:`str` ) or
           :class:`tuple` ( :class:`str` ) or
           :data:`None`
    """

    DROP_INVALID_MESSAGES = False
    """Set to :class:`True` to automatically drop messages that do not match a
    supported message type as defined in
    :const:`~rejected.consumer.Consumer.MESSAGE_TYPE`.

    :default: :data:`False`
    :type: :class:`bool`
    """

    DROP_EXCHANGE = None
    """Assign an exchange to publish dropped messages to. If set to
    :data:`None`, dropped messages are not republished.

    :default: :data:`None`
    :type: :class:`str` or :data:`None`
    """

    ERROR_MAX_RETRIES = None
    """Assign an integer value to limit the number of times a
    :exc:`~rejected.consumer.ProcessingException` is raised for a message
    before it is dropped. `None` disables the dropping of messages due to
    :exc:`~rejected.consumer.ProcessingException`.
    Replaces the deprecated :const:`ERROR_MAX_RETRY` attribute.

    .. versionadded:: 4.0.0

    :default: :data:`None`
    :type: :class:`int` or :data:`None`
    """

    ERROR_MAX_RETRY = None
    """Assign an integer value to limit the number of times a
    :exc:`~rejected.consumer.ProcessingException` is raised for a message
    before it is dropped. `None` disables the dropping of messages due to
    :exc:`~rejected.consumer.ProcessingException`.
    Deprecated by the :const:`ERROR_MAX_RETRIES` attribute.

    .. deprecated:: 4.0.0

    :default: :data:`None`
    :type: :class:`int` or :data:`None`
    """

    ERROR_EXCHANGE = 'errors'
    """The exchange a message will be published to if a
    :exc:`~rejected.consumer.ProcessingException` is raised.

    :default: ``errors``
    :type: :class:`str`
    """

    IGNORE_OOB_STATS = False
    """Suppress warnings when calls to the stats methods are made while no
    message is currently being processed.

    .. versionadded:: 4.0.0

    :default: :data:`False`
    :type: :class:`bool`
    """

    MESSAGE_AGE_KEY = 'message_age'
    """Specify a value that is used for the automatic recording of per-message
    statistics for the message age. You can also override the
    :meth:`~rejected.consumer.Consumer.message_age_key` method to create
    compound keys. For example, to create a key that includes the message
    priority:

    .. code-block:: python

        class Consumer(consumer.Consumer):

            def message_age_key(self):
                return 'priority-{}.message_age'.format(self.priority or 0)

    .. versionadded:: 3.18.6

    :default: ``message_age``
    :type: :class:`str`
    """

    def __init__(self, *args, **kwargs):
        """Creates a new instance of the :class:`~rejected.consumer.Consumer`
        class.

        """
        self._confirmation_futures = {}
        self._connections = {}
        self._correlation_id = None
        self._drop_exchange = kwargs.get('drop_exchange') or self.DROP_EXCHANGE
        self._drop_invalid = (kwargs.get('drop_invalid_messages')
                              or self.DROP_INVALID_MESSAGES)
        self._error_exchange = (kwargs.get('error_exchange')
                                or self.ERROR_EXCHANGE)
        self._error_max_retry = (kwargs.get('error_max_retry')
                                 or self.ERROR_MAX_RETRIES
                                 or self.ERROR_MAX_RETRY)
        self._finished = False
        self._message = None
        self._message_type = kwargs.get('message_type') or self.MESSAGE_TYPE
        self._measurement = None
        self._message_body = None
        self._process = kwargs['process']
        self._settings = kwargs['settings']
        self._yield_condition = locks.Condition()

        # Create a logger that attaches correlation ID to the record
        logger = logging.getLogger(
            kwargs.get('settings').get('_import_module', __name__))
        self.logger = log.CorrelationIDAdapter(logger, {'parent': self})

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
        :meth:`~rejected.consumer.Consumer.process`. One use for extending
        this method is to pre-process messages and isolate logic that
        validates messages and rejected them if data is missing.

        .. note:: Asynchronous support: Decorate this method with
            :func:`tornado.gen.coroutine` to make it asynchronous.

        If this method returns a :class:`~tornado.concurrent.Future`, execution
        will not proceed until the Future has completed.

        :raises: :exc:`rejected.errors.ConsumerException`
        :raises: :exc:`rejected.errors.MessageException`
        :raises: :exc:`rejected.errors.ProcessingException`

        """
        pass

    def process(self):
        """Implement this method for the primary, top-level message processing
        logic for your consumer.

        If the message can not be processed and the Consumer should stop after
        ``N`` failures to process messages, raise the
        :exc:`~rejected.errors.ConsumerException`.

        .. note:: Asynchronous support: Decorate this method with
            :func:`tornado.gen.coroutine` to make it asynchronous.

        :raises: :exc:`rejected.errors.ConsumerException`
        :raises: :exc:`rejected.errors.MessageException`
        :raises: :exc:`rejected.errors.ProcessingException`

        """
        raise NotImplementedError

    def message_age_key(self):
        """Return the key part that is used in submitting message age stats.
        Override this method to change the key part. This could be used to
        include message priority in the key, for example.

        .. versionadded:: 3.18.6

        :rtype: :class:`str`

        """
        return self.MESSAGE_AGE_KEY

    def on_finish(self, exc=None):
        """Called after a message has been processed.  Override this method to
        perform cleanup, logging, etc.

        If an exception is raised during the processing of a message, the
        exception instance is passed into this method.

        If a message is dropped due to setting
        :attr:`~rejected.consumer.Consumer.DROP_INVALID_MESSAGES` to
        :data:`True` or :attr:`~rejected.consumer.Consumer.ERROR_MAX_RETRY`,
        this method will not be invoked.

        .. versionchanged:: 4.0.0

        .. note:: Asynchronous support: Decorate this method with
            :func:`tornado.gen.coroutine` to make it asynchronous.

        :param exc: The instance of the exception raised during processing
            if an exception is raised.
        :type exc:
            :exc:`rejected.errors.ExecutionFinished` or
            :exc:`rejected.errors.ConsumerException` or
            :exc:`rejected.errors.MessageException` or
            :exc:`rejected.errors.ProcessingException` or
            :exc:`rejected.errors.RabbitMQException` or
            :exc:`Exception` or
            :data:`None`

        """
        pass

    def on_blocked(self, name):
        """Called when a connection for this consumer is blocked.

        Implement this method to respond to being blocked.

        .. versionadded:: 3.17

        :param str name: The connection name that is blocked

        """
        pass

    def on_unblocked(self, name):
        """Called when a connection for this consumer is unblocked.

        Implement this method to respond to being blocked.

        .. versionadded:: 3.17

        :param str name: The connection name that is blocked

        """
        pass

    def shutdown(self):
        """Implement to cleanly shutdown your application code when rejected is
        stopping the consumer.

        This could be used for closing database connections or other such
        activities.

        """
        pass

    """Quick-access properties"""

    @property
    def app_id(self):
        """Access the current message's ``app-id`` property as an attribute of
        the consumer class.

        :rtype: :class:`str` or :data:`None`

        """
        return self._message.properties.app_id if self._message else None

    @property
    def body(self):
        """Access the opaque body from the current message.

        :rtype: :class:`bytes` or :data:`None`

        """
        return self._message.body if self._message else None

    @property
    def content_encoding(self):
        """Access the current message's ``content-encoding`` AMQP message
        property as an attribute of the consumer class.

        :rtype: :class:`str` or :data:`None`

        """
        return self._message.properties.content_encoding \
            if self._message else None

    @property
    def content_type(self):
        """Access the current message's ``content-type`` AMQP message property
        as an attribute of the consumer class.

        :rtype: :class:`str` or :data:`None`

        """
        return self._message.properties.content_type if self._message else None

    @property
    def correlation_id(self):
        """Access the current message's ``correlation-id`` AMAP message
        property as an attribute of the consumer class. If the message does not
        have a ``correlation-id`` then, each message is assigned a new UUIDv4
        based ``correlation-id`` value.

        :rtype: :class:`str` or :data:`None`

        """
        return self._correlation_id if self._message else None

    @property
    def exchange(self):
        """Access the AMQP exchange the message was published to as an
        attribute of the consumer class.

        :rtype: :class:`str` or :data:`None`

        """
        return self._message.exchange if self._message else None

    @property
    def expiration(self):
        """Access the current message's ``expiration`` AMQP message property as
        an attribute of the consumer class.

        :rtype: :class:`str` or :data:`None`

        """
        return self._message.properties.expiration if self._message else None

    @property
    def headers(self):
        """Access the current message's ``headers`` AMQP message property as an
        attribute of the consumer class.

        :rtype: :class:`dict`

        """
        return self._message.properties.headers or {} if self._message else {}

    @property
    def is_finished(self):
        """Returns a boolean indicating if the consumer has finished processing
        the current message.

        .. versionadded:: 4.0.0

        :rtype: :class:`bool`

        """
        return self._finished

    @property
    def io_loop(self):
        """Access the :class:`tornado.ioloop.IOLoop` instance for the
        current message.

        .. versionadded:: 3.18.4

        :rtype: :class:`tornado.ioloop.IOLoop` or :data:`None`

        """
        if self._message and self._message.connection:
            return self._connections[self._message.connection].io_loop

    @property
    def message_id(self):
        """Access the current message's ``message-id`` AMQP message property as
        an attribute of the consumer class.

        :rtype: :class:`str` or :data:`None`

        """
        return self._message.properties.message_id if self._message else None

    @property
    def measurement(self):
        """Access the current message's :class:`rejected.data.Measurement`
        instance.

        .. versionadded:: 4.0.0

        :rtype: :class:`rejected.data.Measurement`

        """
        return self._measurement

    @property
    def name(self):
        """Property returning the name of the consumer class.

        :rtype: :class:`str`

        """
        return self.__class__.__name__

    @property
    def priority(self):
        """Access the current message's ``priority`` AMQP message property as
        an attribute of the consumer class.

        :rtype: :class:`int` or :data:`None`

        """
        return self._message.properties.priority if self._message else None

    @property
    def properties(self):
        """Access the current message's AMQP message properties in dict form as
        an attribute of the consumer class.

        :rtype: :class:`dict` or :data:`None`

        """
        return dict(self._message.properties) if self._message else None

    @property
    def redelivered(self):
        """Indicates if the current message has been redelivered.

        :rtype: :class:`bool` or :data:`None`

        """
        return self._message.redelivered if self._message else None

    @property
    def reply_to(self):
        """Access the current message's ``reply-to`` AMQP message property as
        an attribute of the consumer class.

        :rtype: :class:`str` or :data:`None`

        """
        return self._message.properties.reply_to if self._message else None

    @property
    def routing_key(self):
        """Access the routing key for the current message.

        :rtype: :class:`str` or :data:`None`

        """
        return self._message.routing_key if self._message else None

    @property
    def message_type(self):
        """Access the current message's ``type`` AMQP message property as an
        attribute of the consumer class.

        :rtype: :class:`str` or :data:`None`

        """
        return self._message.properties.type if self._message else None

    @property
    def sentry_client(self):
        """Access the Sentry raven :class:`~raven.Client` instance or
        :data:`None`

        Use this object to add tags or additional context to Sentry
        error reports (see :meth:`raven.Client.tags_context`) or
        to report messages (via :meth:`raven.Client.captureMessage`)
        directly to Sentry.

        :rtype: :class:`raven.Client` or :data:`None`

        """
        if hasattr(self._process, 'sentry_client'):
            return self._process.sentry_client

    @property
    def settings(self):
        """Access the consumer settings as specified by the ``config`` section
        for the consumer in the rejected configuration.

        :rtype: :class:`dict`

        """
        return self._settings

    @property
    def timestamp(self):
        """Access the unix epoch timestamp value from the AMQP message
        properties of the current message.

        :rtype: :class:`int` or :data:`None`

        """
        return self._message.properties.timestamp if self._message else None

    @property
    def user_id(self):
        """Access the ``user-id`` AMQP message property from the current
        message's properties.

        :rtype: :class:`str` or :data:`None`

        """
        return self._message.properties.user_id if self._message else None

    """Utility Methods for use by Consumer Code"""

    def require_setting(self, name, feature='this feature'):
        """Raises an exception if the given app setting is not defined.

        As a generalization, this method should called from a Consumer's
        :py:meth:`~rejected.consumer.Consumer.initialize` method. If a required
        setting is not found, this method will cause the
        consumer to shutdown prior to receiving any messages from RabbitMQ.

        :param name: The parameter name
        :type name: :class:`str`
        :param feature: A friendly name for the setting feature
        :type feature: :class:`str`
        :raises: :exc:`~rejected.errors.ConfigurationException`

        """
        if name not in self.settings:
            raise ConfigurationException(
                "You must define the '{}' setting in your "
                "application to use {}".format(name, feature))

    def finish(self):
        """Finishes message processing for the current message. If this is
        called in :meth:`~rejected.consumer.Consumer.prepare`, the
        :meth:`~rejected.consumer.Consumer.process` method is not invoked
        for the current message.

        """
        if self._finished:
            self.logger.warning('Finished called when already finished')
            return
        raise ExecutionFinished

    def publish_message(self,
                        exchange,
                        routing_key,
                        properties,
                        body,
                        connection=None):
        """Publish a message to RabbitMQ on the same channel the original
        message was received on. If
        `publisher confirmations <https://www.rabbitmq.com/confirms.html>`_
        are enabled, the method will return a
        :class:`~tornado.concurrent.Future` that will resolve a :class:`bool`
        that indicates if the publishing was successful.

        .. versionchanged:: 4.0.0
            Return a :class:`~tornado.concurrent.Future` if
            `publisher confirmations <https://www.rabbitmq.com/confirms.html>`_
            are enabled. Removed the ``channel`` parameter.

        :param exchange: The exchange to publish to
        :type exchange: :class:`str`
        :param routing_key: The routing key to publish with
        :type routing_key: :class:`str`
        :param properties: The message properties
        :type properties: :class:`dict`
        :param body: The message body
        :type body: :class:`bytes` or :class:`str`
        :param connection: The connection to use. If it is not
            specified, the channel that the message was delivered on is used.
        :type connection: :class:`str`

        :rtype: :class:`tornado.concurrent.Future` or :data:`None`

        """
        conn = self._publish_connection(connection)
        self.logger.debug('Publishing message to %s:%s (%s)', exchange,
                          routing_key, conn.name)
        basic_properties = self._get_pika_properties(properties)
        with self._measurement.track_duration('publish.{}.{}'.format(
                exchange, routing_key)):
            conn.channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                properties=basic_properties,
                body=body,
                mandatory=conn.publisher_confirmations)
            return self._publisher_confirmation_future(
                conn.name, exchange, routing_key, basic_properties)

    def rpc_reply(self,
                  body,
                  properties=None,
                  exchange=None,
                  reply_to=None,
                  connection=None):
        """Reply to the message that is currently being processed.

        .. versionadded:: 4.0.0

        If the exchange is not specified, the exchange of the message that is
        currently being processed by the Consumer will be used.

        If ``reply_to`` is not provided, it will attempt to use the
        ``reply_to`` property of the message that is currently being processed
        by the consumer. If both are not set, a :exc:`ValueError` is raised.

        If any of the following message properties are not provided, they will
        automatically be assigned:

          - ``app_id``
          - ``correlation_id``
          - ``message_id``
          - ``timestamp``

        The ``correlation_id`` will only be automatically assigned if the
        original message provided a ``message_id``.

        If the connection that the message is being published on has publisher
        confirmations enabled, a :py:class:`~tornado.concurrent.Future` is
        returned.

        :param body: The message body
        :type body: :class:`bytes` or :class:`str`
        :param properties: The AMQP properties to use for the reply message
        :type properties: :class:`dict` or :data:`None`
        :param exchange: The exchange to publish to. Defaults to the exchange
            of the message that is currently being processed by the Consumer.
        :type exchange: :class:`str` or :data:`None`
        :param reply_to: The routing key to send the reply to. Defaults to the
            reply_to property of the message that is currently being processed
            by the Consumer. If neither are set, a :exc:`ValueError` is
            raised.
        :type reply_to: class:`str` or :data:`None`
        :param connection: The connection to use. If it is not
            specified, the channel that the message was delivered on is used.
        :type connection: :class:`str`
        :rtype: :class:`tornado.concurrent.Future` or :data:`None`
        :raises: :exc:`ValueError`

        """
        if reply_to is None and self.reply_to is None:
            raise ValueError('Missing reply_to routing key')
        properties = properties or {}
        if not properties.get('app_id'):
            properties['app_id'] = self.name
        if not properties.get('correlation_id') and self.message_id:
            properties['correlation_id'] = self.message_id
        if not properties.get('message_id'):
            properties['message_id'] = str(uuid.uuid4())
        if not properties.get('timestamp'):
            properties['timestamp'] = int(time.time())
        return self.publish_message(exchange or self.exchange, reply_to
                                    or self.reply_to, properties, body,
                                    connection)

    def send_exception_to_sentry(self, exc_info):
        """Send an exception to Sentry if enabled.

        :param exc_info: exception information as returned from
            :func:`sys.exc_info`
        :type exc_info: :class:`tuple`

        """
        self._process.send_exception_to_sentry(exc_info)

    def set_sentry_context(self, tag, value):
        """Set a context tag in Sentry for the given key and value.

        :param tag: The context tag name
        :type tag: :class:`str`
        :param value: The context value
        :type value: :class:`str`

        """
        if self.sentry_client:
            self.logger.debug('Setting sentry context for %s to %s', tag,
                              value)
            self.sentry_client.tags_context({tag: value})

    def stats_add_duration(self, key, duration):
        """Add a duration to the per-message measurements

        .. versionadded:: 3.19.0

        .. note:: If this method is called when there is not a message being
            processed, a message will be logged at the ``warning`` level to
            indicate the value is being dropped. To suppress these warnings,
            set the :attr:`~rejected.consumer.Consumer.IGNORE_OOB_STATS`
            attribute to :data:`True`.

        :param key: The key to add the timing to
        :type key: :class:`str`
        :param duration: The timing value in seconds
        :type duration: :class:`int` or :class:`float`

        """
        if not self._measurement:
            if not self.IGNORE_OOB_STATS:
                self.logger.warning(
                    'stats_add_timing invoked outside execution')
            return
        self._measurement.add_duration(key, duration)

    def stats_incr(self, key, value=1):
        """Increment the specified key in the per-message measurements

        .. versionadded:: 3.13.0

        .. note:: If this method is called when there is not a message being
            processed, a message will be logged at the ``warning`` level to
            indicate the value is being dropped. To suppress these warnings,
            set the :attr:`rejected.consumer.Consumer.IGNORE_OOB_STATS`
            attribute to :data:`True`.

        :param key: The key to increment
        :type key: :class:`str`
        :param value: The value to increment the key by
        :type value: :class:`int`

        """
        if not self._measurement:
            if not self.IGNORE_OOB_STATS:
                self.logger.warning('stats_incr invoked outside execution')
            return
        self._measurement.incr(key, value)

    def stats_set_tag(self, key, value=1):
        """Set the specified tag/value in the per-message measurements

        .. versionadded:: 3.13.0

        .. note:: If this method is called when there is not a message being
            processed, a message will be logged at the ``warning`` level to
            indicate the value is being dropped. To suppress these warnings,
            set the :attr:`rejected.consumer.Consumer.IGNORE_OOB_STATS`
            attribute to :data:`True`.

        :param key: The key to set
        :type key: :class:`str`
        :param value: The value to set
        :type value: :class:`int`

        """
        if not self._measurement:
            if not self.IGNORE_OOB_STATS:
                self.logger.warning('stats_set_tag invoked outside execution')
            return
        self._measurement.set_tag(key, value)

    def stats_set_value(self, key, value=1):
        """Set the specified key/value in the per-message measurements

        .. versionadded:: 3.13.0

        .. note:: If this method is called when there is not a message being
            processed, a message will be logged at the ``warning`` level to
            indicate the value is being dropped. To suppress these warnings,
            set the :attr:`rejected.consumer.Consumer.IGNORE_OOB_STATS`
            attribute to :data:`True`.

        :param key: The key to set the value for
        :type key: :class:`str`
        :param value: The value
        :type value: :class:`int` or :class:`float`

        """
        if not self._measurement:
            if not self.IGNORE_OOB_STATS:
                self.logger.warning(
                    'stats_set_value invoked outside execution')
            return
        self._measurement.set_value(key, value)

    @contextlib.contextmanager
    def stats_track_duration(self, key):
        """Time around a context and add to the the per-message measurements

        .. versionadded:: 3.19.0

        .. note:: If this method is called when there is not a message being
            processed, a message will be logged at the ``warning`` level to
            indicate the value is being dropped. To suppress these warnings,
            set the :attr:`rejected.consumer.Consumer.IGNORE_OOB_STATS`
            attribute to :data:`True`.


    .. code-block:: python
       :caption: Example Usage

       class Test(consumer.Consumer):

           @gen.coroutine
           def process(self):
               with self.stats_track_duration('track-time'):
                   yield self._time_consuming_function()

        :param key: The key for the timing to track
        :type key: :class:`str`

        """
        start_time = time.time()
        try:
            yield
        finally:
            self.stats_add_duration(
                key, max(start_time, time.time()) - start_time)

    def unset_sentry_context(self, tag):
        """Remove a context tag from sentry

        :param tag: The context tag to remove
        :type tag: :class:`str`

        """
        if self.sentry_client:
            self.sentry_client.tags.pop(tag, None)

    @gen.coroutine
    def yield_to_ioloop(self):
        """Function that will allow Rejected to process IOLoop events while
        in a tight-loop inside an asynchronous consumer.

        .. code-block:: python
           :caption: Example Usage

           class Consumer(consumer.Consumer):

               @gen.coroutine
               def process(self):
                   for iteration in range(0, 1000000):
                       yield self.yield_to_ioloop()

        """
        try:
            yield self._yield_condition.wait(
                self._message.channel.connection.ioloop.time() + 0.001)
        except gen.TimeoutError:
            pass

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

        """
        self.logger.debug('Received: %r', message_in)

        try:
            self._preprocess(message_in, measurement)
        except DropMessage:
            raise gen.Return(data.MESSAGE_DROP)
        except MessageException as exc:
            raise gen.Return(self._on_message_exception(exc))

        try:
            yield self._execute()
        except ExecutionFinished as exc:
            self.logger.debug('Execution finished early')
            if not self._finished:
                self.on_finish(exc)

        except RabbitMQException as exc:
            self._handle_exception(exc)
            raise gen.Return(data.RABBITMQ_EXCEPTION)

        except ConfigurationException as exc:
            self._handle_exception(exc)
            raise gen.Return(data.CONFIGURATION_EXCEPTION)

        except ConsumerException as exc:
            self._handle_exception(exc)
            raise gen.Return(data.CONSUMER_EXCEPTION)

        except MessageException as exc:
            raise gen.Return(self._on_message_exception(exc))

        except ProcessingException as exc:
            self._handle_exception(exc)
            self._republish_processing_error(
                exc.metric or exc.__class__.__name__)
            raise gen.Return(data.PROCESSING_EXCEPTION)

        except (NotImplementedError, Exception) as exc:
            self._handle_exception(exc)
            raise gen.Return(data.UNHANDLED_EXCEPTION)

        else:
            self._finished = True

        self._maybe_clear_confirmation_futures()
        raise gen.Return(data.MESSAGE_ACK)

    def remove_connection(self, name):
        """Remove the connection from the available connections for
        publishing.

        This for internal use and should not be extended or used directly.

        :param str name: The connection name

        """
        del self._connections[name]

    def set_connection(self, connection):
        """Assign the connection to the Consumer so that it may be used
        when requested.

        This for internal use and should not be extended or used directly.

        :param connection: The connection to assign
        :type connection  :class:`~rejected.process.Connection`

        """
        self._connections[connection.name] = connection

    @property
    def _channel(self):
        """Return the channel of the message that is currently being processed.

        :rtype: :class:`pika.channel.Channel`

        """
        return self._message.channel if self._message else None

    def _clear(self):
        """Resets all assigned data for the current message."""
        self._finished = False
        self._measurement = None
        self._message = None
        self._message_body = None

    @gen.coroutine
    def _execute(self):
        """Invoke the lifecycle methods for a message
        (prepare, process, on_finish), yielding if any are coroutines.

        """
        for call in (self.prepare, self.process, self.on_finish):
            self.logger.debug('Invoking %r', call)
            result = call()
            if concurrent.is_future(result):
                yield result

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

    def _handle_exception(self, exc):
        """Common exception handling behavior across all exceptions.

        .. note:: This for internal use and should not be extended or used
            directly.

        """
        exc_info = sys.exc_info()
        self.logger.exception(
            '%s while processing message #%s',
            exc.__class__.__name__, self._message.delivery_tag,
            exc_info=exc_info)
        self._measurement.set_tag('exception', exc.__class__.__name__)
        if hasattr(exc, 'metric') and exc.metric:
            self._measurement.set_tag('error', exc.metric)
        self._process.send_exception_to_sentry(exc_info)
        self._maybe_clear_confirmation_futures()
        if not self._finished:
            self.on_finish(exc)
        self._finished = True

    def _maybe_clear_confirmation_futures(self):
        """Invoked when the message has finished processing, ensuring there
        are no confirmation futures pending.

        """
        for name in self._connections.keys():
            self._connections[name].clear_confirmation_futures()

    def _maybe_drop_message(self):
        """Check to see if processing this message would exceed the configured
        error max retry value. If so, if a drop exchange is configured, publish
        to that. Finally, raise :exc:`DropMessage`.

        :raises: rejected.errors.DropMessage

        """
        if self.headers[_PROCESSING_EXCEPTIONS] >= self._error_max_retry:
            self.logger.warning('Dropping message with %i deaths due to '
                                'ERROR_MAX_RETRY',
                                self.headers[_PROCESSING_EXCEPTIONS])
            if self._drop_exchange:
                self._republish_dropped_message('max retries ({})'.format(
                    self.headers[_PROCESSING_EXCEPTIONS]))
            raise DropMessage

    def _maybe_set_message_age(self):
        """If timestamp is set and the relative age is > 0, record age of the
        message coming in

        """
        if self._message.properties.timestamp:
            message_age = float(
                max(self._message.properties.timestamp, time.time()) -
                self._message.properties.timestamp)
            if message_age > 0:
                self.measurement.add_duration(
                    self.message_age_key(), message_age)

    def _on_message_exception(self, exc):
        """Common code that is invoked when a :exc:`MessageException` is
        raised.

         :param rejected.errors.MessageException exc: The exception raised
         :rtype: int

         """
        self._handle_exception(exc)
        return data.MESSAGE_EXCEPTION

    def _preprocess(self, message_in, measurement):
        """Invoked at the start of execution, setting internal state,
        validating that the message should be processed and not dropped.

        :param message_in: The message to process
        :type message_in: :class:`rejected.data.Message`
        :param measurement: For collecting per-message instrumentation
        :type measurement: :class:`rejected.data.Measurement`
        :raises: rejected.errors.MessageException
        :raises: rejected.errors.DropMessage

        """
        self._clear()
        self._message = message_in
        self._measurement = measurement

        self._maybe_set_message_age()

        # Ensure there is a correlation ID
        self._correlation_id = (
                self._message.properties.correlation_id or
                self._message.properties.message_id or
                str(uuid.uuid4()))

        # Set the Correlation ID for the connection for logging
        self._connections[self._message.connection].correlation_id = \
            self._correlation_id

        # Set the sentry context for the message type if set
        if self.message_type:
            self.set_sentry_context('type', self.message_type)

        # Validate the message type if validation is turned on
        if self._message_type:
            self._validate_message_type()

        # Check the number of ProcessingErrors and possibly drop the message
        if self._error_max_retry and _PROCESSING_EXCEPTIONS in self.headers:
            self._maybe_drop_message()

    def _publisher_confirmation_future(self, name, exchange, routing_key,
                                       properties):
        """Return a future a publisher confirmation result that enables
        consumers to block on the confirmation of a published message.

        Two internal dicts are used for keeping track of state.
        Consumer._delivery_tags is a dict of connection names that keeps
        the last delivery tag expectation and is used to correlate the future
        with the delivery tag that is expected to be confirmed from RabbitMQ.

        This for internal use and should not be extended or used directly.

        :param str name: The connection name for the future
        :param str exchange: The exchange the message was published to
        :param str routing_key: The routing key that was used
        :param properties: The AMQP message properties for the delivery
        :type properties: pika.spec.Basic.Properties
        :rtype: concurrent.Future.

        """
        if self._connections[name].publisher_confirmations:
            future = concurrent.Future()
            self._connections[name].add_confirmation_future(
                exchange, routing_key, properties, future)
            return future

    def _publish_connection(self, name=None):
        """Return the connection to publish. If the name is not specified,
        the connection associated with the current message is returned.

        :param str name:
        :rtype: rejected.process.Connection

        """
        try:
            conn = self._connections[name or self._message.connection]
        except KeyError:
            raise ValueError('Channel {} not found'.format(name))
        if not conn.is_connected or conn.channel.is_closed:
            raise RabbitMQException(conn.name, 599, 'NOT_CONNECTED')
        return conn

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
            self._drop_exchange, self._message.routing_key, self._message.body,
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
            self._error_exchange, self._message.routing_key,
            self._message.body, pika.BasicProperties(**properties))

    def _unsupported_message_type(self):
        """Check if the current message matches the configured message type(s).

        :rtype: bool

        """
        if isinstance(self._message_type, (tuple, list, set)):
            return self.message_type not in self._message_type
        return self.message_type != self._message_type

    def _validate_message_type(self):
        """Check to see if the current message's AMQP type property is
        supported and if not, raise either a :exc:`DropMessage` or
        :exc:`MessageException`.

        :raises: DropMessage
        :raises: MessageException

        """
        if self._unsupported_message_type():
            self.logger.warning(
                'Received unsupported message type: %s', self.message_type)
            if self._drop_invalid:
                if self._drop_exchange:
                    self._republish_dropped_message('invalid type')
                raise DropMessage
            raise MessageException
