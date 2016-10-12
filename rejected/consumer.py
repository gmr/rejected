"""
The :py:class:`Consumer`, :py:class:`PublishingConsumer`,
:py:class:`SmartConsumer`, and :py:class:`SmartPublishingConsumer` provide base
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

    If a consumer raises a `~rejected.consumer.ProcessingException`, the
    message that was being processed will be republished to the exchange
    specified by the ``error`` exchange configuration value or the
    ``ERROR_EXCHANGE`` attribute of the consumer's class. The message will be
    published using the routing key that was last used for the message. The
    original message body and properties will be used and an additional header
    ``X-Processing-Exceptions`` will be added that will contain the number of
    times the message has had a :exc:`~rejected.consumer.ProcessingException`
    raised for it. In combination with a queue that has ``x-message-ttl`` set
    and ``x-dead-letter-exchange`` that points to the original exchange for the
    queue the consumer is consuming off of, you can implement a delayed retry
    cycle for messages that are failing to process due to external resource or
    service issues.

    If ``error_max_retry`` is specified in the configuration or
    ``ERROR_MAX_RETRY`` is set on the class, the headers for each method
    will be inspected and if the value of ``X-Processing-Exceptions`` is
    greater than or equal to the specified value, the message will
    be dropped.

    :param dict settings: The configuration from rejected
    :param rejected.process.Process process: The controlling process
    :param bool drop_invalid_messages: Drop a message if its type property
        doesn't match the specified message type.
    :param str|list message_type: Used to validate the message type of a
        message before processing. This attribute can be set to a string
        that is matched against the AMQP message type or a list of
        acceptable message types.
    :param error_exchange: The exchange to publish a message raising a
        :exc:`~rejected.consumer.ProcessingException` to
    :type error_exchange: str
    :param int error_max_retry: The number of
        :exc:`~rejected.consumer.ProcessingException`s raised on a message
        before a message is dropped. If not specified, messages will never be
        dropped.

    """
    DROP_INVALID_MESSAGES = False
    MESSAGE_TYPE = None
    ERROR_EXCHANGE = 'errors'
    ERROR_MAX_RETRY = None

    def __init__(self, settings, process,
                 drop_invalid_messages=None,
                 message_type=None,
                 error_exchange=None,
                 error_max_retry=None):
        """Creates a new instance of a Consumer class. To perform
        initialization tasks, extend Consumer.initialize

        """
        self._channel = None
        self._drop_invalid = drop_invalid_messages or self.DROP_INVALID_MESSAGES
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
        self._logger = logging.getLogger(settings.get('_import_module',
                                                      __name__))
        self.logger = log.CorrelationAdapter(self._logger, self)

        # Set a Sentry context for the consumer
        self.set_sentry_context('consumer', self.name)

        # Run any child object specified initialization
        self.initialize()

    def initialize(self):
        """Extend this method for any initialization tasks that occur only when
        the `Consumer` class is created."""
        pass

    def prepare(self):
        """Called when a message is received before `process`.

        Asynchronous support: Decorate this method with `.gen.coroutine`
        or `.return_future` to make it asynchronous (the
        `asynchronous` decorator cannot be used on `prepare`).

        If this method returns a `.Future` execution will not proceed
        until the `.Future` is done.
        """
        pass

    def process(self):
        """Extend this method for implementing your Consumer logic.

        If the message can not be processed and the Consumer should stop after
        n failures to process messages, raise the ConsumerException.

        :raises: ConsumerException
        :raises: NotImplementedError

        """
        raise NotImplementedError

    def on_finish(self):
        """Called after the end of a request.
        Override this method to perform cleanup, logging, etc.
        This method is a counterpart to `prepare`.  ``on_finish`` may
        not produce any output, as it is called after the response
        has been sent to the client.
        """
        pass

    def shutdown(self):
        """Override to cleanly shutdown when rejected is stopping"""
        pass

    def finish(self):
        """Finishes message processing for the current message."""
        if self._finished:
            self.logger.warning('Finished called when already finished')
            return
        self._finished = True
        self.on_finish()

    def require_setting(self, name, feature='this feature'):
        """Raises an exception if the given app setting is not defined.

        :param str name: The parameter name
        :param str feature: A friendly name for the setting feature

        """
        if not self.settings.get(name):
            raise Exception("You must define the '%s' setting in your "
                            "application to use %s" % (name, feature))

    def set_sentry_context(self, tag, value):
        """Set a context tag in Sentry for the given key and value.

        :param str tag: The context tag name
        :param str value: The context value

        """
        if self.sentry_client:
            self.logger.debug('Setting sentry context for %s to %s', tag, value)
            self.sentry_client.tags_context({tag: value})

    def stats_add_timing(self, key, duration):
        """Add a timing to the per-message measurements

        :param str key: The key to add the timing to
        :param int|float duration: The timing value

        """
        if not self._measurement:
            LOGGER.warning('stats_add_timing invoked outside execution')
            return
        self._measurement.set_value(key, duration)

    def statsd_add_timing(self, key, duration):
        """Add a timing to the per-message measurements

        :param str key: The key to add the timing to
        :param int|float duration: The timing value

        .. deprecated:: 3.13.0

        """
        warnings.warn('Deprecated, use Consumer.stats_add_timing',
                      DeprecationWarning)
        self.stats_add_timing(key, duration)

    def stats_incr(self, key, value=1):
        """Increment the specified key in the per-message measurements

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

        :param str key: The key to increment
        :param int value: The value to increment the key by

        """
        if not self._measurement:
            LOGGER.warning('stats_set_tag invoked outside execution')
            return
        self._measurement.set_tag(key, value)

    def stats_set_value(self, key, value=1):
        """Set the specified key/value in the per-message measurements

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

        :param str key: The key for the timing to track

        """
        start_time = time.time()
        try:
            yield
        finally:
            self.stats_add_timing(
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
        deadline = self._channel.connection.ioloop.time() + 0.001
        try:
            yield self._yield_condition.wait(deadline)
        except gen.TimeoutError:
            pass

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
    def configuration(self):
        """Access the configuration stanza for the consumer as specified by
        the ``config`` section for the consumer in the rejected configuration.

        .. deprecated:: 3.1
            Use :attr:`.settings` instead.

        :rtype: dict

        """
        warnings.warn('Consumer.configuration is deprecated '
                      'in favor of Consumer.settings',
                      category=DeprecationWarning)
        return self._settings

    @property
    def content_encoding(self):
        """Access the current message's ``content-encoding`` property as an
        attribute of the consumer class.

        :rtype: str

        """
        if not self._message:
            return None
        return (self._message.properties.content_encoding or '').lower() or None

    @property
    def content_type(self):
        """Access the current message's ``content-type`` property as an
        attribute of the consumer class.
        :rtype: str

        """
        if not self._message:
            return None
        return (self._message.properties.content_type or '').lower() or None

    @property
    def correlation_id(self):
        """Access the current message's ``correlation-id`` property as an
        attribute of the consumer class.

        :rtype: str

        """
        if not self._message:
            return None
        return self._message.properties.correlation_id

    @property
    def exchange(self):
        """Access the exchange the message was published to as an attribute
        of the consumer class.

        :rtype: str

        """
        if not self._message:
            return None
        return self._message.exchange

    @property
    def expiration(self):
        """Access the current message's ``expiration`` property as an attribute
        of the consumer class.

        :rtype: str

        """
        if not self._message:
            return None
        return self._message.properties.expiration

    @property
    def headers(self):
        """Access the current message's ``headers`` property as an attribute
        of the consumer class.

        :rtype: dict

        """
        if not self._message:
            return None
        return self._message.properties.headers or dict()

    @property
    def message_id(self):
        """Access the current message's ``message-id`` property as an
        attribute of the consumer class.
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
        """Access the current message's ``priority`` property as an
        attribute of the consumer class.

        :rtype: int

        """
        if not self._message:
            return None
        return self._message.properties.priority

    @property
    def properties(self):
        """Access the current message's properties in dict form as an attribute
        of the consumer class.

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
        """Access the current message's ``reply-to`` property as an
        attribute of the consumer class.

        :rtype: str

        """
        if not self._message:
            return None
        return self._message.properties.reply_to

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
        """Access the current message's ``type`` property as an attribute of
        the consumer class.

        :rtype: str

        """
        if not self._message:
            return None
        return self._message.properties.type

    @property
    def settings(self):
        """Access the consumer settings as specified by the ``config`` section
        for the consumer in the rejected configuration.

        :rtype: dict

        """
        return self._settings

    @property
    def timestamp(self):
        """Access the unix epoch timestamp value from the properties of the
        current message.

        :rtype: int

        """
        if not self._message:
            return None
        return self._message.properties.timestamp

    @property
    def user_id(self):
        """Access the user-id from the current message's properties.

        :rtype: str

        """
        if not self._message:
            return None
        return self._message.properties.user_id

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

    def _clear(self):
        """Resets all assigned data for the current message."""
        self._finished = False
        self._message = None
        self._message_body = None

    @gen.coroutine
    def _execute(self, message_in, measurement):
        """Process the message from RabbitMQ. To implement logic for processing
        a message, extend Consumer._process, not this method.

        :param rejected.Consumer.Message message_in: The message to process
        :param measurement: For collecting per-message instrumentation
        :type measurement: rejected.data.Measurement
        :rtype: bool

        """
        LOGGER.debug('Received: %r', message_in)
        self._clear()
        self._message = message_in
        self._measurement = measurement

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
                    raise gen.Return(data.MESSAGE_DROP)
                raise gen.Return(data.MESSAGE_EXCEPTION)

        # Check the number of ProcessingErrors and possibly drop the message
        if (self._error_max_retry and
                _PROCESSING_EXCEPTIONS in self.headers):
            if self.headers[_PROCESSING_EXCEPTIONS] >= self._error_max_retry:
                self.logger.warning('Dropping message with %i deaths due to '
                                    'ERROR_MAX_RETRY',
                                    self.headers[_PROCESSING_EXCEPTIONS])
                raise gen.Return(data.MESSAGE_DROP)

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
            raise gen.Return(None)

        except exceptions.ConnectionClosed as error:
            self.logger.critical('Connection closed while processing %s: %s',
                                 message_in.delivery_tag, error)
            raise gen.Return(None)

        except ConsumerException as error:
            self.logger.error('ConsumerException processing delivery %s: %r',
                              message_in.delivery_tag, error)
            raise gen.Return(data.CONSUMER_EXCEPTION)

        except MessageException as error:
            self.logger.error('MessageException processing delivery %s: %r',
                              message_in.delivery_tag, error)
            raise gen.Return(data.MESSAGE_EXCEPTION)

        except ProcessingException as error:
            self.logger.error('ProcessingException processing delivery %s: %r',
                              message_in.delivery_tag, error)
            self._republish_processing_error()
            raise gen.Return(data.PROCESSING_EXCEPTION)

        except Exception as error:
            self.log_exception('Exception processing delivery %s: %s',
                               message_in.delivery_tag, error)
            raise gen.Return(data.UNHANDLED_EXCEPTION)

        if not self._finished:
            self.finish()
        self.logger.debug('Post finish')
        raise gen.Return(data.MESSAGE_ACK)

    def _republish_processing_error(self):
        """Republish the original message that was received because a
        ProcessingException was raised.

        Add a header that keeps track of how many times this has happened
        for this message.

        """
        self.logger.debug('Republishing due to ProcessingException')
        properties = dict(self._message.properties) or {}
        if 'headers' not in properties or not properties['headers']:
            properties['headers'] = {}

        if _PROCESSING_EXCEPTIONS not in properties['headers']:
            properties['headers'][_PROCESSING_EXCEPTIONS] = 1
        else:
            try:
                properties['headers'][_PROCESSING_EXCEPTIONS] += 1
            except TypeError:
                properties['headers'][_PROCESSING_EXCEPTIONS] = 1

        self._channel.basic_publish(self._error_exchange,
                                    self._message.routing_key,
                                    self._message.body,
                                    pika.BasicProperties(**properties))

    def _set_channel(self, channel):
        """Assign the _channel attribute to the channel that was passed in.
        This should not be extended.

        :param pika.channel.Channel channel: The channel to assign

        """
        self._channel = channel

    def log_exception(self, msg_format, *args, **kwargs):
        """Customize the logging of uncaught exceptions.

        :param str msg_format: format of msg to log with ``self.logger.error``
        :param args: positional arguments to pass to ``self.logger.error``
        :param kwargs: keyword args to pass into ``self.logger.error``
        :keyword bool send_to_sentry: if omitted or *truthy*, this keyword
            will send the captured exception to Sentry (if enabled).

        By default, this method will log the message using
        :meth:`logging.Logger.error` and send the exception to Sentry.
        If an exception is currently active, then the traceback will be
        logged at the debug level.

        """
        self.logger.error(msg_format, *args)
        exc_info = sys.exc_info()
        if all(exc_info):
            exc_type, exc_value, tb = exc_info
            exc_name = exc_type.__name__
            self.logger.exception('Processor handled %s: %s', exc_name,
                                  exc_value)
        self._process.send_exception_to_sentry(exc_info)


class PublishingConsumer(Consumer):
    """The PublishingConsumer extends the Consumer class, adding two methods,
    one that allows for
    :py:meth:`publishing <rejected.consumer.PublishingConsumer.publish_message>`
    of messages back on the same channel that the consumer is communicating on
    and another for
    :py:meth:`replying to messages<rejected.consumer.PublishingConsumer.reply>`,
    adding RPC reply semantics to the outbound message.

    In any of the consumer base classes, if the ``MESSAGE_TYPE`` attribute is
    set, the ``type`` property of incoming messages will be validated against
    when a message is received, checking for string equality against the
    ``MESSAGE_TYPE`` attribute. If they are not matched, the consumer will not
    process the message and will drop the message without an exception if the
    ``DROP_INVALID_MESSAGES`` attribute is set to ``True``. If it is ``False``,
    a :py:class:`ConsumerException` is raised.

    :param dict settings: The configuration from rejected
    :param rejected.process.Process process: The controlling process
    :param bool drop_invalid_messages: Drop a message if its type property
        doesn't match the specified message type.
    :param str|list message_type: Used to validate the message type of a
        message before processing. This attribute can be set to a string
        that is matched against the AMQP message type or a list of
        acceptable message types.
    :param error_exchange: The exchange to publish `ProcessingException` to
    :type error_exchange: str
    :param int error_max_retry: The number of `ProcessingException`s
        raised on a message before a message is dropped. If not specified,
        messages will never be dropped.

    """

    def initialize(self):
        super(PublishingConsumer, self).initialize()

    def publish_message(self, exchange, routing_key, properties, body):
        """Publish a message to RabbitMQ on the same channel the original
        message was received on.

        :param str exchange: The exchange to publish to
        :param str routing_key: The routing key to publish with
        :param dict properties: The message properties
        :param str body: The message body

        """
        # Convert the dict to pika.BasicProperties
        self.logger.debug('Converting properties')
        msg_props = self._get_pika_properties(properties)

        # Publish the message
        self.logger.debug('Publishing message to %s:%s', exchange, routing_key)
        with self._measurement.track_duration(
                'publish.{}.{}'.format(exchange, routing_key)):
            self._channel.basic_publish(exchange=exchange,
                                        routing_key=routing_key,
                                        properties=msg_props,
                                        body=body)

    def reply(self, response_body, properties,
              auto_id=True,
              exchange=None,
              reply_to=None):
        """Reply to the received message.

        If auto_id is True, a new uuid4 value will be generated for the
        message_id and correlation_id will be set to the message_id of the
        original message. In addition, the timestamp will be assigned the
        current time of the message. If auto_id is False, neither the
        message_id or the correlation_id will be changed in the properties.

        If exchange is not set, the exchange the message was received on will
        be used.

        If reply_to is set in the original properties,
        it will be used as the routing key. If the reply_to is not set
        in the properties and it is not passed in, a ValueException will be
        raised. If reply to is set in the properties, it will be cleared out
        prior to the message being republished.

        :param any response_body: The message body to send
        :param rejected.data.Properties properties: Message properties to use
        :param bool auto_id: Automatically shuffle message_id & correlation_id
        :param str exchange: Override the exchange to publish to
        :param str reply_to: Override the reply_to in the properties
        :raises: ValueError

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

    @staticmethod
    def _get_pika_properties(properties_in):
        """Return a pika.BasicProperties object for a rejected.data.Properties
        object.

        :param dict properties_in: Properties to convert
        :rtype: pika.BasicProperties

        """
        if not properties_in:
            return
        properties = pika.BasicProperties()
        for key in properties_in:
            if properties_in.get(key) is not None:
                setattr(properties, key, properties_in.get(key))
        return properties


class SmartConsumer(Consumer):
    """Base class to ease the implementation of strongly typed message consumers
    that validate and automatically decode and deserialize the inbound message
    body based upon the message properties. Additionally, should one of the
    supported ``content_encoding`` types (``gzip`` or ``bzip2``) be specified
    in the message's property, it will automatically be decoded.

    *Supported MIME types for automatic deserialization are:*

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

    :param dict settings: The configuration from rejected
    :param rejected.process.Process process: The controlling process
    :param bool drop_invalid_messages: Drop a message if its type property
        doesn't match the specified message type.
    :param str|list message_type: Used to validate the message type of a
        message before processing. This attribute can be set to a string
        that is matched against the AMQP message type or a list of
        acceptable message types.
    :param error_exchange: The exchange to publish `ProcessingException` to
    :type error_exchange: str
    :param int error_max_retry: The number of `ProcessingException`s
        raised on a message before a message is dropped. If not specified,
        messages will never be dropped.

    """

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


class SmartPublishingConsumer(SmartConsumer, PublishingConsumer):
    """PublishingConsumer with serialization built in

    :param dict settings: The configuration from rejected
    :param rejected.process.Process process: The controlling process
    :param bool drop_invalid_messages: Drop a message if its type property
        doesn't match the specified message type.
    :param str|list message_type: Used to validate the message type of a
        message before processing. This attribute can be set to a string
        that is matched against the AMQP message type or a list of
        acceptable message types.
    :param error_exchange: The exchange to publish `ProcessingException` to
    :type error_exchange: str
    :param int error_max_retry: The number of `ProcessingException`s
        raised on a message before a message is dropped. If not specified,
        messages will never be dropped.

    """
    def publish_message(self, exchange, routing_key, properties, body,
                        no_serialization=False,
                        no_encoding=False):
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
        :param no_serialization: Turn off auto-serialization of the body
        :param no_encoding: Turn off auto-encoding of the body

        """
        # Convert the rejected.data.Properties object to a pika.BasicProperties
        self.logger.debug('Converting properties')
        properties_out = self._get_pika_properties(properties)

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
        self._channel.basic_publish(exchange=exchange,
                                    routing_key=routing_key,
                                    properties=properties_out,
                                    body=body)

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

        :param str|dict|list: The value to serialize as JSON
        :rtype: bytes

        """
        return json.dumps(value, ensure_ascii=True).encode('utf-8')

    @staticmethod
    def _dump_msgpack_value(value):
        """Serialize a value into MessagePack

        :param str|dict|list: The value to serialize as msgpack
        :rtype: bytes

        """
        return umsgpack.packb(value)

    @staticmethod
    def _dump_pickle_value(value):
        """Serialize a value into the pickle format

        :param any value: The object to pickle
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
        """Dump a dict into a YAML string

        :param dict value: The value to dump as a YAML string
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


class ConsumerException(Exception):
    """May be called when processing a message to indicate a problem that the
    Consumer may be experiencing that should cause it to stop.

    """
    pass


class MessageException(Exception):
    """Invoke when a message should be rejected and not requeued, but not due
    to a processing error that should cause the consumer to stop.

    """
    pass


class ProcessingException(Exception):
    """Invoke when a message should be rejected and not requeued, but not due
    to a processing error that should cause the consumer to stop. This should
    be used for when you want to reject a message which will be republished to
    a retry queue, without anything being stated about the exception.

    """
    pass
