"""
Rejected data objects

"""
import collections
import contextlib
import copy
import time

MESSAGE_ACK = 1
MESSAGE_DROP = 2
MESSAGE_REQUEUE = 3

CONSUMER_EXCEPTION = 10
MESSAGE_EXCEPTION = 11
PROCESSING_EXCEPTION = 12
UNHANDLED_EXCEPTION = 13


class Data(object):

    __slots__ = []

    def __iter__(self):
        """Iterate the attributes and values as key, value pairs.

        :rtype: tuple

        """
        for attribute in self.__slots__:
            yield (attribute, getattr(self, attribute))

    def __repr__(self):
        """Return a string representation of the object and all of its
        attributes.

        :rtype: str

        """
        items = ['%s=%s' % (k, getattr(self, k))
                 for k in self.__slots__ if getattr(self, k)]
        return '<%s(%s)>' % (self.__class__.__name__, items)


class Message(Data):
    """Class for containing all the attributes about a message object creating a
    flatter, move convenient way to access the data while supporting the legacy
    methods that were previously in place in rejected < 2.0

    +------------------------------------------------------------------+
    | Attributes                                                       |
    +======================+===========================================+
    | :attr:`body`         |The AMQP message body                      |
    +----------------------+-------------------------------------------+
    | :attr:`connection`   | The name of the connection that the       |
    |                      | message was received on.                  |
    +----------------------+-------------------------------------------+
    | :attr:`channel`      | The channel that the message was          |
    |                      | was received on.                          |
    +----------------------+-------------------------------------------+
    | :attr:`consumer_tag` | The consumer tag registered with RabbitMQ |
    |                      | that identifies which consumer registered |
    |                      | to receive this message.                  |
    +----------------------+-------------------------------------------+
    | :attr:`delivery_tag` | The delivery tag that represents the      |
    |                      | deliver of this message with RabbitMQ.    |
    +----------------------+-------------------------------------------+
    | :attr:`exchange`     | The exchange the message was published to |
    +----------------------+-------------------------------------------+
    | :attr:`method`       | The :class:`pika.spec.Basic.Deliver` or   |
    |                      | :class:`pika.spec.Basic.Return` object    |
    |                      | that represents how the message was       |
    |                      | received by rejected.                     |
    +----------------------+-------------------------------------------+
    | :attr:`properties`   | The :class:`~pika.spec.BasicProperties`   |
    |                      | object that represents the message's AMQP |
    |                      | properties.                               |
    +----------------------+-------------------------------------------+
    | :attr:`redelivered`  | A flag that indicates the message was     |
    |                      | previously delivered by RabbitMQ.         |
    +----------------------+-------------------------------------------+
    | :attr:`returned`     | A flag that indicates the message was     |
    |                      | returned by RabbitMQ.                     |
    +----------------------+-------------------------------------------+
    | :attr:`routing_key`  | The routing key that was used to deliver  |
    |                      | the message.                              |
    +----------------------+-------------------------------------------+

    """
    __slots__ = ['connection', 'channel', 'method', 'properties', 'body',
                 'consumer_tag', 'delivery_tag', 'exchange', 'redelivered',
                 'routing_key', 'returned']

    def __init__(self, connection, channel, method, properties, body,
                 returned=False):
        """Initialize a message setting the attributes from the given channel,
        method, header and body.

        :param str connection: The connection name for the message
        :param channel: The channel the message was received on
        :type channel: pika.channel.Channel
        :param pika.frames.Method method: pika Method Frame object
        :param pika.spec.BasicProperties properties: message properties
        :param str body: Opaque message body
        :param bool returned: Indicates the message was returned

        """
        self.connection = connection
        self.channel = channel
        self.method = method
        self.properties = Properties(properties)
        self.body = copy.copy(body)
        self.returned = returned

        # Map method properties
        self.consumer_tag = method.consumer_tag
        self.delivery_tag = method.delivery_tag
        self.exchange = method.exchange
        self.redelivered = method.redelivered
        self.routing_key = method.routing_key


class Properties(Data):
    """A class that represents all of the field attributes of AMQP's
    ``Basic.Properties``.

    +-----------------------------------------------------------------+
    | Attributes                                                      |
    +==========================+======================================+
    | :attr:`app_id`           | Creating application id              |
    +--------------------------+--------------------------------------+
    | :attr:`content_type`     | MIME content type                    |
    +--------------------------+--------------------------------------+
    | :attr:`content_encoding` | MIME content encoding                |
    +--------------------------+--------------------------------------+
    | :attr:`correlation_id`   | Application correlation identifier   |
    +--------------------------+--------------------------------------+
    | :attr:`delivery_mode`    | Non-persistent (1) or persistent (2) |
    +--------------------------+--------------------------------------+
    | :attr:`expiration`       | Message expiration specification     |
    +--------------------------+--------------------------------------+
    | :attr:`headers`          | Message header field table           |
    +--------------------------+--------------------------------------+
    | :attr:`message_id`       | Application message identifier       |
    +--------------------------+--------------------------------------+
    | :attr:`priority`         | Message priority, 0 to 9             |
    +--------------------------+--------------------------------------+
    | :attr:`reply_to`         | Address to reply to                  |
    +--------------------------+--------------------------------------+
    | :attr:`timestamp`        | Message timestamp                    |
    +--------------------------+--------------------------------------+
    | :attr:`type`             | Message type name                    |
    +--------------------------+--------------------------------------+
    | :attr:`user_id`          | Creating user id                     |
    +--------------------------+--------------------------------------+

    """
    __slots__ = ['app_id', 'content_type', 'content_encoding',
                 'correlation_id', 'delivery_mode', 'expiration', 'headers',
                 'priority', 'reply_to', 'message_id', 'timestamp', 'type',
                 'user_id']

    def __init__(self, properties=None):
        """Create a base object to contain all of the properties we need

        :param pika.spec.BasicProperties properties: pika.spec.BasicProperties

        """
        for attr in self.__slots__:
            setattr(self, attr, None)
            if properties and getattr(properties, attr):
                setattr(self, attr, getattr(properties, attr))


class Measurement(object):
    """Common Measurement Object that provides common methods for collecting
    and exposes measurement data that is submitted in
    :class:`rejected.process.Process` and :class:`rejected.consumer.Consumer`
    for submission to statsd or influxdb.

    +-------------------------------------------------------------------+
    | Attributes                                                        |
    +===================+===============================================+
    | :attr:`counters`  | Counters that are affected by                 |
    |                   | :meth:`~rejected.data.Measurement.decr` and   |
    |                   | :meth:`~rejected.data.Measurement.incr`       |
    +-------------------+-----------------------------------------------+
    | :attr:`durations` | List of duration values (float or int)        |
    +-------------------+-----------------------------------------------+
    | :attr:`tags`      | Tag key/value pairs for use with InfluxDB     |
    +-------------------+-----------------------------------------------+
    | :attr:`values`    | Numeric values such as integers, gauges,      |
    |                   | and such.                                     |
    +-------------------+-----------------------------------------------+

    .. versionadded:: 3.13.0

    """
    def __init__(self):
        self.durations = {}
        self.counters = collections.Counter()
        self.tags = {}
        self.values = {}

    def decr(self, key, value=1):
        """Decrement a counter.

        :param str key: The key to decrement
        :param int value: The value to decrement by

        """
        self.counters[key] -= value

    def incr(self, key, value=1):
        """Increment a counter.

        :param str key: The key to increment
        :param int value: The value to increment by

        """
        self.counters[key] += value

    def add_duration(self, key, value):
        """Add a duration for the specified key

        :param str key: The value name
        :param float value: The value

        .. versionadded:: 3.19.0

        """
        if key not in self.durations:
            self.durations[key] = []
        self.durations[key].append(value)

    def set_tag(self, key, value):
        """Set a tag. This is only used for InfluxDB measurements.

        :param str key: The tag name
        :param value: The tag value
        :type value: str or bool or int

        """
        self.tags[key] = value

    def set_value(self, key, value):
        """Set a value.

        :param str key: The value name
        :type value: int or float
        :param value: The value

        """
        self.values[key] = value

    @contextlib.contextmanager
    def track_duration(self, key):
        """Context manager that sets a value with the duration of time that it
        takes to execute whatever it is wrapping.

        :param str key: The timing name

        """
        if key not in self.durations:
            self.durations[key] = []
        start_time = time.time()
        try:
            yield
        finally:
            self.durations[key].append(
                max(start_time, time.time()) - start_time)
