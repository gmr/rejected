"""Base rejected message Consumer class that aims at simplifying the process
of writing message Consumers that enforce good behaviors in dealing with
messages.

"""
import bz2
import copy
import csv
import datetime
import json
import logging
import pickle
import pika
import plistlib
import StringIO as stringio
import sys
import time
import uuid
import yaml
import zlib

LOGGER = logging.getLogger(__name__)

BS4_MIME_TYPES = ('text/html', 'text/xml')
PICKLE_MIME_TYPES = ('application/pickle',
                     'application/x-pickle',
                     'application/x-vnd.python.pickle',
                     'application/vnd.python.pickle')
YAML_MIME_TYPES = ('text/yaml', 'text/x-yaml')

# Optional imports
try:
    import bs4
except ImportError:
    LOGGER.warning('BeautifulSoup not found, disabling html and xml support')
    bs4 = None


class Consumer(object):
    """Base consumer class that defines the contract between rejected and
    consumer applications.

    If ``MESSAGE_TYPE`` is set, it will be validated against when a message
    is received, checking the properties.type value. If they are not matched,
    the :py:class:`Consumer` will not process the message and will drop the
    message, returning True if ``DROP_INVALID_MESSAGES`` is ``True``. If it is
    ``False``, the message will cause the :py:class:`Consumer` to return
    ``False`` and return the message to the broker.

    """
    DROP_INVALID_MESSAGES = False
    MESSAGE_TYPE = None

    def __init__(self, configuration):
        """Creates a new instance of a Consumer class. To perform
        initialization tasks, extend Consumer.initialize

        :param dict configuration: The configuration from rejected

        """
        self._config = configuration
        self._channel = None
        self._message = None

        # Run any child object specified initialization
        self.initialize()

    def initialize(self):
        """Extend this method for any initialization tasks"""
        pass

    def process(self):
        """Extend this method for implementing your Consumer logic.

        If the message can not be processed and the Consumer should stop after
        n failures to process messages, raise the ConsumerException.

        :raises: ConsumerException
        :raises: NotImplementedError

        """
        raise NotImplementedError

    def receive(self, message_in):
        """Process the message from RabbitMQ. To implement logic for processing
        a message, extend Consumer._process, not this method.

        :param rejected.Consumer.Message message_in: The message to process
        :rtype: bool

        """
        LOGGER.debug('Received: %r', message_in)
        self._message = message_in

        # Validate the message type if the child sets _MESSAGE_TYPE
        if self.MESSAGE_TYPE and self.MESSAGE_TYPE != self.message_type:
            LOGGER.error('Received a non-supported message type: %s',
                         self.message_type)

            # Should the message be dropped or returned to the broker?
            if self.DROP_INVALID_MESSAGES:
                LOGGER.debug('Dropping the invalid message')
                return
            else:
                raise ConsumerException('Invalid message type')

        # Let the child object process the message
        self.process()

    def set_channel(self, channel):
        """Assign the _channel attribute to the channel that was passed in.
        This should not be extended.

        :param pika.channel.Channel channel: The channel to assign

        """
        self._channel = channel

    @property
    def app_id(self):
        """Return the app-id from the message properties.

        :rtype: str

        """
        return self._message.properties.app_id

    @property
    def body(self):
        """Return the opaque body from the current message.

        :rtype: str

        """
        return self._message.body

    @property
    def configuration(self):
        """Return the configuration data passed in by the rejected Processor.

        :rtype: dict

        """
        return self._config

    @property
    def content_encoding(self):
        """Return the content-encoding from the message properties.

        :rtype: str

        """
        return (self._message.properties.content_encoding or '').lower() or None

    @property
    def content_type(self):
        """Return the content-type from the message properties for the current
        message.

        :rtype: str

        """
        return (self._message.properties.content_type or '').lower() or None

    @property
    def correlation_id(self):
        """Return the correlation-id from the message properties for the current
        message.

        :rtype: str

        """
        return self._message.properties.correlation_id

    @property
    def exchange(self):
        """Return the exchange the message for the current message.

        :rtype: str

        """
        return self._message.exchange

    @property
    def expiration(self):
        """Return the expiration from the message properties for the current
        message.

        :rtype: str

        """
        return self._message.properties.expiration

    @property
    def headers(self):
        """Return the headers from the message properties for the current
         message.

        :rtype: dict

        """
        return self._message.properties.headers or dict()

    @property
    def message_id(self):
        """Return the message-id from the message properties for the current
        message.

        :rtype: str

        """
        return self._message.properties.message_id

    @property
    def name(self):
        """Return the consumer class name.

        :rtype: str

        """
        return self.__class__.__name__

    @property
    def priority(self):
        """Return the priority from the message properties for the current
        message.

        :rtype: int

        """
        return self._message.properties.priority

    @property
    def properties(self):
        """Return the properties for the current message as dict.

        :rtype: dict

        """
        return dict(self._message.properties)

    @property
    def redelivered(self):
        """Indicates if the current message has been redelivered.

        :rtype: bool

        """
        return self._message.redelivered

    @property
    def reply_to(self):
        """Return the priority from the message properties for the current
        message.

        :rtype: str

        """
        return self._message.properties.reply_to

    @property
    def routing_key(self):
        """Return the routing key for the current message.

        :rtype: str

        """
        return self._message.routing_key

    @property
    def message_type(self):
        """Return the type from the current message's properties.

        :rtype: str

        """
        return self._message.properties.type

    @property
    def timestamp(self):
        """Return the unix epoch timestamp value from the properties of the
        current message.

        :rtype: int

        """
        return self._message.properties.timestamp

    @property
    def user_id(self):
        """Return the user-id from the current message's properties.

        :rtype: str

        """
        return self._message.properties.user_id


class PublishingConsumer(Consumer):
    """The PublishingConsumer extends the Consumer class, adding two methods,
    one that allows for arbitrary publishing back on the same channel that the
    consumer is communicating on and another for replying to messages, adding
    RPC reply semantics to the outbound message.

    """
    def publish_message(self, exchange, routing_key, properties, body):
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
        :param rejected.data.Properties: The message properties
        :param bool no_serialization: Turn off auto-serialization of the body
        :param bool no_encoding: Turn off auto-encoding of the body

        """
        # Convert the rejected.data.Properties object to a pika.BasicProperties
        LOGGER.debug('Converting properties')
        msg_props = self._get_pika_properties(properties)

        # Publish the message
        LOGGER.debug('Publishing message to %s:%s', exchange, routing_key)
        self._channel.basic_publish(exchange=exchange,
                                    routing_key=routing_key,
                                    properties=msg_props,
                                    body=body)

    def reply(self, response_body, properties, auto_id=True,
              exchange=None, reply_to=None):
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

        Like with the publish method, if you pass in a non-String object and
        content-type is set to a supported content type, the content will
        be auto-serialized. In addition, if the content-encoding is set to a
        supported encoding, it will be auto-encoded.

        :param any response_body: The message body to send
        :param dict properties: Message properties to use
        :param bool auto_id: Automatically shuffle message_id and correlation_id
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
            LOGGER.debug('New message_id: %s', properties.message_id)
            LOGGER.debug('Correlation_id: %s', properties.correlation_id)

        # Redefine the reply to if needed
        reply_to = reply_to or properties.reply_to

        # Wipe out reply_to if it's set
        if properties.reply_to:
            properties.reply_to = None

        self.publish_message(exchange or self._message.exchange,
                             reply_to,
                             properties,
                             response_body)

    @staticmethod
    def _get_pika_properties(properties_in):
        """Return a pika.BasicProperties object for a rejected.data.Properties
        object.

        :param dict properties_in: Properties to convert
        :rtype: pika.BasicProperties

        """
        properties = pika.BasicProperties()
        for key in properties_in:
            if properties_in.get(key) is not None:
                setattr(properties, key, properties_in.get(key))
        return properties


class SmartConsumer(Consumer):
    """Base class to ease the implementation of strongly typed message consumers
    that validate and automatically decode and deserialize the inbound message
    body based upon the message properties.

    """
    def __init__(self, configuration):
        self._message_body = None
        super(SmartConsumer, self).__init__(configuration)

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

        :param str value: Compressed value
        :rtype: str

        """
        return bz2.decompress(value)

    @staticmethod
    def _decode_gzip(value):
        """Return a zlib decompressed value

        :param str value: Compressed value
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
        return bs4.BeautifulSoup(value)

    @staticmethod
    def _load_csv_value(value):
        """Create a csv.DictReader instance for the sniffed dialect for the
        value passed in.

        :param str value: The CSV value
        :rtype: csv.DictReader

        """
        csv_buffer = stringio.StringIO(value)
        dialect = csv.Sniffer().sniff(csv_buffer.read(1024))
        csv_buffer.seek(0)
        return csv.DictReader(csv_buffer, dialect=dialect)

    @staticmethod
    def _load_json_value(value):
        """Deserialize a JSON string returning the native Python data type
        for the value.

        :param str value: The JSON string
        :rtype: object

        """
        try:
            return json.loads(value, encoding='utf-8')
        except ValueError as error:
            LOGGER.error('Could not decode message body: %s', error,
                         exc_info=sys.exc_info())
            raise MessageException(error)

    @staticmethod
    def _load_pickle_value(value):
        """Deserialize a pickle string returning the native Python data type
        for the value.

        :param str value: The pickle string
        :rtype: object

        """
        return pickle.loads(value)

    @staticmethod
    def _load_plist_value(value):
        """Deserialize a plist string returning the native Python data type
        for the value.

        :param str value: The pickle string
        :rtype: dict

        """
        return plistlib.readPlistFromString(value)

    @staticmethod
    def _load_yaml_value(value):
        """Load an YAML string into an dict object.

        :param str value: The YAML string
        :rtype: any
        :raises: ConsumerException

        """
        return yaml.load(value)


class SmartPublishingConsumer(SmartConsumer, PublishingConsumer):
    """

    """
    def publish_message(self, exchange, routing_key, properties, body,
                        no_serialization=False, no_encoding=False):
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
        :param no_serialization: Turn off auto-serialization of the body
        :param no_encoding: Turn off auto-encoding of the body

        """
        # Convert the rejected.data.Properties object to a pika.BasicProperties
        LOGGER.debug('Converting properties')
        properties_out = self._get_pika_properties(properties)

        # Auto-serialize the content if needed
        if (not no_serialization and not isinstance(body, basestring) and
            properties.content_type):
            LOGGER.debug('Auto-serializing message body')
            body = self._auto_serialize(properties.content_type, body)

        # Auto-encode the message body if needed
        if not no_encoding and properties.content_encoding:
            LOGGER.debug('Auto-encoding message body')
            body = self._auto_encode(properties.content_encoding, body)

        # Publish the message
        LOGGER.debug('Publishing message to %s:%s', exchange, routing_key)
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

        LOGGER.warning('Invalid content-encoding specified for auto-encoding')
        return value

    def _auto_serialize(self, content_type, value):
        """Auto-serialization of the value based upon the content-type value.

        :param str content_type: The content type to serialize
        :param any value: The value to serialize
        :rtype: str

        """
        if content_type == 'application/json':
            LOGGER.debug('Auto-serializing content as JSON')
            return self._dump_json_value(value)

        if content_type in PICKLE_MIME_TYPES:
            LOGGER.debug('Auto-serializing content as Pickle')
            return self._dump_pickle_value(value)

        if content_type  == 'application/x-plist':
            LOGGER.debug('Auto-serializing content as plist')
            return self._dump_plist_value(value)

        if content_type  == 'text/csv':
            LOGGER.debug('Auto-serializing content as csv')
            return self._dump_csv_value(value)

        # If it's XML or HTML auto
        if (bs4 and isinstance(value, bs4.BeautifulSoup) and
            content_type in ('text/html', 'text/xml')):
            LOGGER.debug('Dumping BS4 object into HTML or XML')
            return self._dump_bs4_value(value)

        # If it's YAML, load the content via pyyaml into a dict
        if self.content_type in YAML_MIME_TYPES:
            LOGGER.debug('Auto-serializing content as YAML')
            return self._dump_yaml_value(value)

        LOGGER.warning('Invalid content-type specified for auto-serialization')
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
        buffer = stringio.StringIO()
        writer = csv.writer(buffer,quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerows(value)
        buffer.seek(0)
        value = buffer.read()
        buffer.close()
        return value

    @staticmethod
    def _dump_json_value(value):
        """Serialize a value into JSON

        :param str|dict|list: The value to serialize as JSON
        :rtype: str

        """
        return json.dumps(value, ensure_ascii=False)

    @staticmethod
    def _dump_pickle_value(value):
        """Serialize a value into the pickle format

        :param any value: The object to pickle
        :rtype: str

        """
        return pickle.dumps(value)

    @staticmethod
    def _dump_plist_value(value):
        """Create a plist value from a dictionary

        :param dict value: The value to make the plist from
        :rtype: dict

        """
        return plistlib.writePlistToString(value)

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
        :rtype: str

        """
        return bz2.compress(value)

    @staticmethod
    def _encode_gzip(value):
        """Return zlib compressed value

        :param str value: Uncompressed value
        :rtype: str

        """
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
