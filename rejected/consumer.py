"""Base rejected message Consumer class that aims at simplifying the process
of writing message Consumers that enforce good behaviors in dealing with
messages.

"""
import bz2
import copy
import csv
import datetime
try:
    import simplejson as json
except ImportError:
    import json
import logging
import pickle
import pika
import plistlib
import StringIO as stringio
import time
import uuid
import yaml
import zlib

logger = logging.getLogger(__name__)

# Optional imports
try:
    import bs4
except ImportError:
    logger.warning('BeautifulSoup not found, disabling html and xml support')
    bs4 = None

try:
    import couchconfig
except ImportError:
    logger.warning('couchconfig not found, disabling configuration support')
    couchconfig = None

try:
    import pgsql_wrapper
except ImportError:
    logger.warning('pgsql_wrapper not found, disabling PostgreSQL support')
    pgsql_wrapper = None

try:
    import redis
except ImportError:
    logger.warning('redis-py not found, disabling Redis support')
    redis = None


class Consumer(object):
    """Base Consumer class to ease the implementation of strongly typed message
    Consumers that validate and automatically decode and deserialize the
    inbound message body based upon the message properties.

    If Child._MESSAGE_TYPE is set, it will be validated against when a message
    is received, checking the properties.type value. If they are not matched,
    the Consumer will not process the message and will drop the message,
    returning True if Child._DROP_INVALID_MESSAGES is True. If it is False,
    the message will cause the Consumer to return False and return the message
    to the broker.

    If _DROP_EXPIRED_MESSAGES is True and a message has the expiration property
    set and the expiration has occurred, the message will be dropped.

    """
    _CONFIG_KEYS = ['service', 'config_host', 'config_domain', 'config_ttl']
    _DROP_INVALID_MESSAGES = False
    _DROP_EXPIRED_MESSAGES = False
    _MESSAGE_TYPE = None
    _PICKLE_MIME_TYPES = ['application/pickle',
                          'application/x-pickle',
                          'application/x-vnd.python.pickle',
                          'application/vnd.python.pickle']
    _YAML_MIME_TYPES = ['text/yaml', 'text/x-yaml']

    def __init__(self, configuration):
        """Creates a new instance of a Consumer class. To perform
        initialization tasks, extend Consumer._initialize

        :param dict configuration: The configuration from rejected

        """
        # Carry the configuration for use elsewhere
        self._config = configuration

        # Default channel attribute
        self._channel = None

        # Each message received will be carried as an attribute
        self._message = None
        self._message_body = None


    @property
    def message_app_id(self):
        """Return the app-id from the message properties.

        :rtype: str

        """
        return self._message.properties.app_id

    @property
    def message_body(self):
        """Return the message body, unencoded if needed,
        deserialized if possible.

        :rtype: any

        """
        # Return a materialized view of the body if it has been previously set
        if self._message_body:
            return self._message_body

        # Sanitize a improperly set content-encoding from mybPublish
        if self.message_content_encoding == 'utf-8':
            self._message.properties.content_encoding = None
            self._message_body = self._message.body
            logger.debug('Coerced an incorrect content-encoding of UTF-8 to '
                         'None')

        # Handle bzip2 compressed content
        elif self.message_content_encoding == 'bzip2':
            self._message_body = self._decode_bz2(self._message.body)

        # Handle zlib compressed content
        elif self.message_content_encoding == 'gzip':
            self._message_body = self._decode_gzip(self._message.body)

        # Else we want to assign self._message.body to self._message_body
        else:
            self._message_body = self._message.body

        # Handle the auto-deserialization
        if self.message_content_type == 'application/json':
            self._message_body = self._load_json_value(self._message_body)

        elif self.message_content_type in self._PICKLE_MIME_TYPES:
            self._message_body = self._load_pickle_value(self._message_body)

        elif self.message_content_type == 'application/x-plist':
            self._message_body = self._load_plist_value(self._message_body)

        elif self.message_content_type == 'text/csv':
            self._message_body = self._load_csv_value(self._message_body)

        elif bs4 and self.message_content_type in ('text/html', 'text/xml'):
            self._message_body = self._load_bs4_value(self._message_body)

        elif self.message_content_type in self._YAML_MIME_TYPES:
            self._message_body = self._load_yaml_value(self._message_body)

        # Return the message body
        return self._message_body

    @property
    def message_content_encoding(self):
        """Return the content-encoding from the message properties.

        :rtype: str

        """
        return (self._message.properties.content_encoding or '').lower() or None

    @property
    def message_content_type(self):
        """Return the content-type from the message properties.

        :rtype: str

        """
        return (self._message.properties.content_type or '').lower() or None

    @property
    def message_correlation_id(self):
        """Return the correlation-id from the message properties.

        :rtype: str

        """
        return self._message.properties.correlation_id

    @property
    def message_exchange(self):
        """Return the exchange the message.

        :rtype: str

        """
        return self._message.exchange

    @property
    def message_expiration(self):
        """Return the expiration as a datetime from the message properties.

        :rtype: datetime.datetime

        """
        if not self._message.properties.expiration:
            return None
        float_value = float(self._message.properties.expiration)
        return datetime.datetime.fromtimestamp(float_value)

    @property
    def message_headers(self):
        """Return the headers from the message properties.

        :rtype: dict

        """
        return self._message.properties.headers

    @property
    def message_has_expired(self):
        """Return a boolean evaluation of if the message has expired. If
        expiration is not set, always return False

        :rtype: bool

        """
        if not self._message.properties.expiration:
            return False
        return time.time() >= self._message.properties.expiration

    @property
    def message_id(self):
        """Return the message-id from the message properties.

        :rtype: str

        """
        return self._message.properties.message_id

    @property
    def message_priority(self):
        """Return the priority from the message properties.

        :rtype: int

        """
        return self._message.properties.priority

    @property
    def message_reply_to(self):
        """Return the priority from the message properties.

        :rtype: str

        """
        return self._message.properties.reply_to

    @property
    def message_routing_key(self):
        """Return the routing key the message.

        :rtype: str

        """
        return self._message.routing_key

    @property
    def message_type(self):
        """Return the type from the message properties.

        :rtype: str

        """
        return self._message.properties.type

    @property
    def message_time(self):
        """Return the time of the message from the properties.

        :rtype: datetime.datetime

        """
        if not self._message.properties.timestamp:
            return None
        float_value = float(self._message.properties.timestamp)
        return datetime.datetime.fromtimestamp(float_value)

    @property
    def message_user_id(self):
        """Return the user-id from the message properties.

        :rtype: str

        """
        return self._message.properties.user_id

    def name(self, *args, **kwargs):
        """Return the consumer class name.

        :rtype: str

        """
        return self.__class__.__name__

    def process(self, message_in):
        """Process the message from RabbitMQ. To implement logic for processing
        a message, extend Consumer._process, not this method.

        :param rejected.Consumer.Message message_in: The message to process
        :rtype: bool

        """
        # Clear out our previous body values
        self._message_body = None

        logger.debug('Received: %r', message_in)
        self._message = message_in

        # Validate the message type if the child sets _MESSAGE_TYPE
        if self._MESSAGE_TYPE and self._MESSAGE_TYPE != self.message_type:
            logger.error('Received a non-supported message type: %s',
                         self.message_type)

            # Should the message be dropped or returned to the broker?
            if self._DROP_INVALID_MESSAGES:
                logger.debug('Dropping the invalid message')
                return
            else:
                raise ConsumerException('Invalid message type')

        # Drop expired messages if desired
        if self._DROP_EXPIRED_MESSAGES and self.message_has_expired:
            logger.debug('Message expired %i seconds ago, dropping.',
                         time.time() - self.message_expiration)
            return

        # Let the child object process the message
        self._process()

    @property
    def properties(self):
        """Return the properties for the message as a rejected.data.Properties
        object.

        :rtype: rejected.data.Properties

        """
        return copy.copy(self._message.properties)

    def set_channel(self, channel):
        """Assign the _channel attribute to the channel that was passed in.

        :param pika.channel.Channel channel: The channel to assign

        """
        self._channel = channel

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

        logger.warning('Invalid content-encoding specified for auto-encoding')
        return value

    def _auto_serialize(self, content_type, value):
        """Auto-serialization of the value based upon the content-type value.

        :param str content_type: The content type to serialize
        :param any value: The value to serialize
        :rtype: str

        """
        if content_type == 'application/json':
            logger.debug('Auto-serializing content as JSON')
            return self._dump_json_value(value)

        if content_type  in self._PICKLE_MIME_TYPES:
            logger.debug('Auto-serializing content as Pickle')
            return self._dump_pickle_value(value)

        if content_type  == 'application/x-plist':
            logger.debug('Auto-serializing content as plist')
            return self._dump_plist_value(value)

        if content_type  == 'text/csv':
            logger.debug('Auto-serializing content as csv')
            return self._dump_csv_value(value)

        # If it's XML or HTML auto
        if (bs4 and isinstance(value, bs4.BeautifulSoup) and
            content_type in ('text/html', 'text/xml')):
            logger.debug('Dumping BS4 object into HTML or XML')
            return self._dump_bs4_value(value)

        # If it's YAML, load the content via pyyaml into a dict
        if self.message_content_type in self._YAML_MIME_TYPES:
            logger.debug('Auto-serializing content as YAML')
            return self._dump_yaml_value(value)

        logger.warning('Invalid content-type specified for auto-serialization')
        return value

    def _decode_bz2(self, value):
        """Return a bz2 decompressed value

        :param str value: Compressed value
        :rtype: str

        """
        return bz2.decompress(value)

    def _decode_gzip(self, value):
        """Return a zlib decompressed value

        :param str value: Compressed value
        :rtype: str

        """
        return zlib.decompress(value)

    def _dump_bs4_value(self, value):
        """Return a BeautifulSoup object as a string

        :param bs4.BeautifulSoup value: The object to return a string from
        :rtype: str

        """
        return str(value)

    def _dump_csv_value(self, value):
        """Take a list of lists and return it as a CSV value

        :param list value: A list of lists to return as a CSV
        :rtype: str

        """
        buffer = stringio.StringIO()
        writer = csv.writer(buffer,quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerows(value)
        buffer.seek(0)
        value =  buffer.read()
        buffer.close()
        return value

    def _dump_json_value(self, value):
        """Serialize a value into JSON

        :param str|dict|list: The value to serialize as JSON
        :rtype: str

        """
        return json.dumps(value)

    def _dump_pickle_value(self, value):
        """Serialize a value into the pickle format

        :param any value: The object to pickle
        :rtype: str

        """
        return pickle.dumps(value)

    def _dump_plist_value(self, value):
        """Create a plist value from a dictionary

        :param dict value: The value to make the plist from
        :rtype: dict

        """
        return plistlib.writePlistToString(value)

    def _dump_yaml_value(self, value):
        """Dump a dict into a YAML string

        :param dict value: The value to dump as a YAML string
        :rtype: str

        """
        return yaml.dump(value)

    def _encode_bz2(self, value):
        """Return a bzip2 compressed value

        :param str value: Uncompressed value
        :rtype: str

        """
        return bz2.compress(value)

    def _encode_gzip(self, value):
        """Return zlib compressed value

        :param str value: Uncompressed value
        :rtype: str

        """
        return zlib.decompress(value)

    def _get_pika_properties(self, properties_in):
        """Return a pika.BasicProperties object for a rejected.data.Properties
        object.

        :param rejected.data.Properties properties_in: Properties to convert
        :rtype: pika.BasicProperties

        """
        properties = pika.BasicProperties()
        if properties_in.app_id:
            properties.app_id = properties_in.app_id
        if properties_in.cluster_id:
            properties.cluster_id = properties_in.cluster_id
        if properties_in.content_encoding:
            properties.content_encoding = properties_in.content_encoding
        if properties_in.content_type:
            properties.content_type = properties_in.content_type
        if properties_in.correlation_id:
            properties.correlation_id = properties_in.correlation_id
        if properties_in.delivery_mode:
            properties.delivery_mode = properties_in.delivery_mode
        if properties_in.expiration:
            properties.expiration = properties_in.expiration
        if properties_in.headers:
            properties.headers = copy.deepcopy(properties_in.headers)
        if properties_in.priority:
            properties.priority = properties_in.priority
        if properties_in.reply_to:
            properties.reply_to = properties_in.reply_to
        if properties_in.message_id:
            properties.message_id = properties_in.message_id
        properties.timestamp = properties_in.timestamp or int(time.time())
        if properties_in.type:
            properties.type = properties_in.type
        if properties_in.user_id:
            properties.user_id = properties_in.user_id
        return properties

    def _get_pgsql_cursor(self, host, port, dbname, user, password=None):
        """Connect to PostgreSQL and return the cursor.

        :param str host: The PostgreSQL host
        :param int port: The PostgreSQL port
        :param str dbname: The database name
        :param str user: The user name
        :param str password: The optional password
        :rtype: psycopg2.Cursor
        :raises: ImportError

        """
        if not pgsql_wrapper:
            raise ImportError('pgsql_wrapper not installed')
        # Connect to PostgreSQL
        try:
            connection = pgsql_wrapper.PgSQL(host, port, dbname, user, password)
        except pgsql_wrapper.OperationalError as exception:
            raise IOError(exception)

        # Return a cursor
        return connection.cursor

    def _get_redis_client(self, host, port, db):
        """Return a redis client for the given host, port and db.

        :param str host: The redis host to connect to
        :param int port: The redis port to connect to
        :param int db: The redis database number to use
        :rtype: redis.client.Redis
        :raises: ImportError

        """
        if not redis:
            raise ImportError('redis not installed')
        return redis.Redis(host=host, port=port, db=db)

    def _get_service(self, fqdn):
        """Return the service notation for the fqdn value (tld_domain_service).

        :param str fqdn: The fqdn value (service.domain.tld, etc)
        :rtype: str

        """
        if not couchconfig:
            raise ImportError('couchconfig not installed')
        return couchconfig.service_notation(fqdn)

    def _load_bs4_value(self, value):
        """Load an HTML or XML string into an lxml etree object.

        :param str value: The HTML or XML string
        :rtype: bs4.BeautifulSoup
        :raises: ConsumerException

        """
        if not bs4:
            raise ConsumerException('BeautifulSoup is not enabled')
        return bs4.BeautifulSoup(value)

    def _load_csv_value(self, value):
        """Create a csv.DictReader instance for the sniffed dialect for the
        value passed in.

        :param str value: The CSV value
        :rtype: csv.DictReader

        """
        csv_buffer = stringio.StringIO(value)
        dialect = csv.Sniffer().sniff(csv_buffer.read(1024))
        csv_buffer.seek(0)
        return csv.DictReader(csv_buffer, dialect=dialect)

    def _load_json_value(self, value):
        """Deserialize a JSON string returning the native Python data type
        for the value.

        :param str value: The JSON string
        :rtype: object

        """
        return json.loads(value, use_decimal=True)

    def _load_pickle_value(self, value):
        """Deserialize a pickle string returning the native Python data type
        for the value.

        :param str value: The pickle string
        :rtype: object

        """
        return pickle.loads(value)

    def _load_plist_value(self, value):
        """Deserialize a plist string returning the native Python data type
        for the value.

        :param str value: The pickle string
        :rtype: dict

        """
        return plistlib.readPlistFromString(value)

    def _load_yaml_value(self, value):
        """Load an YAML string into an dict object.

        :param str value: The YAML string
        :rtype: any
        :raises: ConsumerException

        """
        return yaml.load(value)

    def _process(self):
        """Extend this method for implementing your Consumer logic.

        If the message can not be processed and the Consumer should stop after
        n failures to process messages, raise the ConsumerException.

        :raises: ConsumerException
        :raises: NotImplementedError

        """
        raise NotImplementedError

    def _publish_message(self, exchange, routing_key, properties, body,
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
        :param rejected.data.Properties: The message properties
        :param no_serialization: Turn off auto-serialization of the body
        :param no_encoding: Turn off auto-encoding of the body

        """
        # Convert the rejected.data.Properties object to a pika.BasicProperties
        logger.debug('Converting properties')
        properties_out = self._get_pika_properties(properties)

        # Auto-serialize the content if needed
        if (not no_serialization and not isinstance(body, basestring) and
            properties.content_type):
            logger.debug('Auto-serializing message body')
            body = self._auto_serialize(properties.content_type, body)

        # Auto-encode the message body if needed
        if not no_encoding and properties.content_encoding:
            logger.debug('Auto-encoding message body')
            body = self._auto_encode(properties.content_encoding, body)

        # Publish the message
        logger.debug('Publishing message to %s:%s', exchange, routing_key)
        self._channel.basic_publish(exchange=exchange,
                                    routing_key=routing_key,
                                    properties=properties_out,
                                    body=body)

    def _reply(self, response_body, properties, auto_id=True,
               exchange=None, reply_to=None):
        """Reply to the received message.

        If auto_id is True, a new uuid4 value will be generated for the
        message_id and correlation_id will be set to the message_id of the
        original message. In addition, the timestamp will be assigned the
        current time of the message. If auto_id is False, neither the message_id
        or the correlation_id will be changed in the properties.

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
        :param rejected.data.Properties properties: Message properties to use
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
            logger.debug('New message_id: %s', properties.message_id)
            logger.debug('Correlation_id: %s', properties.correlation_id)

        # Redefine the reply to if needed
        reply_to = reply_to or properties.reply_to

        # Wipe out reply_to if it's set
        if properties.reply_to:
            properties.reply_to = None

        self._publish_message(exchange or self._message.exchange,
                              reply_to,
                              properties,
                              response_body)


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
