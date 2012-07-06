"""Base rejected message Consumer class that aims at simplifying the process
of writing message Consumers that enforce good behaviors in dealing with
messages.

"""
__author__ = 'gmr'
__since__ = '2012-06-06'
__version__ = '1.0.0'

import bz2
import csv
import datetime
try:
    import simplejson as json
except ImportError:
    import json
import logging
import pickle
import plistlib
import StringIO as stringio
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
    import redis
except ImportError:
    logger.warning('redis-py not found, disabling Redis support')
    redis = None

try:
    import pgsql_wrapper
except ImportError:
    logger.warning('pgsql_wrapper not found, disabling PostgreSQL support')
    pgsql_wrapper = None


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

    """
    _CONFIG_KEYS = ['service', 'config_host', 'config_domain', 'config_ttl']
    _DROP_INVALID_MESSAGES = True
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
        self._configuration = configuration

        # Automatically add a config object if the values are present
        self._config = None
        if couchconfig and [key for key in self._CONFIG_KEYS
                            if key in configuration.keys()]:
            self._config = self._get_configuration_obj(configuration)

        # Each message received will be carried as an attribute
        self._message = None
        self._message_body = None

        # Call the initialize method
        self._initialize()

    def _initialize(self):
        """Extend this method for actions to take in initializing the Consumer
        instead of extending __init__.

        """
        pass

    def _process(self):
        """Extend this method for implementing your Consumer logic.

        If the message can not be processed and the Consumer should stop after
        n failures to process messages, raise the ConsumerException.

        :raises: ConsumerException
        :raises: NotImplementedError

        """
        raise NotImplementedError

    def _decode_bz2(self, value):
        """Return a bz2 decompressed value.

        :param str value: Compressed value
        :rtype: str

        """
        return bz2.decompress(value)

    def _decode_gzip(self, value):
        """Return a zlib decompressed value.

        :param str value: Compressed value
        :rtype: str

        """
        return zlib.decompress(value)

    def _get_configuration_obj(self, config):
        """Return a new instance of couchconfig.Configuration with the service
        defined in normal, forward DNS notation (service.domain.tld).

        :param dict config: Configuration dictionary
        :rtype: couchconfig.Configuration
        :raises: ImportError

        """
        if not pgsql_wrapper:
            raise ImportError('Could not import couchconfig for configuration '
                              'service support')

        service = self._get_service(config.get('service'))
        return couchconfig.Configuration(service,
                                         config.get('config_host'),
                                         config.get('config_domain'),
                                         config.get('config_ttl'))

    def _get_postgresql_cursor(self, host, port, dbname, user,
                               password=None):
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
            raise ImportError('Could not import pgsql_wrapper for PostgreSQL '
                              'support')
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
            raise ImportError('Could not import redis for redis support')
        return redis.Redis(host=host, port=port, db=db)

    def _get_service(self, fqdn):
        """Return the service notation for the fqdn value (tld_domain_service).

        :param str fqdn: The fqdn value (service.domain.tld, etc)
        :rtype: str

        """
        return couchconfig.service_notation(fqdn)

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

    def _load_html_value(self, value):
        """Load an HTML string into an bs4.BeautifulSoup object.

        :param str value: The HTML string
        :rtype: bs4.BeautifulSoup
        :raises: ConsumerException

        """
        if not bs4:
            raise ConsumerException('BeautifulSoup is not enabled')
        return bs4.BeautifulSoup(value)

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

    def _load_xml_value(self, value):
        """Load an XML string into an lxml etree object.

        :param str value: The XML string
        :rtype: bs4.BeautifulSoup
        :raises: ConsumerException

        """
        if not bs4:
            raise ConsumerException('BeautifulSoup is not enabled')
        return bs4.BeautifulSoup(value)

    def _load_yaml_value(self, value):
        """Load an YAML string into an dict object.

        :param str value: The YAML string
        :rtype: any
        :raises: ConsumerException

        """
        if not bs4:
            raise ConsumerException('YAML is not enabled')
        return yaml.load(value)

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
            logger.debug('Coerced an incorrect content-encoding of utf-8 to '
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

        # If it's JSON, auto-deserialize it
        if self.message_content_type == 'application/json':
            self._message_body = self._load_json_value(self._message_body)

        # If it's pickled, auto unpickle it
        elif self.message_content_type in self._PICKLE_MIME_TYPES:
            self._message_body = self._load_pickle_value(self._message_body)

        elif self.message_content_type == 'application/x-plist':
            self._message_body = self._load_plist_value(self._message_body)

        elif self.message_content_type == 'text/csv':
            self._message_body = self._load_csv_value(self._message_body)

        # If it's HTML, load it into a BeautifulSoup object
        elif bs4 and self.message_content_type == 'text/html':
            self._message_body = self._load_html_value(self._message_body)

        # If it's XML, load the content into lxml.etree
        elif bs4 and self.message_content_type == 'text/xml':
            self._message_body = self._load_xml_value(self._message_body)

        # If it's YAML, load the content via pyyaml into a dict
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

    def process(self, message_in):
        """Process the message from RabbitMQ. To implement logic for processing
        a message, extend Consumer._process, not this method.

        :param rejected.Consumer.Message message_in: The message to process
        :rtype: bool

        """
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

        # Let the child object process the message
        self._process()


class ConsumerException(Exception):
    """May be called when processing a message to indicate a problem that the
    Consumer may be experiencing that should cause it to stop.

    """
    pass
