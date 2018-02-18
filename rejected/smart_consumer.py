"""
SmartConsumer
=============
Auto encoding/decoding and serialization/deserialization consumer.

"""
import importlib
import io
import pkg_resources
import sys

import flatdict
from ietfparse import headers

from rejected import consumer
from rejected.errors import *


try:  # Python 3 unicode class support
    unicode()
except NameError:
    unicode = str

_PYTHON3 = True if sys.version_info > (3, 0, 0) else False


class SmartConsumer(consumer.Consumer):
    """Base class to ease the implementation of strongly typed message
    consumers that validate and automatically decode and deserialize the
    inbound message body based upon the message's
    :class:`content_type <rejected.data.Properties>` property. Additionally,
    should one of the supported
    :class:`content_encoding <rejected.data.Properties>` types (``gzip`` or
    ``bzip2``) be specified in the message's property, it will automatically
    be decoded.

    When publishing a message, the message can be automatically serialized
    and encoded. If the :class:`content_type <rejected.data.Properties>`
    property is specified, the consumer will attempt to automatically serialize
    the outgoing message body. If the
    :class:`content_encoding <rejected.data.Properties>` property is
    specified using a supported encoding (``gzip`` or ``bzip2``), it will
    automatically be encoded as well.

    The consumer not attempt to encode or decode message bodies when the
    message's :class:`content_type <rejected.data.Properties>` is either
    unsupported or set to either ``application/octet-stream`` or
    ``text/plain``.

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

    _IGNORE_TYPES = {'application/octet-stream', 'text/plain'}

    _CODEC_MAP = {
        'bzip2': 'bz2',
        'gzip': 'zlib',
        'zlib': 'zlib'
    }

    _SERIALIZATION_MAP = flatdict.FlatDict({
        'application': {
            'json': {
                'module': 'json',
                'load': 'loads',
                'dump': 'dumps',
                'encoding': True
            },
            'msgpack': {
                'module': 'umsgpack',
                'load': 'unpackb',
                'dump': 'packb',
                'binary': True,
                'enabled': False
            },
            'pickle': {
                'module': 'pickle',
                'load': 'loads',
                'dump': 'dumps',
                'binary': True
            },
            'x-pickle': {
                'module': 'pickle',
                'load': 'loads',
                'dump': 'dumps',
                'binary': True
            },
            'x-plist': {
                'module': 'plistlib',
                'load': 'loads' if _PYTHON3 else 'readPlistFromString',
                'dump': 'dumps' if _PYTHON3 else 'writePlistToString',
                'binary': True
            },
            'vnd.python.pickle': {
                'module': 'pickle',
                'load': 'loads',
                'dump': 'dumps',
                'binary': True
            },
            'x-vnd.python.pickle': {
                'module': 'pickle',
                'load': 'loads',
                'dump': 'dumps',
                'binary': True
            }
        },
        'text': {
            'csv': {
                'module': 'csv',
                'load': '_load_csv',
                'dump': '_dump_csv',
                'common': False
            },
            'html': {
                'module': 'bs4',
                'load': '_load_html',
                'dump': '_dump_bs4',
                'common': False,
                'enabled': False
            },
            'xml': {
                'module': 'bs4',
                'load': '_load_xml',
                'dump': '_dump_bs4',
                'common': False,
                'enabled': False
            },
            'yaml': {
                'module': 'yaml',
                'load': 'load',
                'dump': 'dump'
            }
        }
    }, delimiter='/')

    def __init__(self, *args, **kwargs):
        """Creates a new instance of the
        :class:`~rejected.consumer.SmartConsumer` class. To perform
        initialization tasks, extend
        :meth:`~rejected.consumer.SmartConsumer.initialize` or ensure you
        :meth:`super` this method first.

        """
        super(SmartConsumer, self).__init__(*args, **kwargs)
        installed = pkg_resources.AvailableDistributions()
        for key, pkg in {'application/msgpack': 'u-msgpack-python',
                         'text/html': ('beautifulsoup4', 'lxml'),
                         'text/xml': ('beautifulsoup4', 'lxml')}.items():
            if isinstance(pkg, tuple):
                self._SERIALIZATION_MAP[key]['enabled'] = \
                    all([p in installed for p in pkg])
            else:
                self._SERIALIZATION_MAP[key]['enabled'] = pkg in installed
            self.logger.debug(
                '%s is %s in serialization map', key,
                'enabled' if self._SERIALIZATION_MAP[key]['enabled'] else
                'disabled')

    def publish_message(self,
                        exchange,
                        routing_key,
                        properties,
                        body,
                        no_serialization=False,
                        no_encoding=False,
                        channel=None,
                        connection=None):
        """Publish a message to RabbitMQ on the same channel the original
        message was received on.

        By default, if you pass a non-string object to the body and the
        properties have a supported ``content_type`` set, the body will be
        auto-serialized in the specified ``content_type``.

        If the properties do not have a timestamp set, it will be set to the
        current time.

        If you specify a ``content_encoding`` in the properties and the
        encoding is supported, the body will be auto-encoded.

        Both of these behaviors can be disabled by setting
        ``no_serialization`` or ``no_encoding`` to ``True``.

        If you pass an unsupported content-type or content-encoding when using
        the auto-serialization and auto-encoding features, a :exc:`ValueError`
        will be raised.

        .. versionchanged:: 4.0.0
           The method returns a :py:class:`~tornado.concurrent.Future` if
           `publisher confirmations <https://www.rabbitmq.com/confirms.html>`_
           are enabled on for the connection. In addition, The ``channel``
           parameter is deprecated and will be removed in a future release.

        :param str exchange: The exchange to publish to
        :param str routing_key: The routing key to publish with
        :param dict properties: The message properties
        :param mixed body: The message body to publish
        :param bool no_serialization: Turn off auto-serialization of the body
        :param bool no_encoding: Turn off auto-encoding of the body
        :param str channel: **Deprecated in 4.0.0** Specify the connection
            parameter instead.
        :param str connection: The connection to use. If it is not
            specified, the channel that the message was delivered on is used.
        :rtype: tornado.concurrent.Future or None
        :raises: ValueError

        """
        # Auto-serialize the content if needed
        is_string = (isinstance(body, str) or isinstance(body, bytes)
                     or isinstance(body, unicode))
        if not no_serialization and not is_string and \
                properties.get('content_type'):
            body = self._serialize(
                body, headers.parse_content_type(properties['content_type']))

        # Auto-encode the message body if needed
        if not no_encoding and \
                properties.get('content_encoding') in self._CODEC_MAP.keys():
            body = self._compress(
                body, self._CODEC_MAP[properties['content_encoding']])

        return super(SmartConsumer, self).publish_message(
            exchange, routing_key, properties, body, channel or connection)

    @property
    def body(self):
        """Return the message body, unencoded if needed,
        deserialized if possible.

        :rtype: any

        """
        if self._message_body:  # If already set, return it
            return self._message_body
        self._message_body = self._maybe_decompress_body()
        self._message_body = self._maybe_deserialize_body()
        return self._message_body

    def _compress(self, value, module_name):
        """Compress the value passed in using the named compression module.

        :param bytes value: The uncompressed value
        :rtype: bytes

        """
        self.logger.debug('Decompressing with %s', module_name)
        if not isinstance(value, bytes):
            value = value.encode('utf-8')
        return self._maybe_import(module_name).compress(value)

    @staticmethod
    def _dump_bs4(value):
        """Return a BeautifulSoup object as a string

        :param bs4.BeautifulSoup value: The object to return a string from
        :rtype: str

        """
        return str(value)

    def _dump_csv(self, rows):
        """Take a list of dicts and return it as a CSV value. The

        .. versionchanged:: 4.0.0

        :param list rows: A list of lists to return as a CSV
        :rtype: str

        """
        self.logger.debug('Writing %r', rows)
        csv = self._maybe_import('csv')
        buff = io.StringIO() if _PYTHON3 else io.BytesIO()
        writer = csv.DictWriter(
            buff,
            sorted(set([k for r in rows for k in r.keys()])),
            dialect='excel')
        writer.writeheader()
        writer.writerows(rows)
        value = buff.getvalue()
        buff.close()
        return value

    def _load_bs4(self, value, markup):
        """Load an HTML or XML string into a :class:`bs4.BeautifulSoup`
        instance.

        :param str value: The HTML or XML string
        :param str markup: One of ``html`` or ``xml``
        :rtype: bs4.BeautifulSoup
        :raises: ConsumerException

        """
        return self._maybe_import('bs4').BeautifulSoup(
            value, 'lxml' if markup == 'html' else 'lxml-xml')

    def _load_csv(self, value):
        """Return a class:`csv.DictReader` instance for value passed in.

        :param str value: The CSV value
        :rtype: csv.DictReader

        """
        csv = self._maybe_import('csv')
        buff = io.StringIO() if _PYTHON3 else io.BytesIO()
        buff.write(value)
        buff.seek(0)
        dialect = csv.Sniffer().sniff(buff.read(1024))
        buff.seek(0)
        return csv.DictReader(buff, dialect=dialect)

    def _load_html(self, value):
        """Load a HTML string into a :class:`bs4.BeautifulSoup` instance.

        :param str value: The HTML string
        :rtype: bs4.BeautifulSoup

        """
        return self._load_bs4(value, 'html')

    def _load_xml(self, value):
        """Load a XML string into a :class:`bs4.BeautifulSoup` instance.

        :param str value: The XML string
        :rtype: bs4.BeautifulSoup

        """
        return self._load_bs4(value, 'xml')

    def _maybe_decode(self, value, encoding='utf-8'):
        """If a bytes object is passed in, in the Python 3 environment,
        decode it using the specified encoding to turn it to a str instance.

        :param mixed value: The value to possibly decode
        :param str encoding: The encoding to use
        :rtype: str

        """
        if _PYTHON3 and isinstance(value, bytes):
            try:
                return value.decode(encoding)
            except Exception as err:
                self.logger.exception('Error decoding value: %s', err)
                raise MessageException(
                    str(err), 'decoding-{}'.format(encoding))
        return value

    def _maybe_decompress_body(self):
        """Attempt to decompress the message body passed in using the named
        compression module, if specified.

        :rtype: bytes

        """
        if self.content_encoding:
            if self.content_encoding in self._CODEC_MAP.keys():
                module_name = self._CODEC_MAP[self.content_encoding]
                self.logger.debug('Decompressing with %s', module_name)
                module = self._maybe_import(module_name)
                return module.decompress(self._message.body)
            self.logger.debug('Unsupported content-encoding: %s',
                              self.content_encoding)
        return self._message.body

    def _maybe_deserialize_body(self):
        """Attempt to deserialize the message body based upon the content-type.

        :rtype: mixed

        """
        if not self.content_type:
            return self._message_body
        ct = headers.parse_content_type(self.content_type)
        key = '{}/{}'.format(ct.content_type, ct.content_subtype)
        if key not in self._SERIALIZATION_MAP:
            if key not in self._IGNORE_TYPES:
                self.logger.debug('Unsupported content-type: %s',
                                  self.content_type)
            return self._message_body
        elif not self._SERIALIZATION_MAP[key].get('enabled', True):
            self.logger.debug('%s is not enabled in the serialization map',
                              key)
            return self._message_body
        value = self._message_body
        if not self._SERIALIZATION_MAP[key].get('binary'):
            value = self._maybe_decode(
                self._message_body, ct.parameters.get('charset', 'utf-8'))
        return self._maybe_invoke_serialization(value, 'load', key)

    def _maybe_import(self, module):
        if not hasattr(sys.modules[__name__], module):
            self.logger.debug('Importing %s', module)
            setattr(sys.modules[__name__], module,
                    importlib.import_module(module))
        return sys.modules[module]

    def _maybe_invoke_serialization(self, value, method_type, key):
        if self._SERIALIZATION_MAP[key].get('common', True):
            self.logger.debug('Invoking %s %s using %s', method_type, key,
                              self._SERIALIZATION_MAP[key]['module'])
            method = getattr(
                self._maybe_import(self._SERIALIZATION_MAP[key]['module']),
                self._SERIALIZATION_MAP[key][method_type])
        else:
            method = getattr(self, self._SERIALIZATION_MAP[key][method_type])
        self.logger.debug('Invoking %r for %s %s', method, method_type, key)
        try:
            return method(value)
        except Exception as err:
            self.logger.exception('Error %sing body: %s', method_type, err)
            raise MessageException(
                str(err), 'serialization-{}'.format(method_type))

    def _serialize(self, value, ct):
        """Auto-serialization of the value based upon the content-type value.

        :param any value: The value to serialize
        :param ietfparse.: The content type to serialize
        :rtype: str
        :raises: ValueError

        """
        key = '{}/{}'.format(ct.content_type, ct.content_subtype)
        if key not in self._SERIALIZATION_MAP:
            raise ValueError('Unsupported content-type: {}'.format(key))
        elif not self._SERIALIZATION_MAP[key].get('enabled', True):
            self.logger.debug('%s is not enabled in the serialization map',
                              key)
            raise ValueError('Disabled content-type: {}'.format(key))
        return self._maybe_invoke_serialization(
            self._maybe_decode(value, ct.parameters.get('charset', 'utf-8')),
            'dump', key)
