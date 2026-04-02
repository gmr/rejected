"""Stateless serialization and deserialization functions for message bodies.

Handles content_type dispatch (JSON, msgpack, pickle, plist, CSV, HTML/XML,
YAML) and content_encoding (gzip, bzip2).

"""

import bz2
import csv
import io
import json
import logging
import pickle
import plistlib
import zlib

import yaml

LOGGER = logging.getLogger(__name__)

# Optional imports
try:
    import bs4
except ImportError:
    bs4 = None

try:
    import umsgpack
except ImportError:
    umsgpack = None

try:
    import fastavro
except ImportError:
    fastavro = None

AVRO_DATUM_MIME_TYPE = 'application/vnd.apache.avro.datum'

BS4_MIME_TYPES = ('text/html', 'text/xml')
PICKLE_MIME_TYPES = (
    'application/pickle',
    'application/x-pickle',
    'application/x-vnd.python.pickle',
    'application/vnd.python.pickle',
)
YAML_MIME_TYPES = ('text/yaml', 'text/x-yaml')


class DecodeError(Exception):
    """Raised when a message body cannot be decoded or deserialized."""


class EncodeError(Exception):
    """Raised when a message body cannot be serialized or encoded."""


def decode(body, content_type, content_encoding):
    """Decode and deserialize a message body.

    Handles content_encoding (gzip, bzip2) first, then content_type dispatch.
    Returns the raw body if no matching codec is found.

    :param bytes body: The raw message body
    :param str|None content_type: MIME content type
    :param str|None content_encoding: Content encoding (gzip, bzip2)
    :returns: The decoded/deserialized body
    :raises DecodeError: When deserialization fails

    """
    # Handle content encoding first
    if content_encoding == 'bzip2':
        body = bz2.decompress(body)
    elif content_encoding == 'gzip':
        body = zlib.decompress(body)

    # Handle content type
    if content_type == 'application/json':
        return _load_json(body)
    if umsgpack and content_type == 'application/msgpack':
        return _load_msgpack(body)
    if content_type in PICKLE_MIME_TYPES:
        return pickle.loads(body)
    if content_type == 'application/x-plist':
        return _load_plist(body)
    if content_type == 'text/csv':
        return _load_csv(body)
    if bs4 and content_type in BS4_MIME_TYPES:
        return _load_bs4(body)
    if content_type in YAML_MIME_TYPES:
        return yaml.safe_load(body)

    return body


def encode(body, content_type, content_encoding):
    """Serialize and encode a message body.

    Handles content_type serialization first (if body is not str/bytes),
    then content_encoding.
    Returns body unchanged if no matching codec is found.

    :param body: The message body to serialize
    :param str|None content_type: MIME content type
    :param str|None content_encoding: Content encoding (gzip, bzip2)
    :returns: The serialized/encoded body
    :raises EncodeError: When serialization fails

    """
    # Serialize by content type if body is not already str/bytes
    if not isinstance(body, (str, bytes)):
        if content_type == 'application/json':
            body = json.dumps(body, ensure_ascii=True).encode('utf-8')
        elif umsgpack and content_type == 'application/msgpack':
            body = umsgpack.packb(body)
        elif content_type in PICKLE_MIME_TYPES:
            body = pickle.dumps(body)
        elif content_type == 'application/x-plist':
            body = _dump_plist(body)
        elif content_type == 'text/csv':
            body = _dump_csv(body)
        elif (
            bs4
            and isinstance(body, bs4.BeautifulSoup)
            and content_type in BS4_MIME_TYPES
        ):
            body = str(body)
        elif content_type in YAML_MIME_TYPES:
            body = yaml.dump(body)

    # Handle content encoding
    if content_encoding:
        if not isinstance(body, bytes):
            if isinstance(body, str):
                body = body.encode('utf-8')
        if content_encoding == 'gzip':
            body = zlib.compress(body)
        elif content_encoding == 'bzip2':
            body = bz2.compress(body)

    return body


def decode_avro(body, schema):
    """Deserialize an Avro datum.

    :param bytes body: The Avro-encoded bytes
    :param dict schema: The parsed Avro schema
    :returns: The deserialized data
    :rtype: dict

    """
    if not fastavro:
        raise DecodeError(
            'fastavro is required for Avro support; install rejected[avro]'
        )
    return fastavro.schemaless_reader(io.BytesIO(body), schema)


def encode_avro(body, schema):
    """Serialize to an Avro datum.

    :param dict body: The data to serialize
    :param dict schema: The parsed Avro schema
    :returns: The Avro-encoded bytes
    :rtype: bytes

    """
    if not fastavro:
        raise EncodeError(
            'fastavro is required for Avro support; install rejected[avro]'
        )
    stream = io.BytesIO()
    fastavro.schemaless_writer(stream, schema, body)
    return stream.getvalue()


# --- Internal helpers ---


def _load_json(value):
    """Deserialize a JSON value.

    :param str|bytes value: The JSON string or bytes
    :returns: The deserialized Python object
    :raises DecodeError: If the value is not valid JSON

    """
    if isinstance(value, bytes):
        value = value.decode('utf-8')
    try:
        return json.loads(value)
    except ValueError as error:
        raise DecodeError(str(error)) from error


def _load_msgpack(value):
    """Deserialize a MessagePack value.

    :param bytes value: The msgpack bytes
    :returns: The deserialized Python object
    :raises DecodeError: If the value cannot be unpacked

    """
    try:
        return umsgpack.unpackb(value)
    except ValueError as error:
        raise DecodeError(str(error)) from error


def _load_plist(value):
    """Deserialize a plist value.

    :param bytes value: The plist bytes
    :returns: The deserialized Python object (typically a dict)

    """
    if hasattr(plistlib, 'loads'):
        return plistlib.loads(value)
    try:
        return plistlib.readPlistFromString(value)
    except AttributeError:
        return plistlib.readPlistFromBytes(value)


def _load_csv(value):
    """Deserialize a CSV value into a :class:`csv.DictReader`.

    The dialect is auto-detected from the first 1024 bytes.

    :param str|bytes value: The CSV string or bytes
    :returns: A DictReader over the parsed rows
    :rtype: csv.DictReader

    """
    if isinstance(value, bytes):
        value = value.decode('utf-8')
    csv_buffer = io.StringIO(value)
    dialect = csv.Sniffer().sniff(csv_buffer.read(1024))
    csv_buffer.seek(0)
    return csv.DictReader(csv_buffer, dialect=dialect)


def _load_bs4(value):
    """Parse an HTML or XML string into a BeautifulSoup object.

    :param str|bytes value: The HTML or XML string
    :returns: The parsed document
    :rtype: bs4.BeautifulSoup
    :raises DecodeError: If BeautifulSoup is not installed

    """
    if not bs4:
        raise DecodeError('BeautifulSoup4 is not enabled')
    if isinstance(value, bytes):
        value = value.decode('utf-8')
    return bs4.BeautifulSoup(value, 'html.parser')


def _dump_plist(value):
    """Serialize a dict to plist format.

    :param dict value: The value to serialize
    :returns: The plist bytes
    :rtype: bytes

    """
    if hasattr(plistlib, 'dumps'):
        return plistlib.dumps(value)
    try:
        return plistlib.writePlistToString(value).encode('utf-8')
    except AttributeError:
        return plistlib.writePlistToBytes(value)


def _dump_csv(value):
    """Serialize a list of rows to CSV format.

    :param list value: A list of lists (rows) to serialize
    :returns: The CSV string
    :rtype: str

    """
    buff = io.StringIO()
    writer = csv.writer(buff, quotechar='"', quoting=csv.QUOTE_ALL)
    writer.writerows(value)
    buff.seek(0)
    result = buff.read()
    buff.close()
    return result
