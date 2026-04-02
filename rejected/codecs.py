"""Async message codec with content-type dispatch and Avro support.

Handles serialization, deserialization, compression, and decompression
of message bodies. Avro schema loading is async via file I/O or HTTP.

"""

from __future__ import annotations

import asyncio
import bz2
import csv
import io
import json
import logging
import pathlib
import pickle
import plistlib
import typing
import zlib

import yaml

from . import models

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

try:
    import httpx
except ImportError:
    httpx = None

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


class Codec:
    """Async message codec with optional Avro schema support.

    :param schema_registry: Schema registry configuration for Avro.
        If ``None``, Avro encoding/decoding is not available.

    """

    def __init__(
        self, schema_registry: models.SchemaRegistryConfig | None = None
    ) -> None:
        self._schema_registry = schema_registry
        self._avro_schemas: dict[str, dict[str, typing.Any]] = {}
        self._schema_lock = asyncio.Lock()
        self._http_client: typing.Any | None = None

    async def decode(
        self,
        body: bytes,
        content_type: str | None,
        content_encoding: str | None,
        message_type: str | None = None,
    ) -> typing.Any:
        """Decode and deserialize a message body.

        Handles content_encoding (gzip, bzip2) first, then
        content_type dispatch including Avro if configured.

        """
        if content_encoding == 'bzip2':
            body = bz2.decompress(body)
        elif content_encoding == 'gzip':
            body = zlib.decompress(body)

        if (
            fastavro is not None
            and content_type == AVRO_DATUM_MIME_TYPE
            and message_type
        ):
            schema = await self._avro_schema(message_type)
            return fastavro.schemaless_reader(io.BytesIO(body), schema)

        if content_type == 'application/json':
            return _load_json(body)
        if umsgpack and content_type == 'application/msgpack':
            return _load_msgpack(body)
        if content_type in PICKLE_MIME_TYPES:
            return pickle.loads(body)
        if content_type == 'application/x-plist':
            return plistlib.loads(body)
        if content_type == 'text/csv':
            return _load_csv(body)
        if bs4 and content_type in BS4_MIME_TYPES:
            return _load_bs4(body)
        if content_type in YAML_MIME_TYPES:
            return yaml.safe_load(body)

        return body

    async def encode(
        self,
        body: typing.Any,
        content_type: str | None,
        content_encoding: str | None,
        message_type: str | None = None,
    ) -> typing.Any:
        """Serialize and encode a message body.

        Handles content_type serialization first (if body is not
        str/bytes), then content_encoding compression.

        """
        if not isinstance(body, (str, bytes)):
            if (
                fastavro
                and content_type == AVRO_DATUM_MIME_TYPE
                and message_type
            ):
                schema = await self._avro_schema(message_type)
                stream = io.BytesIO()
                fastavro.schemaless_writer(stream, schema, body)
                body = stream.getvalue()
            elif content_type == 'application/json':
                body = json.dumps(body, ensure_ascii=True).encode('utf-8')
            elif umsgpack and content_type == 'application/msgpack':
                body = umsgpack.packb(body)
            elif content_type in PICKLE_MIME_TYPES:
                body = pickle.dumps(body)
            elif content_type == 'application/x-plist':
                body = plistlib.dumps(body)
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

        if content_encoding:
            if isinstance(body, str):
                body = body.encode('utf-8')
            if content_encoding == 'gzip':
                body = zlib.compress(body)
            elif content_encoding == 'bzip2':
                body = bz2.compress(body)

        return body

    async def close(self) -> None:
        """Close the HTTP client if one was created."""
        if self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None

    # --- Avro schema management ---

    async def _avro_schema(self, message_type: str) -> dict[str, typing.Any]:
        """Return the parsed Avro schema, loading and caching on
        first access. Lock prevents duplicate fetches under
        concurrent message processing."""
        if message_type in self._avro_schemas:
            return self._avro_schemas[message_type]
        async with self._schema_lock:
            if message_type not in self._avro_schemas:
                self._avro_schemas[
                    message_type
                ] = await self._load_avro_schema(message_type)
            return self._avro_schemas[message_type]

    async def _load_avro_schema(
        self, message_type: str
    ) -> dict[str, typing.Any]:
        """Load a schema from the configured registry."""
        if not self._schema_registry or not self._schema_registry.uri:
            raise DecodeError(
                'No schema_registry configured; cannot load Avro '
                f'schema for {message_type}'
            )
        registry = self._schema_registry
        uri = registry.uri.format(message_type)
        LOGGER.debug('Loading Avro schema for %s from %s', message_type, uri)

        if registry.type == 'file':
            return self._load_file_schema(uri)
        if registry.type == 'http':
            return await self._load_http_schema(uri, message_type)

        raise DecodeError(
            f'Unsupported schema registry type: {registry.type!r}'
        )

    @staticmethod
    def _load_file_schema(uri: str) -> dict[str, typing.Any]:
        """Load a schema from a file:// URI."""
        file_path = pathlib.Path(uri.removeprefix('file://'))
        try:
            result: dict[str, typing.Any] = json.loads(file_path.read_text())
            return result
        except FileNotFoundError:
            raise DecodeError(
                f'Missing Avro schema file: {file_path}'
            ) from None

    async def _load_http_schema(
        self, uri: str, message_type: str
    ) -> dict[str, typing.Any]:
        """Load a schema from an HTTP(S) endpoint using httpx."""
        if not httpx:
            raise DecodeError(
                'httpx is required for HTTP schema loading; '
                'install rejected[avro]'
            )
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(timeout=30)
        response = await self._http_client.get(uri)
        if response.status_code != 200:
            raise DecodeError(
                f'Failed to fetch Avro schema for {message_type}: '
                f'HTTP {response.status_code}'
            )
        schema: dict[str, typing.Any] = response.json()
        return schema


# --- Internal helpers (stateless, sync) ---


def _load_json(value: bytes | str) -> typing.Any:
    """Deserialize a JSON value."""
    if isinstance(value, bytes):
        value = value.decode('utf-8')
    try:
        return json.loads(value)
    except ValueError as error:
        raise DecodeError(str(error)) from error


def _load_msgpack(value: bytes) -> typing.Any:
    """Deserialize a MessagePack value."""
    try:
        return umsgpack.unpackb(value)
    except ValueError as error:
        raise DecodeError(str(error)) from error


def _load_csv(value: bytes | str) -> csv.DictReader[str]:
    """Deserialize a CSV value into a DictReader."""
    if isinstance(value, bytes):
        value = value.decode('utf-8')
    csv_buffer = io.StringIO(value)
    dialect = csv.Sniffer().sniff(csv_buffer.read(1024))
    csv_buffer.seek(0)
    return csv.DictReader(csv_buffer, dialect=dialect)


def _load_bs4(value: bytes | str) -> typing.Any:
    """Parse HTML or XML into a BeautifulSoup object."""
    if not bs4:
        raise DecodeError('BeautifulSoup4 is not enabled')
    if isinstance(value, bytes):
        value = value.decode('utf-8')
    return bs4.BeautifulSoup(value, 'html.parser')


def _dump_csv(value: list[list[typing.Any]]) -> str:
    """Serialize a list of rows to CSV."""
    buff = io.StringIO()
    writer = csv.writer(buff, quotechar='"', quoting=csv.QUOTE_ALL)
    writer.writerows(value)
    buff.seek(0)
    result = buff.read()
    buff.close()
    return result
