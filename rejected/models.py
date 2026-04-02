"""Pydantic models for rejected configuration, messages, and data."""

import datetime
import enum
import typing

import pydantic

# Configuration models


class ConnectionRef(pydantic.BaseModel):
    """A named connection reference used in a consumer's connections list."""

    name: str
    consume: bool = True
    confirm: bool = False


class ConnectionConfig(pydantic.BaseModel):
    """A single RabbitMQ connection configuration."""

    model_config = pydantic.ConfigDict(populate_by_name=True)

    host: str = 'localhost'
    port: int = 5672
    user: str = 'guest'
    password: str = pydantic.Field('guest', alias='pass')
    ssl: bool = False
    vhost: str = '/'
    heartbeat_interval: int = 300
    frame_max: int = 131072
    socket_timeout: int = 10
    ssl_options: dict[str, typing.Any] = pydantic.Field(default_factory=dict)


class StatsdConfig(pydantic.BaseModel):
    enabled: bool = False
    host: str = 'localhost'
    port: int = 8125
    prefix: str = 'rejected'
    tcp: bool = False
    include_hostname: bool = True


class PrometheusConfig(pydantic.BaseModel):
    enabled: bool = False
    port: int = 9090


class StatsConfig(pydantic.BaseModel):
    log: bool = False
    prometheus: PrometheusConfig = pydantic.Field(
        default_factory=PrometheusConfig
    )
    statsd: StatsdConfig = pydantic.Field(default_factory=StatsdConfig)


class ConsumerConfig(pydantic.BaseModel):
    consumer: str | None = None
    connections: list[str | ConnectionRef] = pydantic.Field(
        default_factory=list
    )
    qty: int = 1
    queue: str | None = None
    ack: bool = True
    qos_prefetch: int = 1
    max_errors: int = 5
    error_exchange: str | None = None
    schema_uri_format: str | None = None
    sentry_dsn: str | None = None
    drop_exchange: str | None = None
    drop_invalid_messages: bool | None = None
    message_type: str | None = None
    error_max_retry: int | None = None
    config: dict[str, typing.Any] = pydantic.Field(default_factory=dict)


class Config(pydantic.BaseModel):
    """Application configuration."""

    model_config = pydantic.ConfigDict(populate_by_name=True)

    poll_interval: float = 60.0
    sentry_dsn: str | None = None
    stats: StatsConfig = pydantic.Field(default_factory=StatsConfig)
    connections: dict[str, ConnectionConfig] = pydantic.Field(
        default_factory=dict, alias='Connections'
    )
    consumers: dict[str, ConsumerConfig] = pydantic.Field(
        default_factory=dict, alias='Consumers'
    )
    logging: dict[str, typing.Any] = pydantic.Field(default_factory=dict)


# Message model


class Message(pydantic.BaseModel):
    """A fully deserialized AMQP message.

    All properties are pre-populated and the body is already deserialized
    based on the message's ``content_type`` and ``content_encoding``.

    Passed to :meth:`~rejected.consumer.FunctionalConsumer.prepare`,
    :meth:`~rejected.consumer.FunctionalConsumer.process`, and
    :meth:`~rejected.consumer.FunctionalConsumer.finish`.

    """

    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)

    body: bytes

    app_id: str | None = None
    content_encoding: str | None = None
    content_type: str | None = None
    correlation_id: str | None = None
    exchange: str = ''
    expiration: str | None = None
    headers: dict[str, bool | dict | float | int | str | bytes] = (
        pydantic.Field(default_factory=dict)
    )
    message_id: str | None = None
    message_type: str | None = None
    priority: int | None = None
    redelivered: bool = False
    reply_to: str | None = None
    returned: bool = False
    routing_key: str
    timestamp: datetime.datetime | int | None = None
    user_id: str | None = None


# Result codes


class Result(enum.IntEnum):
    """Result codes returned by Consumer.execute() to indicate how the
    message should be handled by the process."""

    MESSAGE_ACK = 1
    MESSAGE_DROP = 2
    MESSAGE_REQUEUE = 3
    CONSUMER_EXCEPTION = 10
    MESSAGE_EXCEPTION = 11
    PROCESSING_EXCEPTION = 12
    UNHANDLED_EXCEPTION = 13


# Internal AMQP transport models


class Properties(pydantic.BaseModel):
    """AMQP Basic.Properties as a pydantic model."""

    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)

    app_id: str | None = None
    content_encoding: str | None = None
    content_type: str | None = None
    correlation_id: str | None = None
    delivery_mode: int | None = None
    expiration: str | None = None
    headers: dict[str, typing.Any] | None = None
    message_id: str | None = None
    priority: int | None = None
    reply_to: str | None = None
    timestamp: int | float | None = None
    type: str | None = None
    user_id: str | None = None

    @classmethod
    def from_pika(cls, properties) -> 'Properties':
        """Create from a :class:`pika.spec.BasicProperties` instance.

        :param properties: Pika properties object
        :type properties: pika.spec.BasicProperties

        """
        if properties is None:
            return cls()
        return cls(
            **{
                attr: getattr(properties, attr, None)
                for attr in cls.model_fields
            }
        )


class InternalMessage(pydantic.BaseModel):
    """Internal representation of an AMQP message as received from pika.

    This is the framework's transport object — not the public
    :class:`Message` model passed to ``FunctionalConsumer``.

    """

    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)

    body: typing.Any = b''
    channel: typing.Any = None
    connection: typing.Any = ''
    consumer_tag: str = ''
    delivery_tag: int = 0
    exchange: str = ''
    method: typing.Any = None
    properties: Properties = pydantic.Field(default_factory=Properties)
    redelivered: bool = False
    returned: bool = False
    routing_key: str = ''

    @classmethod
    def from_pika(
        cls,
        connection: str,
        channel: typing.Any,
        method: typing.Any,
        properties: typing.Any,
        body: bytes,
        returned: bool = False,
    ) -> 'InternalMessage':
        """Create from pika delivery callback arguments.

        :param str connection: The connection name
        :param channel: The pika channel
        :param method: The pika method frame
        :param properties: The pika BasicProperties
        :param bytes body: The message body
        :param bool returned: Whether the message was returned

        """
        return cls(
            body=body,
            channel=channel,
            connection=connection,
            consumer_tag=getattr(method, 'consumer_tag', ''),
            delivery_tag=getattr(method, 'delivery_tag', 0),
            exchange=getattr(method, 'exchange', ''),
            method=method,
            properties=Properties.from_pika(properties),
            redelivered=getattr(method, 'redelivered', False),
            returned=returned,
            routing_key=getattr(method, 'routing_key', ''),
        )
