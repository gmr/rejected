import enum
import typing

import pydantic


class BindingType(str, enum.Enum):
    exchange = 'exchange'
    queue = 'queue'


class LoggingLevel(str, enum.Enum):
    DEBUG = 'DEBUG'
    INFO = 'INFO'
    ERROR = 'ERROR'
    WARNING = 'WARNING'
    CRITICAL = 'CRITICAL'


class Binding(pydantic.BaseModel):
    arguments: dict | None = None
    destination: str
    destination_type: BindingType
    routing_key: str
    source: str


class Exchange(pydantic.BaseModel):
    arguments: dict | None = None
    auto_delete: bool = False
    durable: bool = True
    name: str
    type: str


class Queue(pydantic.BaseModel):
    arguments: dict | None = None
    auto_delete: bool = False
    durable: bool = True
    name: str


class Definitions(pydantic.BaseModel):
    bindings: list[Binding] | None = None
    exchanges: list[Exchange] | None = None
    queues: list[Queue] | None = None

    class Config:
        arbitrary_types_allowed = True


class ComplexConnection(pydantic.BaseModel):
    name: str
    consume: bool | None = True
    confirm: bool | None = False


class Connection(pydantic.BaseModel):
    hostname: str = 'localhost'
    port: int = 5672
    username: str = 'guest'
    password: pydantic.SecretStr = 'guest'
    ssl: bool = False
    virtual_host: str = '/'
    heartbeat_interval: int = 300
    qos_prefetch: int = 1
    definitions: Definitions | None = None

    class Config:
        arbitrary_types_allowed = True


class Consumer(pydantic.BaseModel):
    class_path: str = pydantic.Field(
        validation_alias=pydantic.AliasPath('class'))
    connections: list[str | ComplexConnection]
    queue: str
    qty: int = 1
    ack: bool = True
    max_errors: int = 3
    sentry_dsn: str | None = None
    drop_exchange: str | None = None
    drop_invalid_messages: bool = True
    error_exchange: str | None = None
    message_type: str | None = None
    error_max_retry: int | None = None
    config: dict | None = None

    class Config:
        arbitrary_types_allowed = True


class Statsd(pydantic.BaseModel):
    enabled: bool = False
    host: str = 'localhost'
    port: int = 8125
    prefix: str = 'application.rejected'


class Stats(pydantic.BaseModel):
    log: bool = True
    statsd: Statsd | None = None

    class Config:
        arbitrary_types_allowed = True


class Application(pydantic.BaseModel):
    connections: dict[str, Connection]
    consumers: dict[str, Consumer]
    poll_interval: float | int | None = 60
    sentry_dsn: str | None = None
    stats: Stats | None = None

    class Config:
        arbitrary_types_allowed = True


class Daemon(pydantic.BaseModel):
    user: str | None = None
    group: str | None = None
    pidfile: str | None = None


class Logging(pydantic.BaseModel):
    version: int | None = 1
    filters: dict[str, dict] | None
    formatters: dict[str, dict] | None
    handlers: dict[str, dict] | None
    loggers: dict[str, dict] | None
    root: dict[str, dict] | None = None
    incremental: bool | None = False
    disable_existing_loggers: bool | None = True


class Configuration(pydantic.BaseModel):
    path: str
    application: Application
    daemon: Daemon
    logging: Logging

    class Config:
        arbitrary_types_allowed = True


class Callbacks:
    on_ready: typing.Callable
    on_connection_failure: typing.Callable
    on_closed: typing.Callable
    on_blocked: typing.Callable
    on_unblocked: typing.Callable
    on_confirmation: typing.Callable
    on_delivery: typing.Callable
    on_return: typing.Callable
