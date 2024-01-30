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
    arguments: typing.Optional[dict] = None
    destination: str
    destination_type: BindingType
    routing_key: str
    source: str


class Exchange(pydantic.BaseModel):
    arguments: typing.Optional[dict] = None
    auto_delete: bool = False
    durable: bool = True
    name: str
    type: str


class Queue(pydantic.BaseModel):
    arguments: typing.Optional[dict] = None
    auto_delete: bool = False
    durable: bool = True
    name: str


class Definitions(pydantic.BaseModel):
    bindings: typing.Optional[typing.List[Binding]] = None
    exchanges: typing.Optional[typing.List[Exchange]] = None
    queues: typing.Optional[typing.List[Queue]] = None

    class Config:
        arbitrary_types_allowed = True


class ComplexConnection(pydantic.BaseModel):
    name: str
    consume: typing.Optional[bool] = True
    confirm: typing.Optional[bool] = False


class Connection(pydantic.BaseModel):
    hostname: str = 'localhost'
    port: int = 5672
    username: str = 'guest'
    password: pydantic.SecretStr = 'guest'
    ssl: bool = False
    virtual_host: str = '/'
    heartbeat_interval: int = 300
    qos_prefetch: int = 1
    definitions: typing.Optional[Definitions] = None

    class Config:
        arbitrary_types_allowed = True


class Consumer(pydantic.BaseModel):
    class_path: str = pydantic.Field(
        validation_alias=pydantic.AliasPath('class'))
    connections: typing.List[typing.Union[str, ComplexConnection]]
    queue: str
    qty: int = 1
    ack: bool = True
    max_errors: int = 3
    sentry_dsn: typing.Optional[str] = None
    drop_exchange: typing.Optional[str] = None
    drop_invalid_messages: bool = True
    error_exchange: typing.Optional[str] = None
    message_type: typing.Optional[str] = None
    error_max_retry: typing.Optional[int] = None
    config: typing.Optional[dict] = None

    class Config:
        arbitrary_types_allowed = True


class Statsd(pydantic.BaseModel):
    enabled: bool = False
    host: str = 'localhost'
    port: int = 8125
    prefix: str = 'application.rejected'


class Stats(pydantic.BaseModel):
    log: bool = True
    statsd: typing.Optional[Statsd] = None

    class Config:
        arbitrary_types_allowed = True


class Application(pydantic.BaseModel):
    connections: typing.Dict[str, Connection]
    consumers: typing.Dict[str, Consumer]
    poll_interval: typing.Union[float, int, None] = 60
    sentry_dsn: typing.Optional[str] = None
    stats: typing.Optional[Stats] = None

    class Config:
        arbitrary_types_allowed = True


class Daemon(pydantic.BaseModel):
    user: typing.Optional[str] = None
    group: typing.Optional[str] = None
    pidfile: typing.Optional[str] = None


class Logging(pydantic.BaseModel):
    version: typing.Optional[int] = 1
    filters: typing.Optional[typing.Dict[str, dict]]
    formatters: typing.Optional[typing.Dict[str, dict]]
    handlers: typing.Optional[typing.Dict[str, dict]]
    loggers: typing.Optional[typing.Dict[str, dict]]
    root: typing.Optional[typing.Dict[str, dict]] = None
    incremental: typing.Optional[bool] = False
    disable_existing_loggers: typing.Optional[bool] = True


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
