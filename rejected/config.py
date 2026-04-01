"""Configuration models and loader for rejected."""

import pathlib
import tomllib
import typing

import pydantic
import pydantic.fields
import yaml


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
    frame_max: int = 131072  # spec.FRAME_MAX_SIZE
    socket_timeout: int = 10
    ssl_options: dict[str, typing.Any] = pydantic.Field(default_factory=dict)


class StatsdConfig(pydantic.BaseModel):
    enabled: bool = False
    host: str = 'localhost'
    port: int = 8125
    prefix: str = 'rejected'
    tcp: bool = False
    include_hostname: bool = True


class InfluxDBConfig(pydantic.BaseModel):
    enabled: bool = False
    scheme: str = 'http'
    host: str = 'localhost'
    port: int = 8086
    user: str | None = None
    password: str | None = None
    database: str = 'rejected'


class StatsConfig(pydantic.BaseModel):
    log: bool = False
    statsd: StatsdConfig = pydantic.Field(default_factory=StatsdConfig)
    influxdb: InfluxDBConfig = pydantic.Field(default_factory=InfluxDBConfig)


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
    sentry_dsn: str | None = None
    influxdb_measurement: str | None = None
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


class Settings:
    """Wraps a dict as an attribute- and dict-accessible settings object.
    Passed to Consumer instances as their settings parameter.
    """

    def __init__(self, data: dict | None = None):
        self._data = data or {}

    def get(self, key: str, default=None):
        return self._data.get(key, default)

    def __contains__(self, name: str) -> bool:
        return name in self._data

    def __getattr__(self, name: str):
        try:
            return self._data[name]
        except KeyError:
            raise AttributeError(name) from None

    def __getitem__(self, name: str):
        return self._data[name]

    def __iter__(self):
        return iter(self._data)

    def __repr__(self) -> str:
        return repr(self._data)

    def items(self):
        return self._data.items()

    def keys(self):
        return self._data.keys()

    def values(self):
        return self._data.values()


def load(path: str | pathlib.Path) -> Config:
    """Load and validate configuration from a YAML or TOML file.

    The logging section is extracted from the raw file and stored on
    the returned Config as config.logging.

    :param path: Path to the config file (.yaml, .yml, or .toml)
    :raises FileNotFoundError: If the file does not exist
    :raises ValueError: If the file cannot be parsed or fails validation
    """
    path = pathlib.Path(path)
    if not path.exists():
        raise FileNotFoundError(f'Configuration file not found: {path}')

    try:
        if path.suffix == '.toml':
            if tomllib is None:
                raise ValueError(
                    'tomllib is required for TOML config files '
                    '(available in Python 3.11+)'
                )
            with open(path, 'rb') as f:
                raw = tomllib.load(f)
        else:
            with open(path) as f:
                raw = yaml.safe_load(f) or {}
    except (OSError, yaml.YAMLError) as exc:
        raise ValueError(f'Failed to read configuration: {exc}') from exc

    app_raw = raw.get('Application', raw.get('application', {})) or {}
    logging_raw = raw.get('Logging', raw.get('logging', {})) or {}

    try:
        cfg = Config.model_validate({**app_raw, 'logging': logging_raw})
    except Exception as exc:
        raise ValueError(f'Invalid configuration: {exc}') from exc

    return cfg
