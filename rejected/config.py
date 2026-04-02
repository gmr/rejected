"""Configuration loader for rejected.

Pydantic models live in :mod:`rejected.models`. This module provides the
:class:`Settings` wrapper and the :func:`load` function.

"""

import pathlib
import tomllib
import typing

import yaml

from . import models

# Re-export model classes for backward compatibility
Config = models.Config
ConnectionConfig = models.ConnectionConfig
ConnectionRef = models.ConnectionRef
ConsumerConfig = models.ConsumerConfig
PrometheusConfig = models.PrometheusConfig
StatsdConfig = models.StatsdConfig
StatsConfig = models.StatsConfig


class Settings:
    """Wraps a dict as an attribute- and dict-accessible settings object.

    Passed to Consumer instances as their settings parameter.
    """

    def __init__(self, data: dict[str, typing.Any] | None = None) -> None:
        self._data: dict[str, typing.Any] = data or {}

    def get(self, key: str, default: typing.Any = None) -> typing.Any:
        return self._data.get(key, default)

    def __contains__(self, name: str) -> bool:
        return name in self._data

    def __getattr__(self, name: str) -> typing.Any:
        try:
            return self._data[name]
        except KeyError:
            raise AttributeError(name) from None

    def __getitem__(self, name: str) -> typing.Any:
        return self._data[name]

    def __setitem__(self, name: str, value: typing.Any) -> None:
        self._data[name] = value

    def __iter__(self) -> typing.Iterator[str]:
        return iter(self._data)

    def __repr__(self) -> str:
        return repr(self._data)

    def items(self) -> typing.ItemsView[str, typing.Any]:
        return self._data.items()

    def keys(self) -> typing.KeysView[str]:
        return self._data.keys()

    def values(self) -> typing.ValuesView[typing.Any]:
        return self._data.values()


def load(file_path: str | pathlib.Path) -> models.Config:
    """Load and validate configuration from a YAML or TOML file.

    :raises FileNotFoundError: If the file does not exist
    :raises ValueError: If the file cannot be parsed or fails validation

    """
    path = pathlib.Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f'Configuration file not found: {path}')

    try:
        if path.suffix == '.toml':
            with open(path, 'rb') as f:
                raw = tomllib.load(f)
        elif path.suffix in ('.yaml', '.yml'):
            with open(path) as f:
                raw = yaml.safe_load(f) or {}
        else:
            raise ValueError(f'Unsupported config file type: {path.suffix}')
    except (OSError, yaml.YAMLError) as exc:
        raise ValueError(f'Failed to read configuration: {exc}') from exc
    except tomllib.TOMLDecodeError as exc:
        raise ValueError(f'Failed to read configuration: {exc}') from exc

    if not isinstance(raw, dict):
        raise ValueError('Configuration root must be a mapping')

    app_raw = raw.get('Application', raw.get('application', {})) or {}
    logging_raw = raw.get('Logging', raw.get('logging', {})) or {}

    try:
        cfg = models.Config.model_validate({**app_raw, 'logging': logging_raw})
    except Exception as exc:
        raise ValueError(f'Invalid configuration: {exc}') from exc

    return cfg
