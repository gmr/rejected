import contextlib
import json
import pathlib
import sys

import pydantic
import yaml

from rejected import models


def load_configuration(configuration_file: str) -> models.Configuration:
    """Load in the config and validate it by using pydantic models"""
    try:
        with read_configuration(configuration_file) as config_data:
            try:
                return models.Configuration(path=configuration_file,
                                            **config_data)
            except pydantic.ValidationError as err:
                sys.stderr.write('ERROR: Configuration did not validate:\n\n')
                errors = str(err).split('\n')
                for error in errors[1:]:
                    sys.stderr.write(f'  {error}\n')
                sys.exit(3)
    except RuntimeError as err:
        sys.stderr.write(f'{str(err)}\n')
        sys.exit(2)


@contextlib.contextmanager
def read_configuration(path: str) -> dict:
    """Load in the file returning the raw data structure"""
    config_file = pathlib.Path(path)
    if not config_file.exists():
        raise RuntimeError(f'ERROR: File "{path}" not found')
    with config_file.open('r') as handle:
        if path.endswith('.json'):
            yield json.load(handle)
        elif path.endswith('.yaml') or path.endswith('.yml'):
            yield yaml.safe_load(handle)
        else:
            raise RuntimeError(f'ERROR: Invalid file extension for {path}')
