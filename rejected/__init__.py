"""
Rejected is a Python RabbitMQ Consumer Framework and Controller Daemon

"""

import logging
from importlib.metadata import PackageNotFoundError, version

# Add NullHandler to prevent logging warnings
logging.getLogger(__name__).addHandler(logging.NullHandler())

from rejected.codecs import AVRO_DATUM_MIME_TYPE  # noqa: E402
from rejected.consumer import (  # noqa: E402
    Consumer,
    ConsumerException,
    FunctionalConsumer,
    MessageException,
    ProcessingException,
    RejectedException,
)
from rejected.models import Message, ProcessingContext, Result  # noqa: E402

__author__ = 'Gavin M. Roy <gavinmroy@gmail.com>'
__since__ = '2009-09-10'
try:
    __version__ = version('rejected')
except PackageNotFoundError:
    __version__ = 'unknown'

__all__ = [
    'AVRO_DATUM_MIME_TYPE',
    'Consumer',
    'ConsumerException',
    'FunctionalConsumer',
    'Message',
    'MessageException',
    'ProcessingContext',
    'ProcessingException',
    'RejectedException',
    'Result',
    '__author__',
    '__since__',
    '__version__',
]
