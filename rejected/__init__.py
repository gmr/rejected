"""
Rejected is a Python RabbitMQ Consumer Framework and Controller Daemon

"""
__author__ = 'Gavin M. Roy <gavinmroy@gmail.com>'
__since__ = '2009-09-10'
__version__ = '3.13'

import sys
import logging
try:
    # not available in python 2.6
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

PYTHON26 = sys.version_info < (2,7)

# Add NullHandler to prevent logging warnings
logging.getLogger(__name__).addHandler(NullHandler())

from rejected.consumer import Consumer
from rejected.consumer import PublishingConsumer
from rejected.consumer import SmartConsumer
from rejected.consumer import SmartPublishingConsumer
from rejected.consumer import ConsumerException
from rejected.consumer import MessageException
