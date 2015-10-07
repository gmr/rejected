"""
Rejected is a Python RabbitMQ Consumer Framework and Controller Daemon

"""
__author__ = 'Gavin M. Roy <gavinmroy@gmail.com>'
__since__ = "2009-09-10"
__version__ = "3.9.0"

import logging
try:
    # not available in python 2.6
    from logging import NullHandler
except ImportError:

    class NullHandler(logging.Handler):

        def emit(self, record):
            pass

# Add NullHandler to prevent logging warnings
logging.getLogger(__name__).addHandler(NullHandler())

from consumer import Consumer
from consumer import PublishingConsumer
from consumer import SmartConsumer
from consumer import SmartPublishingConsumer
from consumer import ConsumerException
from consumer import MessageException
