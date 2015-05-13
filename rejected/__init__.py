"""
Rejected is a Python RabbitMQ Consumer Framework and Controller Daemon

"""
import pkg_resources

__author__ = 'Gavin M. Roy <gavinmroy@gmail.com>'
__since__ = "2009-09-10"
__version__ = pkg_resources.get_distribution(__name__).version

from consumer import Consumer
from consumer import PublishingConsumer
from consumer import SmartConsumer
from consumer import SmartPublishingConsumer
from consumer import ConsumerException
from consumer import MessageException

import logging
try:
    from logging import NullHandler
except ImportError:

    class NullHandler(logging.Handler):
        """Python 2.6 does not have a NullHandler"""

        def emit(self, record):
            """Emit a record
            :param record record: The record to emit
            """
            pass


logging.getLogger('rejected').addHandler(NullHandler())
