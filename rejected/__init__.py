"""
Rejected is a Python RabbitMQ Consumer Framework and Controller Daemon

"""
__author__ = 'Gavin M. Roy <gavinmroy@gmail.com>'
__since__ = '2009-09-10'
__version__ = '3.15.1'

import logging
from logging import NullHandler

# Add NullHandler to prevent logging warnings
logging.getLogger(__name__).addHandler(NullHandler())

from rejected.consumer import Consumer
from rejected.consumer import PublishingConsumer
from rejected.consumer import SmartConsumer
from rejected.consumer import SmartPublishingConsumer
from rejected.consumer import ConsumerException
from rejected.consumer import MessageException

__all__ = [
    'Consumer',
    'PublishingConsumer',
    'SmartConsumer',
    'SmartPublishingConsumer',
    'ConsumerException',
    'MessageException'
]
