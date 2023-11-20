"""
Rejected is a Python RabbitMQ Consumer Framework and Controller Daemon

"""
import logging

# Add NullHandler to prevent logging warnings
logging.getLogger(__name__).addHandler(logging.NullHandler())

from rejected.consumer import (  # noqa: E402
    Consumer,
    ConsumerException,
    MessageException,
    ProcessingException,
    PublishingConsumer,
    SmartConsumer,
    SmartPublishingConsumer)

__author__ = 'Gavin M. Roy <gavinmroy@gmail.com>'
__since__ = '2009-09-10'
__version__ = '3.22.2'

__all__ = [
    '__author__',
    '__since__',
    '__version__',
    'Consumer',
    'ConsumerException',
    'MessageException',
    'ProcessingException',
    'PublishingConsumer',
    'SmartConsumer',
    'SmartPublishingConsumer'
]
