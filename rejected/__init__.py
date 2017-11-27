"""
Rejected is a Python RabbitMQ Consumer Framework and Controller Daemon

"""
from rejected.consumer import Consumer
from rejected.consumer import SmartConsumer
from rejected.consumer import ConsumerException
from rejected.consumer import MessageException
from rejected.consumer import ProcessingException
from rejected.errors import RabbitMQException

__author__ = 'Gavin M. Roy <gavinmroy@gmail.com>'
__since__ = '2009-09-10'
__version__ = '4.0.0'

__all__ = [
    'Consumer',
    'PublishingConsumer',
    'SmartConsumer',
    'SmartPublishingConsumer',
    'ConsumerException',
    'MessageException',
    'ProcessingException',
    'RabbitMQException'
]
