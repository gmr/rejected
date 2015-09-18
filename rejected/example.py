"""Example Rejected Consumer"""
from rejected import consumer

import avroconsumer

import logging
import random

from tornado import gen
from tornado import httpclient

__version__ = '1.0.0'

LOGGER = logging.getLogger(__name__)


class ExampleConsumer(consumer.Consumer):

    def process(self):
        LOGGER.debug('Message: %r', self.body)
        action = random.randint(0, 10000)
        if action == 0:
            raise consumer.ConsumerException('zomg')
        elif action < 5:
            raise consumer.MessageException('reject')


class AsyncExampleConsumer(avroconsumer.HTTPLoaderMixin,
                           avroconsumer.DatumConsumer):

    def process(self):
        LOGGER.info('Message: %r', self.body)
