"""Example Rejected Consumer"""
from rejected import consumer
import logging
import random

__version__ = '1.0.0'

LOGGER = logging.getLogger(__name__)


class ExampleConsumer(consumer.Consumer):

    def process(self):
        LOGGER.info('Message: %r', self._message.body)
        action = int(random.random() * 100)
        if action == 0:
            raise consumer.ConsumerException('zomg')
        elif action < 2:
            LOGGER.debug('Raising message exception')
            raise consumer.MessageException('reject')
        elif action < 5:
            LOGGER.debug('Raising unhandled exception')
            raise ValueError('Unhandled exception')
