"""Example Rejected Consumer"""
from rejected import consumer
import logging
import random

__version__ = '1.0.0'

LOGGER = logging.getLogger(__name__)


class ExampleConsumer(consumer.Consumer):

    def process(self):
        LOGGER.debug('Message: %r', self._message.body)
        action = random.randint(0, 1000)
        if action == 0:
            raise ValueError('Unhandled exception')
        elif action < 2:
            raise consumer.ConsumerException('zomg')
        elif action < 5:
            raise consumer.MessageException('reject')

