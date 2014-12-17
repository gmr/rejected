"""Example Rejected Consumer"""
from rejected import consumer
import logging
import random

__version__ = '1.0.0'

LOGGER = logging.getLogger(__name__)

class Consumer(consumer.Consumer):

    def process(self):
        #LOGGER.info('Received message %s, a %s message: %r',
        #            self.message_id, self.message_type, self.body)
        chance = random.randint(0, 100)
        if chance < 1:
            raise consumer.MessageException('zomg')
        #if 0 < chance < 3:
        #    raise consumer.MessageException('reject')
        #elif 2 < chance < 4:
        #    raise ValueError('Unhandled exception')

