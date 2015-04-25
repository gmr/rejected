"""Example Rejected Consumer"""
from rejected import consumer
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


class AsyncExampleConsumer(consumer.Consumer):

    @gen.coroutine
    def process(self):
        LOGGER.debug('Message: %r', self.body)
        http_client = httpclient.AsyncHTTPClient()
        results = yield [http_client.fetch('http://www.github.com'),
                         http_client.fetch('http://www.reddit.com')]
        LOGGER.info('Length: %r', [len(r.body) for r in results])
