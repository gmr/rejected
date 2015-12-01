"""Example Rejected Consumer"""
from rejected import consumer

import random

from tornado import gen
from tornado import httpclient

__version__ = '1.0.0'


class ExampleConsumer(consumer.SmartConsumer):

    def process(self):
        self.logger.info('Message: %r', self.body)
        action = random.randint(0, 100)
        if action == 0:
            raise consumer.ConsumerException('zomg')
        elif action < 5:
            raise consumer.MessageException('reject')
        elif action < 10:
            raise consumer.ProcessingException('publish')


class AsyncExampleConsumer(consumer.Consumer):

    @gen.coroutine
    def process(self):
        self.logger.info('Message: %r', self.body)
        http_client = httpclient.AsyncHTTPClient()
        results = yield [http_client.fetch('http://www.google.com'),
                         http_client.fetch('http://www.bing.com')]
        self.logger.info('Length: %r', [len(r.body) for r in results])
