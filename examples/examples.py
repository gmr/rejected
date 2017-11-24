"""Example Rejected Consumer"""
import random
import time

from rejected import consumer
from tornado import gen, httpclient

__version__ = '1.0.0'


class ExampleConsumer(consumer.SmartConsumer):

    def process(self):
        self.logger.info('Message: %r', self.body)
        action = random.randint(0, 100)
        self.stats_incr('action', action)
        if action == 0:
            raise consumer.ConsumerException('zomg')
        elif action < 5:
            raise consumer.MessageException('reject')
        elif action < 10:
            raise consumer.ProcessingException('publish')


class AsyncExampleConsumer(consumer.Consumer):
    """This consumer should spin indefinitely on making Async HTTP requests"""

    def __init__(self, *args, **kwargs):
        super(AsyncExampleConsumer, self).__init__(*args, **kwargs)
        self.http_client = httpclient.AsyncHTTPClient()

    @gen.coroutine
    def process(self):
        with self.stats_track_duration('async_fetch'):
            results = yield [self.http_client.fetch('http://www.google.com'),
                             self.http_client.fetch('http://www.bing.com')]
        self.logger.info('Length: %r', [len(r.body) for r in results])
        yield self.publish_message(self.exchange, self.routing_key,
                                   {'type': 'example',
                                    'timestamp': int(time.time())},
                                   'async_fetch request')
        self.logger.info('Pre sleep')
        yield gen.sleep(1)
        self.logger.info('Post sleep')
