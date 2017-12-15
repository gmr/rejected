"""Example Rejected Consumer"""
import random
import threading
import time
import uuid

from rejected import consumer, utils
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
        self.logger.info(
            'Processing message %s',
            utils.message_info(
                self.exchange, self.routing_key, self._message.properties))

        with self.stats_track_duration('async_fetch'):
            results = yield [self.http_client.fetch('http://www.google.com'),
                             self.http_client.fetch('http://www.bing.com')]
        self.logger.info('HTTP Status Codes: %r', [r.code for r in results])
        result = yield self.publish_message(
            self.exchange, self.routing_key,
            {'correlation_id': self.message_id,
             'message_id': str(uuid.uuid4()),
             'type': 'example',
             'timestamp': int(time.time())},
            'async_fetch request')
        self.logger.info('Confirmation result: %r', result)
        yield gen.sleep(1)


class BlockingConsumer(consumer.Consumer):

    @gen.coroutine
    def process(self):
        thread = threading.Thread(target=self.blocking_thing)
        thread.start()
        while thread.is_alive():
            yield gen.sleep(1)
            self.logger.debug('Still waiting on thread')
        self.logger.info('Done')

    def blocking_thing(self):
        self.logger.info('Starting the blocking sleep')
        time.sleep(30)
        self.logger.info('Done blocking sleep')
