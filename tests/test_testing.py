# coding=utf-8
"""Tests for rejected.testing"""
from rejected import consumer, testing


class TestPublishedMessages(testing.AsyncTestCase):

    def get_consumer(self):
        class Consumer(consumer.SmartConsumer):
            def process(self):
                for i in range(10):
                    self.publish_message(
                        exchange='my_exchange',
                        routing_key='my_routing_key',
                        body=i,
                        properties={
                            'type': 'my_type',
                            'content_type': 'my_content_type'
                        })
        return Consumer

    def test_order_preserved(self):
        self.process_message()
        self.assertEqual(10, len(self.published_messages))
        for i, published_message in zip(range(10), self.published_messages):
            self.assertEqual(i, published_message.body)
            self.assertEqual('my_exchange', published_message.exchange)
            self.assertEqual('my_routing_key', published_message.routing_key)
            self.assertEqual('my_type',
                             published_message.properties.type)
            self.assertEqual('my_content_type',
                             published_message.properties.content_type)


class TestProcessingException(testing.AsyncTestCase):

    def get_consumer(self):
        class Consumer(consumer.SmartConsumer):
            def process(self):
                raise consumer.ProcessingException
        return Consumer

    def test_republished(self):
        self.process_message()
        self.assertEqual(1, len(self.published_messages))
        published_message = self.published_messages[0]

        self.assertEqual(
            self.consumer._message.routing_key,
            published_message.routing_key)
        self.assertEqual(
            self.consumer._error_exchange,
            published_message.exchange)
        self.assertEqual(
            self.consumer._message.body,
            published_message.body)
        for (attr, value) in self.consumer._message.properties:
            if attr == 'headers':
                self.assertEqual(
                    {'X-Processing-Exception': 'ProcessingException',
                     'X-Processing-Exceptions': 1},
                    published_message.properties.headers)
            else:
                self.assertEqual(
                    value, getattr(published_message.properties, attr))


class TestMessageException(testing.AsyncTestCase):

    def get_consumer(self):
        class Consumer(consumer.SmartConsumer):
            MESSAGE_TYPE = 'a_type'
        return Consumer

    def test_no_drop(self):
        self.process_message()
        self.assertEqual(0, len(self.published_messages))

    def test_drop(self):
        self.consumer._drop_exchange = 'drop'
        self.consumer._drop_invalid = True
        self.process_message(message_type='bad_type')
        self.assertEqual(1, len(self.published_messages))
        published_message = self.published_messages[0]

        self.assertEqual(
            self.consumer._message.routing_key,
            published_message.routing_key)
        self.assertEqual(
            self.consumer._drop_exchange,
            published_message.exchange)
        self.assertEqual(
            self.consumer._message.body,
            published_message.body)
        for (attr, value) in self.consumer._message.properties:
            if attr == 'headers':
                headers = published_message.properties.headers
                self.assertTrue(headers.pop('X-Dropped-Timestamp'))
                self.assertEqual(
                    {'X-Dropped-By': 'Consumer',
                     'X-Dropped-Reason': 'invalid type',
                     'X-Original-Exchange': 'rejected'},
                    headers)
            else:
                self.assertEqual(
                    value, getattr(published_message.properties, attr))
