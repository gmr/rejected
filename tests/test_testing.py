"""Tests for rejected.testing"""

from rejected import consumer, exceptions, testing


class TestPublishedMessages(testing.AsyncTestCase):
    def get_consumer(self):
        class TestConsumer(consumer.Consumer):
            async def process(self):
                for i in range(10):
                    await self.publish_message(
                        exchange='my_exchange',
                        routing_key='my_routing_key',
                        body=i,
                        properties={
                            'type': 'my_type',
                            'content_type': 'my_content_type',
                        },
                    )

        return TestConsumer

    async def test_order_preserved(self):
        await self.process_message()
        self.assertEqual(10, len(self.published_messages))
        for i, published_message in zip(
            range(10), self.published_messages, strict=False
        ):
            self.assertEqual(i, published_message.body)
            self.assertEqual('my_exchange', published_message.exchange)
            self.assertEqual('my_routing_key', published_message.routing_key)
            self.assertEqual('my_type', published_message.properties.type)
            self.assertEqual(
                'my_content_type', published_message.properties.content_type
            )


class TestProcessingException(testing.AsyncTestCase):
    def get_consumer(self):
        class TestConsumer(consumer.Consumer):
            async def process(self):
                raise exceptions.ProcessingException

        return TestConsumer

    async def test_republished(self):
        with self.assertRaises(exceptions.ProcessingException):
            await self.process_message()
        self.assertEqual(1, len(self.published_messages))


class TestMessageException(testing.AsyncTestCase):
    def get_consumer(self):
        class TestConsumer(consumer.Consumer):
            MESSAGE_TYPE = 'a_type'

        return TestConsumer

    async def test_no_drop(self):
        with self.assertRaises(exceptions.MessageException):
            await self.process_message()
        self.assertEqual(0, len(self.published_messages))

    async def test_drop(self):
        self.consumer._drop_exchange = 'drop'
        self.consumer._drop_invalid = True
        await self.process_message(message_type='bad_type')
        self.assertEqual(1, len(self.published_messages))
        published_message = self.published_messages[0]
        self.assertEqual('drop', published_message.exchange)


class TestUnhandledException(testing.AsyncTestCase):
    def get_consumer(self):
        class TestConsumer(consumer.Consumer):
            async def process(self):
                raise ValueError('This is a test exception')

        return TestConsumer

    async def test_stacktrace(self):
        with self.assertRaises(ValueError):
            await self.process_message({'foo': 'bar'})
