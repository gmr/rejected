"""
The :class:`rejected.testing.AsyncTestCase` provides a based class for the
easy creation of tests for your consumers. The test cases exposes multiple
methods to make it easy to setup a consumer and process messages. It is
build on top of :class:`tornado.testing.AsyncTestCase` which extends
:class:`unittest.TestCase`.

To get started, override the
:meth:`rejected.testing.AsyncTestCase.get_consumer` method.

Next, the :meth:`rejected.testing.AsyncTestCase.get_settings` method can be
overridden to define the settings that are passed into the consumer.

Finally, to invoke your Consumer as if it were receiving a message, the
:meth:`~rejected.testing.AsyncTestCase.process_message` method should be
invoked.

.. note:: Tests are asynchronous, so each test should be decorated with
            :meth:`~rejected.testing.gen_test`.

Example
-------
The following example expects that when the message is processed by the
consumer, the consumer will raise a  :exc:`~rejected.consumer.MessageException`.

.. code:: python

    from rejected import consumer, testing

    import my_package


    class ConsumerTestCase(testing.AsyncTestCase):

        def get_consumer(self):
            return my_package.Consumer

        def get_settings(self):
            return {'remote_url': 'http://foo'}

        @testing.gen_test
        def test_consumer_raises_message_exception(self):
            with self.assertRaises(consumer.MessageException):
                yield self.process_message({'foo': 'bar'})

"""
import json
import time
import uuid

from helper import config
import mock
from pika import spec
from tornado import gen, testing

from rejected import consumer, data

gen_test = testing.gen_test
"""Testing equivalent of ``@gen.coroutine``, to be applied to test methods."""


class AsyncTestCase(testing.AsyncTestCase):
    """:class:`~unittest.TestCase` subclass for testing
    :class:`~tornado.ioloop.IOLoop`-based asynchronous code.

    """
    _consumer = None

    def setUp(self):
        super(AsyncTestCase, self).setUp()
        self.correlation_id = str(uuid.uuid4())
        self.consumer = self._create_consumer()

    def tearDown(self):
        super(AsyncTestCase, self).tearDown()
        if not self.consumer._finished:
            self.consumer.finish()

    def get_consumer(self):
        """Override to return the consumer class for testing.

        :rtype: class

        """
        return consumer.Consumer

    def get_settings(self):
        """Override this method to provide settings to the consumer during
        construction.

        :return:

        """
        return {}

    @gen.coroutine
    def process_message(self, message_body,
                        content_type='application/json', message_type=None,
                        exchange='rejected', routing_key='routing-key'):
        """Process a message as if it were being delivered by RabbitMQ. When
        invoked, an AMQP message will be locally created and passed into the
        consumer. With using the default values for the method, if you pass in
        a JSON serializable object, the message body will automatically be
        JSON serialized.

        :param any message_body: the body of the message to create
        :param str content_type: The mime type
        :param str message_type: identifies the type of message to create
        :param str exchange: The exchange the message should appear to be from
        :param str routing_key: The message's routing key
        :raises: rejected.consumer.ConsumerException
        :raises: rejected.consumer.MessageException
        :raises: rejected.consumer.ProcessingException
        :returns: bool

        """
        result = yield self.consumer._execute(
            self._create_message(message_body, content_type, message_type,
                                 exchange, routing_key), data.Measurement())
        if result == data.MESSAGE_ACK:
            raise gen.Return(True)
        elif result == data.CONSUMER_EXCEPTION:
            raise consumer.ConsumerException()
        elif result == data.MESSAGE_EXCEPTION:
            raise consumer.MessageException()
        elif result == data.PROCESSING_EXCEPTION:
            raise consumer.ProcessingException()

    def _create_consumer(self):
        """Creates the per-test instance of the consumer that is going to be
        tested.

        :rtype: rejected.consumer.Consumer

        """
        cls = self.get_consumer()
        settings = config.Data(self.get_settings())
        obj = cls(settings, mock.Mock('rejected.process.Process'))
        obj._process = mock.Mock()
        obj._process.send_exception_to_sentry = mock.Mock()
        obj.initialize()
        obj._channel = mock.Mock('pika.channel.Channel')
        obj._channel.basic_publish = mock.Mock()
        obj._correlation_id = self.correlation_id
        return obj

    def _create_message(self, message,
                        content_type='application/json',
                        message_type=None,
                        exchange='rejected',
                        routing_key='test'):
        """Create a message instance for the consumer.

        :param any message: the body of the message to create
        :param str content_type: The mime type
        :param str message_type: identifies the type of message to create
        :param str exchange: The exchange the message should appear to be from
        :param str routing_key: The message's routing key

        """
        if isinstance(message, dict) and content_type == 'application/json':
            message = json.dumps(message)
        return data.Message(
            channel=self.consumer._channel,
            method=spec.Basic.Deliver(
                'ctag0', 1, False, exchange, routing_key),
            properties=spec.BasicProperties(
                app_id='rejected.testing',
                content_type=content_type,
                correlation_id=self.correlation_id,
                delivery_mode=1,
                message_id=str(uuid.uuid4()),
                timestamp=int(time.time()),
                type=message_type
            ), body=message)
