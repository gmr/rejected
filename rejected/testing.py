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
from tornado import gen, ioloop, testing

from rejected import consumer, data

gen_test = testing.gen_test
"""Testing equivalent of :func:`tornado.gen.coroutine`, to be applied to test
methods.

"""


class AsyncTestCase(testing.AsyncTestCase):
    """:class:`~unittest.TestCase` subclass for testing
    :class:`~tornado.ioloop.IOLoop`-based asynchronous code.

    """
    _consumer = None

    def setUp(self):
        super(AsyncTestCase, self).setUp()
        self.correlation_id = str(uuid.uuid4())
        self.process = self._create_process()
        self.consumer = self._create_consumer()
        self.channel = self.process.connections['mock'].channel

    def tearDown(self):
        super(AsyncTestCase, self).tearDown()
        if not self.consumer._finished:
            self.consumer.finish()

    def get_consumer(self):
        """Override to return the consumer class for testing.

        :rtype: :class:`rejected.consumer.Consumer`

        """
        return consumer.Consumer

    def get_settings(self):
        """Override this method to provide settings to the consumer during
        construction.

        :rtype: dict

        """
        return {}

    def create_message(self, message, properties=None,
                       exchange='rejected', routing_key='test'):
        """Create a message instance for use with the consumer in testing.

        :param any message: the body of the message to create
        :param dict properties: AMQP message properties
        :param str exchange: The exchange the message should appear to be from
        :param str routing_key: The message's routing key
        :rtype: :class:`rejected.data.Message`

        """
        if not properties:
            properties = {}
        if isinstance(message, dict) and \
                properties.get('content_type') == 'application/json':
            message = json.dumps(message)
        return data.Message(
            connection='mock',
            channel=self.process.connections['mock'].channel,
            method=spec.Basic.Deliver(
                'ctag0', 1, False, exchange, routing_key),
            properties=spec.BasicProperties(
                app_id=properties.get('app_id', 'rejected.testing'),
                content_encoding=properties.get('content_encoding'),
                content_type=properties.get('content_type'),
                correlation_id=properties.get(
                    'correlation_id', self.correlation_id),
                delivery_mode=properties.get('delivery_mode', 1),
                expiration=properties.get('expiration'),
                headers=properties.get('headers'),
                message_id=properties.get('message_id', str(uuid.uuid4())),
                priority=properties.get('priority'),
                reply_to=properties.get('reply_to'),
                timestamp=properties.get('timestamp', int(time.time())),
                type=properties.get('type'),
                user_id=properties.get('user_id')
            ), body=message, returned=False)

    @gen.coroutine
    def process_message(self,
                        message_body=None,
                        content_type='application/json',
                        message_type=None,
                        properties=None,
                        exchange='rejected',
                        routing_key='routing-key'):
        """Process a message as if it were being delivered by RabbitMQ. When
        invoked, an AMQP message will be locally created and passed into the
        consumer. With using the default values for the method, if you pass in
        a JSON serializable object, the message body will automatically be
        JSON serialized.

        If an exception is not raised, a :class:`~rejected.data.Measurement`
        instance is returned that will contain all of the measurements
        collected during the processing of the message.

        :param any message_body: the body of the message to create
        :param str content_type: The mime type
        :param str message_type: identifies the type of message to create
        :param dict properties: AMQP message properties
        :param str exchange: The exchange the message should appear to be from
        :param str routing_key: The message's routing key
        :raises: :exc:`rejected.consumer.ConsumerException`
        :raises: :exc:`rejected.consumer.MessageException`
        :raises: :exc:`rejected.consumer.ProcessingException`
        :rtype: :class:`rejected.data.Measurement`

        """
        properties = properties or {}
        properties.setdefault('content_type', content_type)
        properties.setdefault('correlation_id', self.correlation_id)
        properties.setdefault('timestamp', int(time.time()))
        properties.setdefault('type', message_type)

        measurement = data.Measurement()

        result = yield self.consumer.execute(
            self.create_message(message_body, properties,
                                exchange, routing_key),
            measurement)
        if result == data.MESSAGE_ACK:
            raise gen.Return(measurement)
        elif result == data.CONSUMER_EXCEPTION:
            raise consumer.ConsumerException()
        elif result == data.MESSAGE_EXCEPTION:
            raise consumer.MessageException()
        elif result == data.PROCESSING_EXCEPTION:
            raise consumer.ProcessingException()
        raise gen.Return(measurement)

    @staticmethod
    def _create_channel():
        obj = mock.Mock('pika.channel.Channel')
        obj.basic_ack = mock.Mock()
        obj.basic_nack = mock.Mock()
        obj.basic_publish = mock.Mock()
        obj.basic_reject = mock.Mock()
        return obj

    def _create_connection(self):
        obj = mock.Mock('pika.adapters.tornado_connection.TornadoConnection')
        obj.ioloop = ioloop.IOLoop.current()
        obj.channel = self._create_channel()
        obj.channel.connection = obj
        return obj

    def _create_consumer(self):
        """Creates the per-test instance of the consumer that is going to be
        tested.

        :rtype: rejected.consumer.Consumer

        """
        cls = self.get_consumer()
        obj = cls(config.Data(self.get_settings()), self.process)
        obj.initialize()
        obj._message = self.create_message('dummy')
        obj.set_channel('mock', self.process.connections['mock'].channel)
        return obj

    def _create_process(self):
        obj = mock.Mock('rejected.process.Process')
        obj.connections = {'mock': self._create_connection()}
        obj.sentry_client = mock.Mock()
        obj.sentry_client.tags = mock.Mock()
        obj.send_exception_to_sentry = mock.Mock()
        return obj
