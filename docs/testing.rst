Testing Support
===============

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
consumer, the consumer will raise a :exc:`~rejected.consumer.MessageException`.

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
