Testing API
===========
Rejected's testing API extends Tornado's :py:mod:`~tornado.testing` module API
to make writing consumer application tests easy to write without having to
create your own mocks for Rejected's consumer application contracts.

Decorator
---------
All of your test methods when using the functions in :py:class:`~rejected.testing.AsyncTestCase`
need to use the ``@testing.gen_test`` decorator.

.. py:decorator:: rejected.testing.gen_test

    :py:mod:`rejected.testing` namespaced import of Tornado's
    :py:func:`@tornado.testing.gen_test <tornado.testing.gen_test>` decorator.

    Example:

    .. code-block:: python

       from rejected import testing

       class ConsumerTestCase(testing.AsyncTestCase):

          @testing.gen_test
          def test_consumer_raises_message_exception(self):
              result = yield self.process_message({'foo': 'bar'})

Base Test Case
--------------
Extend :py:class:`~rejected.testing.AsyncTestCase` to implement your consumer
tests.

.. autoclass:: rejected.testing.AsyncTestCase

   .. rubric:: Consumer Testing Functions

   .. automethod:: rejected.testing.AsyncTestCase.get_consumer
   .. automethod:: rejected.testing.AsyncTestCase.get_settings
   .. automethod:: rejected.testing.AsyncTestCase.create_message
   .. automethod:: rejected.testing.AsyncTestCase.process_message
   .. autoattribute:: rejected.testing.AsyncTestCase.published_messages
   .. automethod:: rejected.testing.AsyncTestCase.publishing_side_effect

   .. rubric:: Tornado AsyncTestCase Specific Functions

   .. automethod:: rejected.testing.AsyncTestCase.get_new_ioloop
   .. automethod:: rejected.testing.AsyncTestCase.stop
   .. automethod:: rejected.testing.AsyncTestCase.wait

   .. rubric:: Test Setup and Tear down

   .. automethod:: rejected.testing.AsyncTestCase.setUp
   .. automethod:: rejected.testing.AsyncTestCase.tearDown

   .. note:: For additional methods available in this class, see the
             :class:`unittest.TestCase` documentation.

Test Results
------------
To test that the messages your consumer may be publishing are correct, any
calls to :class:`Consumer.publish_message <rejected.consumer.Consumer.publish_message>`
are recorded to :attr:`AsyncTestCase.published_messages <rejected.testing.AsyncTestCase.published_messages>`
as a list of :class:`~rejected.testing.PublishedMessage` objects.

.. autoclass:: rejected.testing.PublishedMessage
   :members:

Exceptions
----------
The following exceptions are available for use in testing via the
:meth:`~rejected.testing.AsyncTestCase.publishing_side_effect` method:

.. autoclass:: rejected.testing.UndeliveredMessage
.. autoclass:: rejected.testing.UnroutableMessage

tornado.gen
-----------
Tornado's :py:mod:`tornado.gen` module is imported into the
:py:mod:`rejected.testing` module as :py:mod:`rejected.testing.gen` for your convenience.

Methods of note are:

.. automethod:: tornado.gen.sleep

.. note:: For additional methods available in this namespace, see the
          :py:mod:`tornado.gen` documentation.
