.. _consumers_intro:

Writing Consumers
=================
The :class:`~rejected.consumer.Consumer` and :class:`~rejected.consumer.SmartConsumer`
classes provide base classes for your consumer applications. Your application's
:class:`~rejected.consumer.Consumer` class is invoked for each message received
from RabbitMQ.

While the :class:`~rejected.consumer.Consumer` class provides all the structure
required for implementing a rejected consumer, the
:class:`~rejected.consumer.SmartConsumer` adds functionality designed to make
writing consumers even easier. When messages are received by consumers extending
:class:`~rejected.consumer.SmartConsumer`, if the message's
:class:`content_type <rejected.data.Properties>` property contains one of the
supported mime-types, the message body will automatically be deserialized,
making the deserialized message body available via the
:attr:`~rejected.consumer.SmartConsumer.body`  attribute. Additionally, should one of
the supported :class:`content_encoding <rejected.data.Properties>` types
(``gzip`` or ``bzip2``) be specified in the message's property, it will
automatically be decoded.

Message Processing Lifecycle
----------------------------
When a message is received from RabbitMQ, the
:meth:`Consumer.prepare <rejected.consumer.Consumer.prepare>`,
:meth:`Consumer.process <rejected.consumer.Consumer.process>`, and
:meth:`Consumer.on_finish <rejected.consumer.Consumer.on_finish>` methods are
invoked in order. Extend these methods to implement the primary behaviors for
your consumer application.

.. note:: If :meth:`Consumer.finish <rejected.consumer.Consumer.finish>` is
   called in the :meth:`Consumer.prepare <rejected.consumer.Consumer.prepare>`
   method, :meth:`Consumer.process <rejected.consumer.Consumer.process>`
   will **not** be called.

The following example consumer demonstrates the use of the three methods that
are typically invoked for every message that is delivered. While you do not
have to implement the :meth:`Consumer.prepare <rejected.consumer.Consumer.prepare>`
or :meth:`Consumer.on_finish <rejected.consumer.Consumer.on_finish>` methods, you
must implement the :py:meth:`Consumer.process <rejected.consumer.Consumer.process>`
method for your consumer to properly function.

**Example Consumer**

.. code-block:: python

   class Consumer(consumer.Consumer):

       def __init__(self, *args, **kwargs):

          # Make sure you invoke Super when extending ``__init__``
           super(Consumer, self).__init__(*args, **kwargs)

           self.current_id, self.previous_id = None, None

       def prepare(self):
           try:
              self.current_id = self.body['id']
           except KeyError:
               raise consumer.MessageException(
                   'Missing ID in body', metric='missing-id')
           return super(Consumer, self).prepare()

       def process(self):
           self.logger.info('Current ID: %s, Previous ID: %s',
                            self.current_id, self.previous_id)

       def on_finish(self, exc=None):
           self.previous_id = self.current_id
           self.current_id = None


Exceptions
----------
There are three exception types that consumer applications should raise to handle
problems that may arise when processing a message. When these exceptions are raised,
rejected will reject the message delivery, letting RabbitMQ know that there was
a failure.

The :exc:`~rejected.errors.ConsumerException` should be
raised when there is a problem in the consumer itself, such as inability to contact
a database server or other resources. When a :exc:`~rejected.errors.ConsumerException`
is raised, the message will be rejected *and* requeued, leaving the message in its
original place in the RabbitMQ queue. Additionally, rejected keeps track of
consumer exceptions and will shutdown the consumer process and start a new one
once a consumer has exceeded its configured maximum error count within a ``60``
second window. The default maximum error count is ``5``.

The :exc:`~rejected.errors.MessageException` should be raised when there is a
problem with the message. When this exception is raised, the message will be
rejected on the RabbitMQ server *without* requeue, discarding the message.
This should be done when there is a problem with the message itself, such as a
malformed payload or non-supported value in properties like
:class:`content_type <rejected.data.Properties>` or
:class:`type <rejected.data.Properties>` .

If a consumer raises a :exc:`~rejected.consumer.ProcessingException`, the
message that was being processed will be republished to the exchange
specified by the ``error`` exchange configuration value or the
:attr:`~rejected.consumer.Consumer.ERROR_EXCHANGE` attribute of the consumer's
class. The message will be published using the routing key that was last used
for the message. The original message body and properties will be used and two
additional header property values may be added:

- ``X-Processing-Exception`` contains the string value of the exception that was
   raised, if specified.
- ``X-Processing-Exceptions`` contains the quantity of processing exceptions
   that have been raised for the message.

In combination with a queue that has ``x-message-ttl`` set
and ``x-dead-letter-exchange`` that points to the original exchange for the
queue the consumer is consuming off of, you can implement a delayed retry
cycle for messages that are failing to process due to external resource or
service issues.

If :attr:`~rejected.consumer.Consumer.ERROR_MAX_RETRIES` is set on the class,
the headers for each method will be inspected and if the value of
``X-Processing-Exceptions`` is greater than or equal to the
:attr:`~rejected.consumer.Consumer.ERROR_MAX_RETRIES` value, the message will
be dropped.

.. note:: If unhandled exceptions are raised by a consumer, they will be caught
      by rejected, logged, and treated like a
      :exc:`~rejected.consumer.ConsumerException`.


Republishing of Dropped Messages
--------------------------------
If the consumer is configured by specifying
:attr:`~rejected.consumer.Consumer.DROP_EXCHANGE` as an attribute of
the consumer class or in the consumer configuration with the ``drop_exchange``
configuration variable, when a message is dropped, it is published to that
exchange prior to the message being rejected in RabbitMQ. When the
message is republished, four new values are added to the AMQP ``headers``
message property: ``X-Dropped-By``, ``X-Dropped-Reason``, ``X-Dropped-Timestamp``,
``X-Original-Exchange``.

The ``X-Dropped-By`` header value contains the configured name of the
consumer that dropped the message. ``X-Dropped-Reason`` contains the
reason the message was dropped (eg invalid message type or maximum error
count). ``X-Dropped-Timestamp`` value contains the ISO-8601 formatted
timestamp of when the message was dropped. Finally, the
``X-Original-Exchange`` value contains the original exchange that the
message was published to.

Message Type Validation
-----------------------
If the :attr:`~rejected.consumer.Consumer.MESSAGE_TYPE` attribute is set,
the :class:`type <rejected.data.Properties>` value of incoming messages
will be validated against when a message is received, checking the
:attr:`~rejected.consumer.Consumer.MESSAGE_TYPE` attribute. If the attribute
is a string, a string equality check is performed. If the attribute is a
:class:`list`, :class:`set`, or :class:`tuple`, the message
:class:`type <rejected.data.Properties>` value will checked for membership in
:attr:`~rejected.consumer.Consumer.MESSAGE_TYPE`. If there is no match the
message will not be processed. In addition, if the
:attr:`~rejected.consumer.Consumer.DROP_INVALID_MESSAGES` is set to
:class:`True`, the consumer will drop the message without republishing it.
Otherwise a :exc:`rejected.errors.MessageException` is raised.

Additional Information
----------------------

 - :doc:`api_consumer`
 - :doc:`api_smart_consumer`
 - :doc:`api_mixins`
 - :doc:`api_exceptions`

Examples
--------
The following example illustrates a very simple consumer that simply logs each
message body as it's received.

.. code:: python

    import logging

    from rejected import consumer

    __version__ = '1.0.0'


    class ExampleConsumer(consumer.Consumer):

        def process(self):
            self.logger.info(self.body)

All interaction with RabbitMQ with regard to connection management and message
handling, including acknowledgements and rejections are automatically handled
for you.

The ``__version__`` variable provides context in the rejected log files when
consumers are started and can be useful for investigating consumer behaviors in
production.

In this next example, a contrived ``ExampleConsumer._connect_to_database`` method
is added that will return ``False``. When ``ExampleConsumer.process`` evaluates
if it could connect to the database and finds it can not, it will raise a
:exc:ted.consumer.ConsumerException` which will requeue the message
in RabbitMQ and increment an error counter. When too many errors occur, rejected
will automatically restart the consumer after a brief quiet period.

.. code:: python

    import logging

    from rejected import consumer

    __version__ = '1.0.0'


    class ExampleConsumer(consumer.Consumer):

        def _connect_to_database(self):
            return False

        def prepare(self):
            if not self._connect_to_database:
                raise consumer.ConsumerException('Database error')

        def process(self):
            self.logger.info(self.body)

Some consumers are also publishers. In this next example, the message body will
be republished to a new exchange on the same RabbitMQ connection:

.. code:: python

    import logging

    from rejected import consumer

    __version__ = '1.0.0'

    class ExampleConsumer(consumer.Consumer):

        def process(self):
            LOGGER.info(self.body)
            self.publish('new-exchange', 'routing-key', {}, self.body)

Be sure to check out :doc:`consumer class documentation <api_consumers>` for more
information.
