.. _consumer_api:

Consumer API
============
The :py:class:`Consumer <rejected.consumer.Consumer>` and
:py:class:`SmartConsumer <rejected.consumer.SmartConsumer>` classes to extend
for consumer applications.

While the :py:class:`Consumer <rejected.consumer.Consumer>` class provides all
the structure required for implementing a rejected consumer,
the :py:class:`SmartConsumer <rejected.consumer.SmartConsumer>` adds
functionality designed to make writing consumers even easier. When messages
are received by consumers extending :py:class:`SmartConsumer <rejected.consumer.SmartConsumer>`,
if the message's ``content_type`` property contains one of the supported mime-types,
the message body will automatically be deserialized, making the deserialized
message body available via the ``body`` attribute. Additionally, should one of
the supported ``content_encoding`` types (``gzip`` or ``bzip2``) be specified in the
message's property, it will automatically be decoded.

Message Type Validation
-----------------------
In any of the consumer base classes, if the ``MESSAGE_TYPE`` attribute is set,
the ``type`` property of incoming messages will be validated against when a message is
received, checking for string equality against the ``MESSAGE_TYPE`` attribute.
If they are not matched, the consumer will not process the message and will drop the
message without an exception if the ``DROP_INVALID_MESSAGES`` attribute is set to
``True``. If it is ``False``, a
:py:class:`ConsumerException <rejected.consumer.MessageException>` is raised.

Republishing of Dropped Messages
--------------------------------
If the consumer is configured by specifying ``DROP_EXCHANGE`` as an attribute of
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

Consumer Classes
----------------
.. toctree::
   :glob:
   :maxdepth: 1

   api_consumer
   api_smart_consumer

Exceptions
----------
There are three exception types that consumer applications should raise to handle
problems that may arise when processing a message. When these exceptions are raised,
rejected will reject the message delivery, letting RabbitMQ know that there was
a failure.

The :py:class:`ConsumerException <rejected.consumer.ConsumerException>` should be
raised when there is a problem in the consumer itself, such as inability to contact
a database server or other resources. When a
:py:class:`ConsumerException <rejected.consumer.ConsumerException>` is raised,
the message will be rejected *and* requeued, adding it back to the RabbitMQ it
was delivered back to. Additionally, rejected keeps track of consumer exceptions
and will shutdown the consumer process and start a new one once a consumer has
exceeded its configured maximum error count within a ``60`` second window. The
default maximum error count is ``5``.

The :py:class:`MessageException <rejected.consumer.MessageException>` should be
raised when there is a problem with the message. When this exception is raised,
the message will be rejected on the RabbitMQ server *without* requeue, discarding
the message. This should be done when there is a problem with the message itself,
such as a malformed payload or non-supported properties like ``content-type``
or ``type``.

If a consumer raises a :exc:`~rejected.consumer.ProcessingException`, the
message that was being processed will be republished to the exchange
specified by the ``error`` exchange configuration value or the
``ERROR_EXCHANGE`` attribute of the consumer's class. The message will be
published using the routing key that was last used for the message. The
original message body and properties will be used and two additional
header property values may be added:

- ``X-Processing-Exception`` contains the string value of the exception that was
raised, if specified.
- ``X-Processing-Exceptions`` contains the quantity of processing exceptions
that have been raised for the message.

In combination with a queue that has ``x-message-ttl`` set
and ``x-dead-letter-exchange`` that points to the original exchange for the
queue the consumer is consuming off of, you can implement a delayed retry
cycle for messages that are failing to process due to external resource or
service issues.

If ``ERROR_MAX_RETRY`` is set on the class, the headers for each method
will be inspected and if the value of ``X-Processing-Exceptions`` is
greater than or equal to the ``ERROR_MAX_RETRY`` value, the message will
be dropped.

.. note:: If unhandled exceptions are raised by a consumer, they will be caught by rejected, logged, and turned into a :py:class:`ConsumerException <rejected.consumer.ConsumerException>`.

.. autoclass:: rejected.consumer.RejectedException
   :members:

.. autoclass:: rejected.consumer.ConsumerException
   :members:

.. autoclass:: rejected.consumer.MessageException
   :members:

.. autoclass:: rejected.consumer.ProcessingException
   :members:
