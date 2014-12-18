Consumer API
============
The :py:class:`Consumer <rejected.consumer.Consumer>`, :py:class:`PublishingConsumer <rejected.consumer.PublishingConsumer>`,
:py:class:`SmartConsumer <rejected.consumer.SmartConsumer>`, and :py:class:`SmartPublishingConsumer <rejected.consumer.SmartPublishingConsumer>` provide base
classes to extend for consumer applications.

While the :py:class:`Consumer <rejected.consumer.Consumer>` class provides all the structure required for
implementing a rejected consumer, the :py:class:`SmartConsumer <rejected.consumer.SmartConsumer>` adds
functionality designed to make writing consumers even easier. When messages
are received by consumers extending :py:class:`SmartConsumer <rejected.consumer.SmartConsumer>`, if the message's
``content_type`` property contains one of the supported mime-types, the message
body will automatically be deserialized, making the deserialized message body
available via the ``body`` attribute. Additionally, should one of the supported
``content_encoding`` types (``gzip`` or ``bzip2``) be specified in the
message's property, it will automatically be decoded.

Message Type Validation
-----------------------
In any of the consumer base classes, if the ``MESSAGE_TYPE`` attribute is set,
the ``type`` property of incoming messages will be validated against when a message is
received, checking for string equality against the ``MESSAGE_TYPE`` attribute.
If they are not matched, the consumer will not process the message and will drop the
message without an exception if the ``DROP_INVALID_MESSAGES`` attribute is set to
``True``. If it is ``False``, a :py:class:`ConsumerException <rejected.consumer.ConsumerException>`
is raised.

Consumer Classes
----------------
.. toctree::
   :glob:
   :maxdepth: 1

   api_consumer
   api_publishing_consumer
   api_smart_consumer
   api_smart_publishing_consumer

Exceptions
----------
There are two exception types that consumer applications should raise to handle
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

.. note:: If unhandled exceptions are raised by a consumer, they will be caught by rejected,
logged, and turned into a :py:class:`ConsumerException <rejected.consumer.ConsumerException>`.

.. autoclass:: rejected.consumer.ConsumerException
   :members:

.. autoclass:: rejected.consumer.MessageException
   :members:
