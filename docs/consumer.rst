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

.. autoclass:: rejected.consumer.ConsumerException
   :members:

.. autoclass:: rejected.consumer.MessageException
   :members:
