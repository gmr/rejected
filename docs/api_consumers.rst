Consumer Classes
================
The :class:`~rejected.consumer.Consumer`, and
:class:`~rejected.smart_consumer.SmartConsumer` provide base classes to extend
for consumer applications.

While the :class:`~rejected.smart_consumer.SmartConsumer` class provides all
the structure required for implementing a rejected consumer, the
:class:`~rejected.smart_consumer.SmartConsumer` adds functionality designed to
make writing consumers even easier. When messages are received by consumers
extending :class:`~rejected.smart_consumer.SmartConsumer`, if the message's
:attr:`~rejected.smart_consumer.SmartConsumer.content_type` property contains
one of the supported mime-types, the message body will automatically be
deserialized, making the deserialized message body
:attr:`~rejected.smart_consumer.SmartConsumer.content_encoding` types
(``gzip`` or ``bzip2``) be specified in the message's property, it will
automatically be decoded.

The following built-in classes are available to extend for implementing a
consumer application:

.. toctree::
   :maxdepth: 1

   api_consumer
   api_smart_consumer

In addition to the consumer classes, rejected also contains :doc:`mixins <api_mixins>`
adding additional functionality.
