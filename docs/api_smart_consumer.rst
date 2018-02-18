SmartConsumer
=============
An opinionated consumer class that attempts to automatically understand message
bodies based upon AMQP message property values.

The :class:`~rejected.smart_consumer.SmartConsumer` class adds functionality
designed to make writing consumers even easier. When messages are received by
consumers extending :class:`~rejected.smart_consumer.SmartConsumer`, if the
message's :class:`content_type <rejected.data.Properties>` property contains
one of the supported mime-types, the message body will automatically be
deserialized, making the deserialized message body available via the
:attr:`~rejected.consumer.Consumer.body` attribute. Additionally, should one
of the supported :class:`content_encoding <rejected.data.Properties>` types
(``gzip`` or ``bzip2``) be specified in the message's property,
it will automatically be decoded.

Supported :class:`~rejected.smart_consumer.SmartConsumer` MIME types are:

 - ``application/json``
 - ``application/msgpack`` (with `u-msgpack-python <https://pypi.org/project/u-msgpack-python/>`_ installed)
 - ``application/pickle``
 - ``application/x-pickle``
 - ``application/x-plist``
 - ``application/x-vnd.python.pickle``
 - ``application/vnd.python.pickle``
 - ``text/csv``
 - ``text/html`` (with `beautifulsoup4 <https://pypi.org/project/beautifulsoup4/>`_ installed)
 - ``text/xml`` (with `beautifulsoup4 <https://pypi.org/project/beautifulsoup4/>`_ installed)
 - ``text/yaml``
 - ``text/x-yaml``

If the :attr:`~rejected.smart_consumer.SmartConsumer.MESSAGE_TYPE` attribute is set,
the :class:`type <rejected.data.Properties>` value of incoming messages
will be validated against when a message is received, checking the
:attr:`~rejected.smart_consumer.SmartConsumer.MESSAGE_TYPE` attribute. If the attribute
is a string, a string equality check is performed. If the attribute is a
:class:`list`, :class:`set`, or :class:`tuple`, the message
:class:`type <rejected.data.Properties>` value will checked for membership in
:attr:`~rejected.smart_consumer.SmartConsumer.MESSAGE_TYPE`. If there is no match the
message will not be processed. In addition, if the
:attr:`~rejected.smart_consumer.SmartConsumer.DROP_INVALID_MESSAGES` is set to
:class:`True`, the consumer will drop the message without republishing it.
Otherwise a :exc:`~rejected.errors.MessageException` is raised.

.. autoclass:: rejected.smart_consumer.SmartConsumer

   .. rubric:: Extendable Per-Message Methods

   The :py:meth:`SmartConsumer.prepare <rejected.smart_consumer.SmartConsumer.prepare>`,
   :py:meth:`SmartConsumer.process <rejected.smart_consumer.SmartConsumer.process>`, and
   :py:meth:`SmartConsumer.on_finish <rejected.smart_consumer.SmartConsumer.on_finish>`
   methods are invoked in order, once per message that is delivered from RabbitMQ.
   Extend these methods to implement the primary behaviors for your consumer application.

   .. automethod:: rejected.smart_consumer.SmartConsumer.prepare(self)
   .. automethod:: rejected.smart_consumer.SmartConsumer.process(self)
   .. automethod:: rejected.smart_consumer.SmartConsumer.on_finish(self, exc=None)

   .. rubric:: Class Constants

   The following class-level constants can have a direct impact on the message
   processing behavior of a consumer.

   .. autoattribute:: rejected.smart_consumer.SmartConsumer.MESSAGE_TYPE
   .. autoattribute:: rejected.smart_consumer.SmartConsumer.DROP_INVALID_MESSAGES
   .. autoattribute:: rejected.smart_consumer.SmartConsumer.DROP_EXCHANGE
   .. autoattribute:: rejected.smart_consumer.SmartConsumer.ERROR_MAX_RETRIES
   .. autoattribute:: rejected.smart_consumer.SmartConsumer.ERROR_MAX_RETRY
   .. autoattribute:: rejected.smart_consumer.SmartConsumer.ERROR_EXCHANGE
   .. autoattribute:: rejected.smart_consumer.SmartConsumer.IGNORE_OOB_STATS
   .. autoattribute:: rejected.smart_consumer.SmartConsumer.MESSAGE_AGE_KEY

   .. rubric:: Object Properties

   The following object level properties can be used to access the current
   Tornado :class:`~tornado.ioloop.IOLoop`, the consumer's name, and the
   consumer's configuration as defined in the ``config`` stanza in the consumer
   configuration.

   .. autoattribute:: rejected.smart_consumer.SmartConsumer.io_loop
   .. autoattribute:: rejected.smart_consumer.SmartConsumer.name
   .. autoattribute:: rejected.smart_consumer.SmartConsumer.settings

   .. rubric:: General Extendable Methods

   Extend the any of the following methods to implement behaviors that are used
   at various stages of the Consumer's application lifecycle.

   .. automethod:: rejected.smart_consumer.SmartConsumer.initialize(self)
   .. automethod:: rejected.smart_consumer.SmartConsumer.message_age_key(self)
   .. automethod:: rejected.smart_consumer.SmartConsumer.on_blocked(self, name)
   .. automethod:: rejected.smart_consumer.SmartConsumer.on_unblocked(self, name)
   .. automethod:: rejected.smart_consumer.SmartConsumer.require_setting(self, name, feature)
   .. automethod:: rejected.smart_consumer.SmartConsumer.shutdown(self)

   .. rubric:: Publishing Methods

   The following methods are used to publish messages from the consumer while
   processing a message.

   .. automethod:: rejected.smart_consumer.SmartConsumer.publish_message(self, exchange, routing_key, properties, body, channel=None, connection=None)
   .. automethod:: rejected.smart_consumer.SmartConsumer.rpc_reply(self, body, properties=None, exchange=None, reply_to=None, connection=None)

   .. rubric:: Stats Methods

   The following methods are used to collect statistical information that is
   submitted to InfluxDB or StatsD if configured.

   .. Note:: All data collected by invoking these methods is not submitted until
      after the message has been fully processed.

   .. automethod:: rejected.smart_consumer.SmartConsumer.stats_add_duration(self, key, duration)
   .. automethod:: rejected.smart_consumer.SmartConsumer.stats_incr(self, key, value=1)
   .. automethod:: rejected.smart_consumer.SmartConsumer.stats_set_tag(self, key, value=1)
   .. automethod:: rejected.smart_consumer.SmartConsumer.stats_set_value(self, key, value=1)
   .. automethod:: rejected.smart_consumer.SmartConsumer.stats_track_duration(self, key)
   .. autoattribute:: rejected.smart_consumer.SmartConsumer.measurement

   .. rubric:: Sentry Support

   The following methods are available to a consumer if
   `Sentry <https://sentry.io>` integration is enabled.

   .. autoattribute:: rejected.smart_consumer.SmartConsumer.sentry_client
   .. automethod:: rejected.smart_consumer.SmartConsumer.send_exception_to_sentry(self, exc_info)
   .. automethod:: rejected.smart_consumer.SmartConsumer.set_sentry_context(self, tag, value)
   .. automethod:: rejected.smart_consumer.SmartConsumer.unset_sentry_context(self, tag)

   .. rubric:: Other

   The following methods can be invoked while processing a message.

   .. automethod:: rejected.smart_consumer.SmartConsumer.finish(self)
   .. automethod:: rejected.smart_consumer.SmartConsumer.yield_to_ioloop(self)

   .. rubric:: Message Related Properties

   The following properties of a consumer object instance are used to access
   top-level information about the current message, including the message body,
   and routing information.

   .. autoattribute:: rejected.smart_consumer.SmartConsumer.body
   .. autoattribute:: rejected.smart_consumer.SmartConsumer.exchange
   .. autoattribute:: rejected.smart_consumer.SmartConsumer.routing_key
   .. autoattribute:: rejected.smart_consumer.SmartConsumer.properties
   .. autoattribute:: rejected.smart_consumer.SmartConsumer.redelivered

   .. rubric:: Message AMQP Properties

   The following consumer object properties contain the AMQP message properties
   that were specified for the current message.

   .. autoattribute:: rejected.smart_consumer.SmartConsumer.app_id
   .. autoattribute:: rejected.smart_consumer.SmartConsumer.content_encoding
   .. autoattribute:: rejected.smart_consumer.SmartConsumer.content_type
   .. autoattribute:: rejected.smart_consumer.SmartConsumer.correlation_id
   .. autoattribute:: rejected.smart_consumer.SmartConsumer.expiration
   .. autoattribute:: rejected.smart_consumer.SmartConsumer.headers
   .. autoattribute:: rejected.smart_consumer.SmartConsumer.message_id
   .. autoattribute:: rejected.smart_consumer.SmartConsumer.message_type
   .. autoattribute:: rejected.smart_consumer.SmartConsumer.priority
   .. autoattribute:: rejected.smart_consumer.SmartConsumer.reply_to
   .. autoattribute:: rejected.smart_consumer.SmartConsumer.timestamp
   .. autoattribute:: rejected.smart_consumer.SmartConsumer.user_id

