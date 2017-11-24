rejected.consumer.SmartConsumer
===============================
An opinionated consumer class that attempts to automatically understand message
bodies based upon AMQP message property values.

.. autoclass:: rejected.consumer.SmartConsumer

   .. rubric:: Extendable Per-Message Methods

   The :py:meth:`~rejected.consumer.SmartConsumer.prepare`,
   :py:meth:`~rejected.consumer.SmartConsumer.process`, and
   :py:meth:`~rejected.consumer.SmartConsumer.on_finish` methods are invoked in
   order, once per message that is delivered from RabbitMQ. Extend these methods
   to implement the primary behaviors for your consumer application.

   .. note:: If :py:meth:`~rejected.consumer.SmartConsumer.finish` is called in the
      :py:meth:`~rejected.consumer.SmartConsumer.prepare` method,
      :py:meth:`~rejected.consumer.SmartConsumer.process` will **not** be called.

   The following example consumer demonstrates the use of the three methods
   that a generally invoked for every message that is delivered. While
   you do not have to implement the :py:meth:`~rejected.consumer.SmartConsumer.prepare`
   or :py:meth:`~rejected.consumer.SmartConsumer.on_finish` methods, you must
   implement the :py:meth:`~rejected.consumer.SmartConsumer.process` method for your
   consumer to properly function.

   .. code-block:: python
      :caption: Message Lifecycle Example

      class Consumer(consumer.SmartConsumer):

          def __init__(self, *args, **kwargs):
              super(Consumer, self).__init__(*args, **kwargs)
              self.current_id, self.previous_id = None, None

          def prepare(self):
              try:
                 self.current_id = self.body['id']
              except KeyError:
                  raise consumer.MessageException('Missing ID in body',
                                                  metric='missing-id')
              return super(Consumer, self).prepare()

          def process(self):
              self.logger.info('Current ID: %s, Previous ID: %s',
                               self.current_id, self.previous_id)

          def on_finish(self):
              self.previous_id = self.current_id
              self.current_id = None

   .. automethod:: rejected.consumer.SmartConsumer.prepare(self)
   .. automethod:: rejected.consumer.SmartConsumer.process(self)
   .. automethod:: rejected.consumer.SmartConsumer.on_finish(self)


   .. rubric:: Class Constants

   The following class-level constants can have a direct impact on the message
   processing behavior of a consumer.

   .. autoattribute:: rejected.consumer.SmartConsumer.MESSAGE_TYPE
   .. autoattribute:: rejected.consumer.SmartConsumer.DROP_INVALID_MESSAGES
   .. autoattribute:: rejected.consumer.SmartConsumer.DROP_EXCHANGE
   .. autoattribute:: rejected.consumer.SmartConsumer.ERROR_MAX_RETRIES
   .. autoattribute:: rejected.consumer.SmartConsumer.ERROR_MAX_RETRY
   .. autoattribute:: rejected.consumer.SmartConsumer.ERROR_EXCHANGE
   .. autoattribute:: rejected.consumer.SmartConsumer.IGNORE_OOB_STATS
   .. autoattribute:: rejected.consumer.SmartConsumer.MESSAGE_AGE_KEY

   .. rubric:: Object Properties

   The following object level properties can be used to access the current
   Tornado :class:`~tornado.ioloop.IOLoop`, the consumer's name, and the
   consumer's configuration as defined in the ``config`` stanza in the consumer
   configuration.

   .. autoattribute:: rejected.consumer.SmartConsumer.io_loop
   .. autoattribute:: rejected.consumer.SmartConsumer.name
   .. autoattribute:: rejected.consumer.SmartConsumer.settings

   .. rubric:: General Extendable Methods

   Extend the any of the following methods to implement behaviors that are used
   at various stages of the Consumer's application lifecycle.

   .. automethod:: rejected.consumer.SmartConsumer.initialize(self)
   .. automethod:: rejected.consumer.SmartConsumer.message_age_key(self)
   .. automethod:: rejected.consumer.SmartConsumer.on_blocked(self, name)
   .. automethod:: rejected.consumer.SmartConsumer.on_unblocked(self, name)
   .. automethod:: rejected.consumer.SmartConsumer.require_setting(self, name, feature)
   .. automethod:: rejected.consumer.SmartConsumer.shutdown(self)

   .. rubric:: Publishing Methods

   The following methods are used to publish messages from the consumer while
   processing a message.

   .. automethod:: rejected.consumer.SmartConsumer.publish_message(self, exchange, routing_key, properties, body, channel=None, connection=None)
   .. automethod:: rejected.consumer.SmartConsumer.rpc_reply(self, body, properties=None, exchange=None, reply_to=None, connection=None)

   .. rubric:: Stats Methods

   The following methods are used to collect statistical information that is
   submitted to InfluxDB or StatsD if configured.

   .. Note:: All data collected by invoking these methods is not submitted until
      after the message has been fully processed.

   .. automethod:: rejected.consumer.SmartConsumer.stats_add_duration(self, key, duration)
   .. automethod:: rejected.consumer.SmartConsumer.stats_incr(self, key, value=1)
   .. automethod:: rejected.consumer.SmartConsumer.stats_set_tag(self, key, value=1)
   .. automethod:: rejected.consumer.SmartConsumer.stats_set_value(self, key, value=1)
   .. automethod:: rejected.consumer.SmartConsumer.stats_track_duration(self, key)
   .. autoattribute:: rejected.consumer.SmartConsumer.measurement

   .. rubric:: Sentry Support

   The following methods are available to a consumer if
   `Sentry <https://sentry.io>` integration is enabled.

   .. autoattribute:: rejected.consumer.SmartConsumer.sentry_client
   .. automethod:: rejected.consumer.SmartConsumer.send_exception_to_sentry(self, exc_info)
   .. automethod:: rejected.consumer.SmartConsumer.set_sentry_context(self, tag, value)
   .. automethod:: rejected.consumer.SmartConsumer.unset_sentry_context(self, tag)

   .. rubric:: Other

   The following methods can be invoked while processing a message.

   .. automethod:: rejected.consumer.SmartConsumer.finish(self)
   .. automethod:: rejected.consumer.SmartConsumer.yield_to_ioloop(self)

   .. rubric:: Message Related Properties

   The following properties of a consumer object instance are used to access
   top-level information about the current message, including the message body,
   and routing information.

   .. autoattribute:: rejected.consumer.SmartConsumer.body
   .. autoattribute:: rejected.consumer.SmartConsumer.exchange
   .. autoattribute:: rejected.consumer.SmartConsumer.routing_key
   .. autoattribute:: rejected.consumer.SmartConsumer.properties
   .. autoattribute:: rejected.consumer.SmartConsumer.redelivered

   .. rubric:: Message AMQP Properties

   The following consumer object properties contain the AMQP message properties
   that were specified for the current message.

   .. autoattribute:: rejected.consumer.SmartConsumer.app_id
   .. autoattribute:: rejected.consumer.SmartConsumer.content_encoding
   .. autoattribute:: rejected.consumer.SmartConsumer.content_type
   .. autoattribute:: rejected.consumer.SmartConsumer.correlation_id
   .. autoattribute:: rejected.consumer.SmartConsumer.expiration
   .. autoattribute:: rejected.consumer.SmartConsumer.headers
   .. autoattribute:: rejected.consumer.SmartConsumer.message_id
   .. autoattribute:: rejected.consumer.SmartConsumer.message_type
   .. autoattribute:: rejected.consumer.SmartConsumer.priority
   .. autoattribute:: rejected.consumer.SmartConsumer.reply_to
   .. autoattribute:: rejected.consumer.SmartConsumer.timestamp
   .. autoattribute:: rejected.consumer.SmartConsumer.user_id

