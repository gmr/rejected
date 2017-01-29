Release Notes
=============

What's New in Rejected 3.17
---------------------------
- Consumers can now have multiple connections, with the ability to specify if a connection should consume or not
- Rejected now only spawns processes the configured quantity of consumers instead of the configured quantity of consumers * number of connections per consumer.
- :class:`rejected.consumer.PublishingConsumer` has been merged into :class:`rejected.consumer.Consumer`
- :class:`rejected.consumer.SmartPublishingConsumer` has been merged into :class:`rejected.consumer.SmartConsumer`
- Add new ``enabled`` flag in the config for statsd and influxdb stats monitoring
- Add a new behavior that puts pending messages sent into a :class:`collections.deque` when a consumer is processing instead of just blocking on message delivery until processing is done. This could have a negative impact on memory utilization for consumers with large messages, but can be controlled by the ``qos_prefetch`` setting.
- Consumers can now receive messages returned from RabbitMQ
- Consumers now have a callback to be notified when RabbitMQ blocks and unblocks a connection
- :class:`rejected.testing.AsyncTestCase` improvements
- Minor bugfixes
