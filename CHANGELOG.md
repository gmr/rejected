# ChangeLog

## 3.18.9

- Added `rejected.testing.AsyncTestCase.published_messages` and `rejected.testing.PublishedMessage`
- Updated testing documentation
- Updated the setup.py extras install for testing to install all testing dependencies
- Made `raven` optional in `rejected.testing`

## 3.18.8

- Fix the mocks in `rejected.testing`

## 3.18.7

- Fix child process errors in shutdown
- Fix unfiltered connection list returned to a process, introduced in 3.18.4

## 3.18.6

- Move message age stat to Consumer, add method to override key

## 3.18.5

- Treat NotImplementedError as an unhandled exception

## 3.18.4

- Handle UNHANDLED_EXCEPTION in rejected.testing
- Add the `rejected.consumer.Consumer.io_loop` property
- Add the `testing` setup.py `extras_require` entry

## 3.18.3

- Fix ``rejected.consumer.Consumer.require_setting``

## 3.18.2

- Fix the republishing of dropped messages

## 3.18.1

- Fix ``ProcessingException`` AMQP header property assignment

## 3.18.0

- Add connection as an attribute of channel in ``rejected.testing``
- Refactor how error text is extracted in ``rejected.consumer.Consumer.execute``
- When a message raises a ProcessingException, the string value of the exception is added to the AMQP message headers property
- Messages dropped by a consumer can now be republished to a different exchange

## 3.17.4

- Don't start consuming until all connections are ready, fix shutdown

## 3.17.3

- Fix publisher confirmations

## 3.17.2

- Don't blow up if `stats` is not defined in config

## 3.17.1

- Documentation updates
- Fix the test for Consumer configuration

## 3.17.0

- `rejected.testing` updates
- Add automatic assignment of `correlation-id` to `rejected.consumer.Consumer`
- Only use `sentry_client` if it’s configured
- Behavior change: Don't spawn a process per connection, Spawn `qty` consumers with N connections
- Add State.is_active
- Add attributes for the connection the message was received on and if the message was published by the consumer and returned by RabbitMQ
- Deprecate `PublishingConsumer` and `SmartPublishingConsumer`, folding them into `Consumer` and `SmartConsumer` respectively
- Refactor to not have a singular channel instance, but rather a dict of channels for all connections
- Add the ability to specify a channel to publish a message on, defaulting to the channel the message was delivered on
- Add a property that indicates the current message that is being processed was returned by RabbitMQ
- Change `Consumer._execute` and `Consumer._set_channel` to be “public” but will hide from docs.
- Major Process refactor
    - Create a new Connection class to isolate direct AMQP connection/channel management from the Process class.
    - Alter Process to allow for multiple connections. This allows a consumer to consume from multiple AMQP broker connections or have AMQP broker connections that are not used for consuming. This could be useful for consuming from one broker and publishing to another broker in a different data center.
    - Add new ``enabled`` flag in the config for statsd and influxdb stats monitoring
    - Add a new behavior that puts pending messages sent into a ``collections.deque`` when a consumer is processing instead of just blocking on message delivery until processing is done. This could have a negative impact on memory utilization for consumers with large messages, but can be controlled by the ``qos_prefetch`` setting.
    - Process now sends messages returned from RabbitMQ to the Consumer
    - Process now will notify a consumer when RabbitMQ blocks and unblocks a connection

## 3.16.7

- Allow for any AMQP properties when testing

## 3.16.6

- Refactor and cleanup Sentry configuration and behavior

## 3.16.5

- Fix InfluxDB error metrics

## 3.16.4

- Update logging levels in `rejected.consumer.Consumer._execute`
- Set exception error strings in per-request measurements

## 3.16.3

- Better exception logging/sentry use in async consumers

## 3.16.2

- Fix a bug using -o in Python 3

## 3.16.1

- Add `rejected.consumer.Consumer.send_exception_to_sentry`

## 3.16.0

- Add `rejected.testing` testing framework

## 3.15.1

- Ensure that message age is always a float

## 3.15.0

- Sentry Updates
    - Catch all top-level startup exceptions and send them to sentry
    - Fix the sending of consumer exceptions to sentry

## 3.14.0

- Cleanup the shutdown and provide way to bypass cache in active_processes
- If a consumer has not responded back with stats info after 3 attempts, it will be shutdown and a new consumer will take its place.
- Add the consumer name to the extra values for logging

## 3.13.4

- Properly handle finishing in `rejected.consumer.Consumer.prepare`
- Fix default/class level config of error exchange, etc

## 3.13.3

- Fix `rejected.consumer.Consumer.stats_track_duration`

## 3.13.2

- Better backwards compatibility with `rejected.consumer.Consumer` "stats" commands

## 3.13.1

- Bugfixes:
    - Construct the proper InfluxDB base URL
    - Fix the mixin __init__ signature to support the new kwargs
    - Remove overly verbose logging

## 3.13.0

- Remove Python 2.6 support
- Documentation Updates
- consumer.Consumer: Accept multiple MESSAGE_TYPEs.
- PublishingConsumer: Remove routing key from metric. 
- Add per-consumer sentry configuration 
- Refactor Consumer stats and statsd support
- Update to use the per-message measurement
    - Changes how we submit measurements to statsd
      - Drops some redundant measurements that were submitted
      - Renames the exception measurement names
    - Adds support for InfluxDB
