Changelog
=========

3.20.5
------

- When TCP statsd is enabled and the statsd client can not connect, shutdown `rejected.process.Process`
- Only log TCP statsd send_metric failures when the client believes it is connected
- Log state on AMQP connection failure

3.20.4
------

- FIXED :meth:`~rejected.consumer.Consumer.initialize` getting called twice in `rejected.testing`

3.20.3
------

- Catch an operational exceptions when checking if a process is still alive

3.20.2
------

- Catch a few operational exceptions when starting a consumer process

3.20.1
------

- Don't expect asyncio's Futures to have `exc_info()`.

3.20.0
------

- flake8 cleanup
- Update pins and minor fixes allowing support for Tornado 6, pika 0.13

3.19.21
-------

- Better handle RabbitMQ connection failures, forced connection close, broker shutdowns, etc

3.19.20
-------

- Address odd :meth:`~rejected.process.Connection.on_channel_closed` behavior with
  spinning connection creation when :exc:`pika.exceptions.ConnectionClosed`
  while trying to create a new channel.

3.19.19
-------

- FIXED :exc:`rejected.consumer.RejectedException` to not blow up when an exception
  was created with no args or kwargs.

3.19.18
-------

- FIXED :exc:`rejected.consumer.RejectedException` log message formatting when
  using format strings in the passed in value.

3.19.17
-------

- Ensure exceptions are cast to strings when logging in :class:`rejected.consumer.Consumer`

3.19.16
-------

- FIXED :meth:`~rejected.process.Connection.on_open` when pika raises
  :exc:`pika.exceptions.ConnectionClosed` when trying to create a new channel.

3.19.15
-------

- FIXED :meth:`~rejected.process.Connection.on_channel_closed` when pika raises
  :exc:`pika.exceptions.ConnectionClosed` when trying to create a new channel.

3.19.14
-------

- Fix misnamed ACK_PROCESSING_EXCEPTIONS constant in processing logic
- Hard pin to pika 0.12.0 due to breaking changes in 0.13

3.19.13
-------

- FIXED :exc:`rejected.consumer.RejectedException` to pull the metric and value
  args from the kwargs instead of explicitly defining them. This allows for
  consumers prior to 3.19 to experience the same metric style behavior as
  before.
- ADDED ``ACK_PROCESSING_EXCEPTIONS`` class level attribute to :class:`rejected.consumer.Consumer`
  that allows a consumer to ack a :exc:`~rejected.consumer.ProcessingException`
  instead of rejecting it, constraining the use of dead-lettering in RbbitMQ
  to :exc:`~rejected.consumer.MessageException`s.

3.19.12
-------

- Loosen the pika pin to work with Python 3.7

3.19.11
-------

- FIXED consumer cancellation handling to shutdown the connection

3.19.10
-------

- Address shutdown and dead process bugs

3.19.9
------

- TCP statsd wants a linefeed

3.19.8
------

- FIXED statsd TCP configuration setting bug (str vs bool)
- Log setup exceptions


3.19.7
------

- ADDED ability to connect to statsd via TCP for submitting metrics

3.19.6
------

- ADDED ability to disable including the hostname when submitting stats to statsd

3.19.5
------

- Add SSL connection flag support to configuration `#20 <https://github.com/gmr/rejected/pull/20>`_ - `code-fabriek <https://github.com/code-fabriek>`_
- Fix documentation for :py:class:`rejected.data.Measurement`
- Alter logging levels for connection failures
- Add :py:attr:`rejected.testing.AsyncTestCase.measurement`

3.19.4
------

- Try to handle a MCP process cleanup race condition better (Sentry REJECTED-DA)

3.19.3
------

- Really fix a bug with the processing time and message age metrics

3.19.2
------

- Fix a bug with the processing time and message age metrics
- Catch a timeout when waiting on a zombie

3.19.1
------

- Fix a bug in the new durations code

3.19.0
------

- Sentry client changes:
  - Do not assign version, let the client figure that out
  - Do not specify the versions of loaded modules, let the client figure that out
- Add `rejected.data.Measurement.add_duration`, changing the behavior of
  recorded durations, creating a stack of timings instead of a single timing
  for the key. For InfluxDB submissions, if there is a only a single value,
  that metric will continue to submit as previous versions. If there are multiple,
  the average, min, max, median, and 95th percentile values will be submitted.
- Add `rejected.consumer.Consumer.stats_add_duration`
- Deprecate `rejected.consumer.Consumer.stats_add_timing`
- Deprecate `rejected.consumer.Consumer.stats_add_timing`
- Consumer tags are now in the format `[consumer-name]-[os PID]`
- Created a base exception class `rejected.consumer.RejectedException`
- `rejected.consumer.ConsumerException`, `rejected.consumer.MessageException`,
  and `rejected.consumer.ProcessingException` extend `rejected.consumer.RejectedException`
- If a `rejected.consumer.ConsumerException`, `rejected.consumer.MessageException`,
  or `rejected.consumer.ProcessingException` are passed a keyword of `metric`,
  the consumer will automatically instrument a counter (statsd) or tag (InfluxDB)
  using the `metric` value.
- `rejected.consumer.ConsumerException`, `rejected.consumer.MessageException`,
  and `rejected.consumer.ProcessingException` now support "new style" string formatting,
  automatically applying the args and keyword args that are passed into the creation
  of the exception.
- Logging levels for exceptions changed:
  - `rejected.consumer.ConsumerException` are logged with error
  - `rejected.consumer.MessageException` are logged with info
  - `rejected.consumer.ProcessingException` are logged with warning
- Fix the handling of child startup failures in the MCP
- Fix a bug where un-configured consumers caused an exception in the MCP
- Handle the edge case when a connection specified in the consumer config does not exist
- Refactor how the version of the consumer module or package is determined
- Add `ProcessingException` as a top-level package export
- Fix misc docstrings
- Fix the use of `SIGABRT` being used from child processes to notify the MCP when
  processes exit, instead register for `SIGCHLD` in the MCP.

3.18.9
------

- Added :meth:`rejected.testing.AsyncTestCase.published_messages` and :class:`rejected.testing.PublishedMessage`
- Updated testing documentation
- Updated the setup.py extras install for testing to install all testing dependencies
- Made `raven` optional in `rejected.testing`

3.18.8
------

- Fix the mocks in `rejected.testing`

3.18.7
------

- Fix child process errors in shutdown
- Fix unfiltered connection list returned to a process, introduced in 3.18.4

3.18.6
------

- Move message age stat to Consumer, add method to override key

3.18.5
------

- Treat NotImplementedError as an unhandled exception

3.18.4
------

- Handle UNHANDLED_EXCEPTION in rejected.testing
- Add the `rejected.consumer.Consumer.io_loop` property
- Add the `testing` setup.py `extras_require` entry

3.18.3
------

- Fix ``rejected.consumer.Consumer.require_setting``

3.18.2
------

- Fix the republishing of dropped messages

3.18.1
------

- Fix ``ProcessingException`` AMQP header property assignment

3.18.0
------

- Add connection as an attribute of channel in ``rejected.testing``
- Refactor how error text is extracted in ``rejected.consumer.Consumer.execute``
- When a message raises a ProcessingException, the string value of the exception is added to the AMQP message headers property
- Messages dropped by a consumer can now be republished to a different exchange

3.17.4
------

- Don't start consuming until all connections are ready, fix shutdown

3.17.3
------

- Fix publisher confirmations

3.17.2
------

- Don't blow up if `stats` is not defined in config

3.17.1
------

- Documentation updates
- Fix the test for Consumer configuration

3.17.0
------

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

3.16.7
------

- Allow for any AMQP properties when testing

3.16.6
------

- Refactor and cleanup Sentry configuration and behavior

3.16.5
------

- Fix InfluxDB error metrics

3.16.4
------

- Update logging levels in `rejected.consumer.Consumer._execute`
- Set exception error strings in per-request measurements

3.16.3
------

- Better exception logging/sentry use in async consumers

3.16.2
------

- Fix a bug using -o in Python 3

3.16.1
------

- Add `rejected.consumer.Consumer.send_exception_to_sentry`

3.16.0
------

- Add `rejected.testing` testing framework

3.15.1
------

- Ensure that message age is always a float

3.15.0
------

- Sentry Updates
    - Catch all top-level startup exceptions and send them to sentry
    - Fix the sending of consumer exceptions to sentry

3.14.0
------

- Cleanup the shutdown and provide way to bypass cache in active_processes
- If a consumer has not responded back with stats info after 3 attempts, it will be shutdown and a new consumer will take its place.
- Add the consumer name to the extra values for logging

3.13.4
------

- Properly handle finishing in `rejected.consumer.Consumer.prepare`
- Fix default/class level config of error exchange, etc

3.13.3
------

- Fix `rejected.consumer.Consumer.stats_track_duration`

3.13.2
------

- Better backwards compatibility with `rejected.consumer.Consumer` "stats" commands

3.13.1
------

- Bugfixes:
    - Construct the proper InfluxDB base URL
    - Fix the mixin __init__ signature to support the new kwargs
    - Remove overly verbose logging

3.13.0
------

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
