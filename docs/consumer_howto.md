# Consumer How-To

## Basic Consumer

The following example illustrates a very simple consumer that logs each
message body as it's received.

```python
import logging

import rejected

__version__ = '1.0.0'

LOGGER = logging.getLogger(__name__)


class ExampleConsumer(rejected.Consumer):

    async def process(self):
        LOGGER.info(self.body)
```

All interaction with RabbitMQ with regard to connection management and message
handling, including acknowledgements and rejections are automatically handled
for you.

The `__version__` variable provides context in the rejected log files when
consumers are started and can be useful for investigating consumer behaviors in
production.

## Consumer Lifecycle

`Consumer` processes one message at a time using a lock. Override the
following async methods to hook into the lifecycle:

| Method | When it runs |
|--------|-------------|
| `initialize()` | Once, before the first message is processed |
| `prepare()` | Before `process()` for each message |
| `process()` | Your consumer logic (required) |
| `finish()` | After `process()` for each message |
| `on_finish()` | After processing completes (including errors) |
| `shutdown()` | When the process is stopping |

## Error Handling

In this next example, a contrived `_connect_to_database` method returns `False`.
When `process` evaluates the result and finds it cannot connect, it raises a
`ConsumerException` which will requeue the message in RabbitMQ and increment an
error counter. When too many errors occur, rejected will automatically restart
the consumer after a brief quiet period.

```python
import logging

import rejected

__version__ = '1.0.0'

LOGGER = logging.getLogger(__name__)


class ExampleConsumer(rejected.Consumer):

    def _connect_to_database(self):
        return False

    async def process(self):
        if not self._connect_to_database():
            raise rejected.ConsumerException('Database error')
        LOGGER.info(self.body)
```

### Exception Types

There are three exception types that consumers should raise to handle
problems when processing a message:

**`ConsumerException`**

Raised when there is a problem with the consumer itself, such as inability to
contact a database server or other resources. The message will be rejected
*and* requeued, adding it back to the queue it was delivered from.
Rejected tracks consumer exceptions and will shut down the consumer process
and start a new one once the configured `max_errors` count is exceeded within
a 60-second window (default: 5).

**`MessageException`**

Raised when there is a problem with the message itself, such as a malformed
payload or unsupported properties. The message will be rejected *without*
requeue, discarding the message.

**`ProcessingException`**

Raised when a message should be retried later. The message is republished to
the configured `error_exchange` (default: `errors`) with the original routing
key and body. Two headers are added:

- `X-Processing-Exception` -- the string value of the exception
- `X-Processing-Exceptions` -- a counter of how many times the message has
  been retried

Combined with a queue that has `x-message-ttl` and `x-dead-letter-exchange`
pointing back to the original exchange, this implements a delayed retry cycle.

If `ERROR_MAX_RETRY` is set on the consumer class (or `error_max_retry` in
the config), messages that exceed the retry count will be dropped. If a
`drop_exchange` is configured, dropped messages are republished there with
`X-Dropped-By`, `X-Dropped-Reason`, `X-Dropped-Timestamp`, and
`X-Original-Exchange` headers.

!!! note
    Unhandled exceptions raised by a consumer are caught by rejected, logged,
    reported to Sentry (if configured), and treated as unhandled errors that
    requeue the message and increment the error counter.

## Message Type Validation

If the `MESSAGE_TYPE` class attribute is set, the `type` property of incoming
messages is validated against it. `MESSAGE_TYPE` can be a string, list, tuple,
or set.

If the type does not match:

- If `DROP_INVALID_MESSAGES` is `True` (or `drop_invalid_messages` in config),
  the message is silently dropped. If `DROP_EXCHANGE` is also set, the message
  is republished there before being dropped.
- Otherwise, a `MessageException` is raised.

```python
class StrictConsumer(rejected.Consumer):
    MESSAGE_TYPE = 'user.created'
    DROP_INVALID_MESSAGES = True
    DROP_EXCHANGE = 'dead-letter'

    async def process(self):
        LOGGER.info('Processing user creation: %s', self.body)
```

## Publishing Messages

Consumers can publish messages using `publish_message`. Note that it is
an `async` method:

```python
class ExampleConsumer(rejected.Consumer):

    async def process(self):
        LOGGER.info(self.body)
        await self.publish_message(
            exchange='new-exchange',
            routing_key='routing-key',
            properties={'content_type': 'application/json'},
            body=self.body,
        )
```

The `properties` parameter is a dict of AMQP properties (e.g., `content_type`,
`type`, `correlation_id`, `headers`). The message body will be automatically
serialized and compressed based on the `content_type` and `content_encoding`
properties.

## FunctionalConsumer

`FunctionalConsumer` is designed for concurrent message processing. Unlike
`Consumer`, it does not hold a lock -- multiple messages may be processed
in parallel. Instead of accessing message properties via `self.body` etc.,
the processing context is passed explicitly:

```python
import logging

import rejected

LOGGER = logging.getLogger(__name__)


class MyConcurrentConsumer(rejected.FunctionalConsumer):

    async def prepare(self, ctx: rejected.ProcessingContext):
        LOGGER.debug('Preparing to process %s', ctx.message.message_id)

    async def process(self, ctx: rejected.ProcessingContext):
        LOGGER.info('Processing: %s', ctx.message.body)

    async def finish(self, ctx: rejected.ProcessingContext):
        LOGGER.debug('Finished processing %s', ctx.message.message_id)
```

The `ProcessingContext` provides:

- `ctx.message` -- the `Message` model with all AMQP properties
- `ctx.channel` -- the pika channel for the connection
- `ctx.connection` -- the connection object
- `ctx.measurement` -- per-message `Measurement` for custom metrics
- `ctx.raw_body` -- the original bytes before decoding
- `ctx.result` -- the `Result` enum indicating message disposition
- `ctx.received_at` -- monotonic timestamp when the message was received

Use `FunctionalConsumer` with `qos_prefetch > 1` to process multiple
messages concurrently.

## Custom Metrics

Consumers can emit custom metrics that are forwarded to statsd and/or
Prometheus:

```python
class MetricsConsumer(rejected.Consumer):

    async def process(self):
        # Increment a counter
        self.stats_incr('items_processed', len(self.body.get('items', [])))

        # Track a duration
        with self.stats_track_duration('db_query'):
            result = await self.query_database()

        # Set a gauge value
        self.stats_set_value('queue_depth', result.pending_count)
```

See the [Prometheus Metrics](configuration.md#prometheus-metrics) section
for how these map to Prometheus metric types.

## GarbageCollectorMixin

The `GarbageCollectorMixin` can be used to periodically invoke `gc.collect()`
after processing messages. By default it collects every 10,000 messages.
Configure the frequency via the `gc_collection_frequency` setting:

```python
from rejected.mixins import GarbageCollectorMixin

import rejected


class MyConsumer(GarbageCollectorMixin, rejected.Consumer):

    async def process(self):
        ...
```

```yaml
Consumers:
  my_consumer:
    consumer: mypackage.MyConsumer
    connections: [rabbitmq]
    queue: my_queue
    config:
      gc_collection_frequency: 5000
```

## Content Type Handling

Rejected automatically decodes and deserializes message bodies based on the
message's `content_type` property:

| Content Type | Deserialized To | Requirement |
|-------------|----------------|-------------|
| `application/json` | `dict`/`list` | built-in |
| `application/msgpack` | `dict`/`list` | `rejected[msgpack]` |
| `application/vnd.apache.avro.datum` | `dict` | `rejected[avro]` |
| `application/x-plist` | `dict` | built-in |
| `text/csv` | `csv.DictReader` | built-in |
| `text/html` | `BeautifulSoup` | `rejected[html]` |
| `text/xml` | `BeautifulSoup` | `rejected[html]` |
| `text/yaml`, `text/x-yaml` | `dict`/`list` | built-in |

If the `content_encoding` property is set to `gzip` or `bzip2`, the message
body is decompressed before deserialization.

When publishing messages, serialization and compression are applied
automatically in reverse based on the same properties.
