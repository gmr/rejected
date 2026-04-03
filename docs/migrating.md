# Migrating from 3.x to 4.0

Rejected 4.0 is a major release that modernizes the framework around `asyncio`,
Pydantic models, and a cleaner module structure. This guide covers all breaking
changes and how to update your consumers.

## Python Version

Rejected 4.0 requires **Python 3.11+**. Python 2.7 and 3.5-3.10 are no longer
supported.

## Tornado Removed

The most significant change in 4.0 is the removal of the Tornado dependency.
Rejected now uses `asyncio` directly via pika's `AsyncioConnection` adapter.

**If your consumer uses any Tornado APIs** (`tornado.gen`, `tornado.ioloop`,
`tornado.concurrent`, etc.), you must replace them with their `asyncio`
equivalents.

| 3.x (Tornado) | 4.0 (asyncio) |
|---------------|--------------|
| `@gen.coroutine` / `yield` | `async def` / `await` |
| `tornado.gen.sleep(n)` | `asyncio.sleep(n)` |
| `tornado.concurrent.Future` | `asyncio.Future` |
| `self.io_loop` | `asyncio.get_event_loop()` |
| `self.yield_to_ioloop()` | `await asyncio.sleep(0)` |

## Consumer Classes

### All methods are now async

In 3.x, consumer methods (`process`, `prepare`, `finish`, etc.) were
synchronous by default and could optionally use `@gen.coroutine`. In 4.0,
all lifecycle methods **must** be `async def`:

```python
# 3.x
class MyConsumer(consumer.Consumer):
    def process(self):
        self.logger.info(self.body)

# 3.x with coroutine
class MyConsumer(consumer.Consumer):
    @gen.coroutine
    def process(self):
        yield self.do_something()

# 4.0
import rejected

class MyConsumer(rejected.Consumer):
    async def process(self):
        self.logger.info(self.body)
        await self.do_something()
```

### publish_message is now async

In 3.x, `publish_message` was synchronous. In 4.0, it is `async` because
it may encode the message body using an async Avro codec:

```python
# 3.x
self.publish_message('exchange', 'routing-key', {}, self.body)

# 4.0
await self.publish_message('exchange', 'routing-key', {}, self.body)
```

### SmartConsumer and PublishingConsumer removed

`SmartConsumer` and `PublishingConsumer` have been removed. Their functionality
is now built into the base `Consumer` class:

- **Auto-deserialization** (previously `SmartConsumer`): `Consumer` now
  automatically decodes message bodies based on `content_type` and
  `content_encoding`. `self.body` returns the deserialized value.
- **Publishing** (previously `PublishingConsumer`): `Consumer` has always
  supported `publish_message`; the separate class was a no-op since 3.x.

```python
# 3.x
class MyConsumer(consumer.SmartConsumer):
    def process(self):
        # self.body is auto-deserialized
        data = self.body
        self.publish_message(...)

# 4.0
import rejected

class MyConsumer(rejected.Consumer):
    async def process(self):
        # self.body is auto-deserialized (same behavior)
        data = self.body
        await self.publish_message(...)
```

### FunctionalConsumer (new)

4.0 introduces `FunctionalConsumer` for concurrent message processing.
Unlike `Consumer` (which holds a lock), `FunctionalConsumer` passes a
`ProcessingContext` to each method and allows multiple messages to be
processed in parallel:

```python
import rejected

class MyConcurrentConsumer(rejected.FunctionalConsumer):
    async def process(self, ctx: rejected.ProcessingContext):
        self.logger.info('Body: %s', ctx.message.body)
```

Use `FunctionalConsumer` with `qos_prefetch > 1` in the configuration.

### ACK_PROCESSING_EXCEPTIONS behavior

The `ACK_PROCESSING_EXCEPTIONS` class attribute still works the same way.
When `True`, messages that raise `ProcessingException` are acknowledged
instead of rejected.

## Module Reorganization

### New modules

| Module | Purpose |
|--------|---------|
| `rejected.codecs` | Message serialization/deserialization (extracted from `Consumer`) |
| `rejected.config` | Configuration loading and `Settings` class |
| `rejected.connection` | RabbitMQ connection management (extracted from `process.py`) |
| `rejected.exceptions` | Exception classes (extracted from `consumer.py`) |
| `rejected.measurement` | `Measurement` class (moved from `rejected.data`) |
| `rejected.models` | Pydantic models for config, messages, and processing context |
| `rejected.prometheus` | Prometheus metrics exporter (new) |

### Removed modules

| Module | Replacement |
|--------|-------------|
| `rejected.data` | `rejected.measurement.Measurement`, `rejected.models.Message` |

### Import changes

In 4.0, the primary consumer classes, exceptions, and models are all
available directly from the `rejected` package:

```python
# 3.x
from rejected.consumer import Consumer, ConsumerException, MessageException
from rejected.data import Measurement

# 4.0 (preferred — use top-level imports)
import rejected

class MyConsumer(rejected.Consumer): ...
class MyConcurrent(rejected.FunctionalConsumer): ...

# Exceptions
raise rejected.ConsumerException('...')
raise rejected.MessageException('...')
raise rejected.ProcessingException('...')

# Models
ctx: rejected.ProcessingContext
msg: rejected.Message
result: rejected.Result
```

Sub-module imports still work for backward compatibility or if you prefer
explicit paths:

```python
from rejected.exceptions import ConsumerException
from rejected.measurement import Measurement
from rejected.models import Message, ProcessingContext
```

## Data Classes

### Message

The `rejected.data.Message` and `rejected.data.Properties` classes have been
replaced by `rejected.Message` (a Pydantic model). If you accessed these
directly in tests or custom code:

```python
# 3.x
from rejected.data import Message, Properties
msg = Message(channel, method, header, properties, body)
msg.properties.content_type

# 4.0
import rejected

msg = rejected.Message(
    delivery_tag=1,
    exchange='exchange',
    routing_key='key',
    body=b'...',
    content_type='application/json',
    # ... all properties are top-level fields
)
msg.content_type
```

Key differences:

- Properties are top-level fields on `Message`, not nested under
  `msg.properties`
- The AMQP `type` property is accessed as `msg.type` (matching the AMQP
  property name)
- `Message` is a Pydantic `BaseModel` with validation
- `body` starts as raw bytes; the `Codec` class decodes it asynchronously
  before the consumer sees it

### Measurement

`rejected.data.Measurement` has moved to `rejected.measurement.Measurement`.
The API is unchanged.

## Testing

### AsyncTestCase

`rejected.testing.AsyncTestCase` now extends
`unittest.IsolatedAsyncioTestCase` instead of `tornado.testing.AsyncTestCase`.

```python
# 3.x
from rejected import testing

class MyTest(testing.AsyncTestCase):
    def get_consumer(self):
        return MyConsumer

    @testing.gen_test
    def test_it(self):
        yield self.process_message({'key': 'value'})

# 4.0
from rejected import testing

class MyTest(testing.AsyncTestCase):
    def get_consumer(self):
        return MyConsumer

    async def test_it(self):
        await self.process_message({'key': 'value'})
```

Key changes:

- Test methods must be `async def` (no `@gen_test` decorator)
- `setUp` -> `asyncSetUp` (called automatically by the framework)
- `create_message` -> `create_context` (returns a `ProcessingContext`)
- `process_message` returns a `Measurement` on success (unchanged)
- `process_message` raises exceptions directly on consumer errors (unchanged)

### PublishedMessage

`rejected.testing.PublishedMessage` is unchanged. Access published messages
via `self.published_messages` in your tests.

## Configuration

### TOML support

4.0 adds TOML configuration file support alongside YAML. Use a `.toml` file
extension and the configuration will be parsed accordingly.

### schema_registry (new)

Avro schema configuration has moved from the consumer level to the
application level:

```yaml
# 3.x (not available - schemas were not configurable)

# 4.0
Application:
  schema_registry:
    type: file   # or http
    uri: file:///etc/avro/{0}.avsc
```

### Consumer connections

The structured connection `publisher_confirmation` field has been renamed to
`confirm`:

```yaml
# 3.x
connections:
  - name: rabbitmq
    consume: true
    publisher_confirmation: true

# 4.0
connections:
  - name: rabbitmq
    consume: true
    confirm: true
```

### Removed configuration

| Removed | Notes |
|---------|-------|
| `Daemon` section | Rejected no longer daemonizes; use systemd/supervisord |
| `stats.influxdb` | InfluxDB support removed; use Prometheus |
| `dynamic_qos` | QoS is now static via `qos_prefetch` |
| `-f` / `--foreground` CLI flag | Rejected always runs in the foreground |

## Removed APIs

| 3.x API | 4.0 Replacement |
|---------|----------------|
| `consumer.SmartConsumer` | `rejected.Consumer` (auto-deserializes) |
| `consumer.PublishingConsumer` | `rejected.Consumer` (always could publish) |
| `self.io_loop` | `asyncio.get_event_loop()` |
| `self.yield_to_ioloop()` | `await asyncio.sleep(0)` |
| `self.reply(...)` | Use `await self.publish_message(...)` with `reply_to` |
| `self.sentry_client` | Use `self.send_exception_to_sentry()` |
| `self.stats_add_timing(...)` | `self.stats_add_duration(...)` |
| `self.statsd_add_timing(...)` | `self.stats_add_duration(...)` |
| `self.statsd_incr(...)` | `self.stats_incr(...)` |
| `self.statsd_track_duration(...)` | `self.stats_track_duration(...)` |
| `rejected.data.Data` | Removed |
| `rejected.data.Message` | `rejected.Message` |
| `rejected.data.Properties` | Fields are on `rejected.Message` |
| `pickle` content types | Removed for security (RCE risk) |

## Prometheus Metrics (new)

4.0 adds a built-in Prometheus metrics exporter. Enable it in the
configuration:

```yaml
Application:
  stats:
    prometheus:
      enabled: true
      port: 9090
```

See the [Configuration](configuration.md#prometheus-metrics) page for the
full list of exposed metrics. Custom metrics emitted via `stats_incr`,
`stats_add_duration`, and `stats_set_value` are automatically forwarded
to Prometheus.

## Quick Migration Checklist

1. Ensure Python >= 3.11
2. Update imports to use `import rejected` and `rejected.Consumer`, etc.
3. Replace `@gen.coroutine` / `yield` with `async def` / `await`
4. Make `process()`, `prepare()`, `finish()` async
5. Add `await` to `publish_message()` calls
6. Replace `SmartConsumer` / `PublishingConsumer` with `rejected.Consumer`
7. Replace `tornado` imports with `asyncio` equivalents
8. Replace `from rejected.data import ...` with `rejected.Message`,
   `rejected.measurement.Measurement`, etc.
9. Update tests to use `async def` test methods (no `@gen_test`)
10. Rename `publisher_confirmation` to `confirm` in connection config
11. Remove `Daemon` section from config (if present)
12. Remove `stats.influxdb` from config (use Prometheus instead)
13. Remove any `pickle` content type usage
14. Replace deprecated `statsd_*` method calls with `stats_*` equivalents
