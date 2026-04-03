# Configuration

Rejected supports both [YAML](http://yaml.org) and [TOML](https://toml.io)
configuration files. The file format is determined by the file extension
(`.yaml`/`.yml` or `.toml`).

The configuration file is split into two main sections: `Application` and
`Logging`.

## Application

The application section contains the following top-level options:

| Option | Description | Default |
|--------|-------------|---------|
| `poll_interval` | How often to poll consumer processes for status (seconds) | `60.0` |
| `sentry_dsn` | Global Sentry DSN for all consumers | |
| `schema_registry` | Avro schema registry configuration (see below) | |
| `stats` | Metrics submission configuration | |
| `Connections` | RabbitMQ connection definitions | |
| `Consumers` | Consumer type configurations | |

### schema_registry

Configure the Avro schema registry for automatic message
serialization/deserialization. Requires `rejected[avro]`.

| Option | Description | Default |
|--------|-------------|---------|
| `type` | Registry type: `file` or `http` | `file` |
| `uri` | URI template with `{0}` placeholder for message type | |

Examples:

```yaml
schema_registry:
  type: file
  uri: file:///etc/avro/schemas/{0}.avsc
```

```yaml
schema_registry:
  type: http
  uri: https://schema-registry.example.com/subjects/{0}/versions/latest
```

### stats

| Option | Description | Default |
|--------|-------------|---------|
| `log` | Log consumer process stats at each poll interval | `false` |
| `prometheus` | Prometheus metrics exporter configuration | |
| `statsd` | Statsd metrics submission configuration | |

#### prometheus

Requires `rejected[prometheus]` to be installed.

| Option | Description | Default |
|--------|-------------|---------|
| `enabled` | Enable the Prometheus metrics HTTP endpoint | `false` |
| `port` | Port to serve the `/metrics` endpoint on | `9090` |

See [Prometheus Metrics](#prometheus-metrics) below for the full list of
exposed metrics.

#### statsd

| Option | Description | Default |
|--------|-------------|---------|
| `enabled` | Enable statsd metrics reporting | `false` |
| `host` | Statsd server hostname or IP address | `localhost` |
| `port` | Statsd server port | `8125` |
| `prefix` | Prefix for all metric paths | `rejected` |
| `include_hostname` | Include the hostname in the metric path | `true` |
| `tcp` | Use TCP instead of UDP | `false` |

### Connections

Each RabbitMQ connection entry is a named object with connection attributes.

| Option | Description | Default |
|--------|-------------|---------|
| `host` | RabbitMQ server hostname or IP address | `localhost` |
| `port` | RabbitMQ server port | `5672` |
| `vhost` | Virtual host to connect to | `/` |
| `user` | Username | `guest` |
| `pass` | Password | `guest` |
| `ssl` | Enable SSL/TLS for the connection | `false` |
| `ssl_options` | SSL/TLS options (see below) | |
| `heartbeat_interval` | AMQP heartbeat interval in seconds | `300` |
| `frame_max` | Maximum AMQP frame size in bytes | `131072` |
| `socket_timeout` | Socket timeout in seconds | `10` |

#### ssl_options

| Option | Description |
|--------|-------------|
| `ca_certs` | File path to concatenated CA certificates |
| `ca_path` | Directory path to PEM formatted CA certificates |
| `ca_data` | PEM encoded CA certificate data |
| `protocol` | SSL protocol constant name or integer (default: `PROTOCOL_TLS_CLIENT`) |
| `certfile` | File path to PEM formatted client certificate |
| `keyfile` | File path to certificate private key |
| `password` | Password for decrypting the private key |
| `ciphers` | Available ciphers in OpenSSL cipher list format |
| `server_hostname` | Override the expected server hostname for SNI |

!!! warning
    Using a protocol other than `PROTOCOL_TLS_CLIENT` will log a warning.
    `PROTOCOL_TLS_CLIENT` is strongly recommended as it provides secure
    defaults including certificate verification.

### Consumers

Each consumer entry is a named object with the following attributes:

| Option | Description | Default |
|--------|-------------|---------|
| `consumer` | The `package.module.Class` path to the consumer code | |
| `connections` | Connections to use (see below) | |
| `qty` | Number of consumer processes to run | `1` |
| `queue` | RabbitMQ queue name to consume from (defaults to consumer name) | |
| `ack` | Explicitly acknowledge messages (`no_ack = !ack`) | `true` |
| `qos_prefetch` | QoS prefetch count (set > 1 for concurrent processing with `FunctionalConsumer`) | `1` |
| `max_errors` | Errors within 60s before restarting the consumer | `5` |
| `error_exchange` | Exchange to republish messages to on `ProcessingException` | |
| `error_max_retry` | Max `ProcessingException` retries before dropping | |
| `sentry_dsn` | Consumer-specific Sentry DSN (overrides global) | |
| `drop_exchange` | Exchange to publish dropped messages to | |
| `drop_invalid_messages` | Drop messages with non-matching type instead of raising `MessageException` | |
| `message_type` | Validate message `type` property before processing | |
| `config` | Free-form key-value settings passed to the consumer | |

#### Consumer Connections

Connections can be specified as a simple list of connection names:

```yaml
Consumers:
  my_consumer:
    connections:
      - connection1
      - connection2
```

Or with structured values for finer control:

```yaml
Consumers:
  my_consumer:
    connections:
      - name: connection1
        consume: true
        confirm: false
      - name: connection2
        consume: false
        confirm: true
```

Structured connection options:

| Option | Description | Default |
|--------|-------------|---------|
| `name` | The connection name, as defined in the Connections section | |
| `consume` | Whether to consume messages on this connection | `true` |
| `confirm` | Enable publisher confirmations on this connection | `false` |

## Logging

Rejected uses `logging.config.dictConfig` to configure the Python standard
logging module. See the
[Python logging.config documentation](https://docs.python.org/3/library/logging.config.html#dictionary-schema-details)
for the full schema.

```yaml
Logging:
  version: 1
  formatters:
    verbose:
      format: '%(levelname) -10s %(asctime)s %(process)-6d %(processName) -15s %(name) -10s %(funcName) -20s: %(message)s'
      datefmt: '%Y-%m-%d %H:%M:%S'
  handlers:
    console:
      class: logging.StreamHandler
      formatter: verbose
  loggers:
    rejected:
      handlers: [console]
      level: INFO
      propagate: true
    myconsumer:
      handlers: [console]
      level: DEBUG
      propagate: true
  disable_existing_loggers: true
  incremental: false
```

!!! tip
    If your application is not logging anything, ensure that you have created a
    logger section for your consumer package. For example, if your consumer is
    `myconsumer.MyConsumer`, make sure there is a `myconsumer` logger entry.

### Correlation ID Logging

Rejected provides `rejected.log.CorrelationFilter` and
`rejected.log.CorrelationAdapter` for including the message correlation ID
in log output. Use filters to route log records with and without correlation
IDs to different formatters:

```yaml
Logging:
  version: 1
  formatters:
    verbose:
      format: '%(levelname) -10s %(asctime)s %(name)s: %(message)s'
    verbose_correlation:
      format: '%(levelname) -10s %(asctime)s %(name)s: %(message)s {CID %(correlation_id)s}'
  filters:
    correlation:
      '()': rejected.log.CorrelationFilter
      'exists': true
    no_correlation:
      '()': rejected.log.CorrelationFilter
      'exists': false
  handlers:
    console:
      class: logging.StreamHandler
      formatter: verbose
      filters: [no_correlation]
    console_correlation:
      class: logging.StreamHandler
      formatter: verbose_correlation
      filters: [correlation]
  loggers:
    rejected:
      handlers: [console, console_correlation]
      level: INFO
```

## Example Configuration

```yaml
%YAML 1.2
---
Application:
  poll_interval: 10.0
  # sentry_dsn: https://your-sentry-dsn
  stats:
    log: true
    prometheus:
      enabled: false
      port: 9090
    statsd:
      enabled: false
      host: localhost
      port: 8125
      prefix: application.rejected
  Connections:
    rabbitmq:
      host: localhost
      port: 5672
      user: guest
      pass: guest
      ssl: false
      vhost: /
      heartbeat_interval: 60
    rabbitmq2:
      host: localhost
      port: 5672
      user: rejected
      pass: rabbitmq
      ssl: false
      vhost: /
      heartbeat_interval: 60
  Consumers:
    async:
      consumer: mypackage.AsyncConsumer
      connections:
        - rabbitmq
        - name: rabbitmq2
          consume: false
          confirm: false
      qty: 1
      queue: test
      ack: true
      qos_prefetch: 100
      max_errors: 100

    sync:
      consumer: mypackage.SyncConsumer
      connections:
        - rabbitmq
      qty: 1
      queue: generated_messages
      ack: true
      max_errors: 100
      error_exchange: errors
      qos_prefetch: 1
      config:
        foo: true
        bar: baz

Logging:
  version: 1
  formatters:
    verbose:
      format: "%(levelname) -10s %(asctime)s %(process)-6d %(processName) -25s %(name) -20s %(funcName) -25s: %(message)s"
      datefmt: "%Y-%m-%d %H:%M:%S"
  handlers:
    console:
      class: logging.StreamHandler
      formatter: verbose
  loggers:
    rejected:
      level: INFO
      propagate: true
      handlers: [console]
    mypackage:
      level: DEBUG
      propagate: true
      handlers: [console]
  disable_existing_loggers: true
  incremental: false
```

## Command-Line Interface

```
usage: rejected [-h] -c FILE [-P DIR] [-o CONSUMER] [-p PATH] [-q N]
                [--version]

RabbitMQ consumer framework

options:
  -h, --help            show this help message and exit
  -c FILE, --config FILE
                        Path to the configuration file (YAML or TOML)
  -P DIR, --profile DIR
                        Profile consumer modules, writing output to DIR
  -o CONSUMER, --only CONSUMER
                        Only run the named consumer
  -p PATH, --prepend-path PATH
                        Prepend PATH to sys.path before importing consumers
  -q N, --qty N         Override the consumer quantity (use with -o)
  --version             show program's version number and exit
```

If you specify `-P /path/to/write/data/to`, rejected will automatically enable
`cProfile`, writing the profiling data to the specified path.

## Prometheus Metrics

When `stats.prometheus.enabled` is `true` and `rejected[prometheus]` is
installed, rejected exposes a `/metrics` HTTP endpoint on the configured port
for Prometheus to scrape.

### Built-in Counters

All labeled by `consumer` name.

| Metric | Description |
|--------|-------------|
| `rejected_messages_acked_total` | Messages acknowledged |
| `rejected_messages_dropped_total` | Messages dropped |
| `rejected_messages_failed_total` | Messages that resulted in errors |
| `rejected_messages_nacked_total` | Messages negatively acknowledged |
| `rejected_messages_processed_total` | Messages processed |
| `rejected_messages_redelivered_total` | Redelivered messages |
| `rejected_messages_requeued_total` | Messages requeued |
| `rejected_processing_seconds_total` | Total processing time in seconds |
| `rejected_exceptions_total` | Exceptions (labeled by `consumer` and `type`) |

Exception `type` values: `consumer_exception`, `message_exception`,
`processing_exception`, `unhandled_exception`.

### Built-in Histograms

All labeled by `consumer` name.

| Metric | Description |
|--------|-------------|
| `rejected_processing_duration_seconds` | Per-message processing duration |
| `rejected_message_age_seconds` | Age of messages at time of processing |

### Built-in Gauges

| Metric | Description |
|--------|-------------|
| `rejected_consumer_processes` | Number of active consumer processes |

### Custom Metrics

Metrics created via consumer stats methods are automatically forwarded to
Prometheus as dynamically created metrics:

| Consumer Method | Prometheus Type | Metric Name Pattern |
|----------------|----------------|---------------------|
| `stats_add_duration(key, value)` | Histogram | `rejected_custom_{key}_seconds` |
| `stats_track_duration(key)` | Histogram | `rejected_custom_{key}_seconds` |
| `stats_incr(key, value)` | Counter | `rejected_custom_{key}_total` |
| `stats_set_value(key, value)` | Gauge | `rejected_custom_{key}` |

Metric names are sanitized: any characters not in `[a-zA-Z0-9_]` are replaced
with underscores.
