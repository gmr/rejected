# Configuration File Syntax

The rejected configuration uses [YAML](http://yaml.org) as the markup language.
YAML's format, like Python code, is whitespace dependent for control structure in
blocks. If you're having problems with your rejected configuration, the first
thing you should do is ensure that the YAML syntax is correct.

The configuration file is split into two main sections: `Application` and `Logging`.

## Application

The application section of the configuration is broken down into multiple top-level options:

| Option | Description |
|--------|-------------|
| `poll_interval` | How often rejected should poll consumer processes for status in seconds (int/float) |
| `sentry_dsn` | If Sentry support is installed, optionally set a global DSN for all consumers (str) |
| `stats` | Enable and configure metrics submission via statsd and/or Prometheus (obj) |
| `Connections` | A subsection with RabbitMQ connection information for consumers (obj) |
| `Consumers` | Where each consumer type is configured (obj) |

### stats

| Option | Description |
|--------|-------------|
| `log` | Toggle top-level logging of consumer process stats (bool) |
| `prometheus` | Configure the Prometheus metrics exporter (obj) |
| `statsd` | Configure the submission of per-message measurements to statsd (obj) |

#### prometheus

Requires `rejected[prometheus]` to be installed.

| Option | Description |
|--------|-------------|
| `enabled` | Toggle the Prometheus metrics HTTP endpoint on and off (bool) |
| `port` | The port to serve the `/metrics` endpoint on. Default: `9090` (int) |

See [Prometheus Metrics](#prometheus-metrics) below for the full list of exposed metrics.

#### statsd

| Option | Description |
|--------|-------------|
| `enabled` | Toggle statsd reporting off and on (bool) |
| `prefix` | An optional prefix to use when creating the statsd metric path (str) |
| `host` | The hostname or ip address of the statsd server (str) |
| `port` | The port of the statsd server. Default: `8125` (int) |
| `include_hostname` | Include the hostname in the measurement path. Default: `True` (bool) |
| `tcp` | Use TCP to connect to statsd (true/false). Default: `false` (str) |

### Connections

Each RabbitMQ connection entry should be a nested object with a unique name with connection attributes.

| Option | Description |
|--------|-------------|
| `host` | The hostname or ip address of the RabbitMQ server (str) |
| `port` | The port of the RabbitMQ server (int) |
| `vhost` | The virtual host to connect to (str) |
| `user` | The username to connect as (str) |
| `pass` | The password to use (str) |
| `ssl_options` | Optional: the SSL options for the SSL connection socket |
| `heartbeat_interval` | Optional: the AMQP heartbeat interval (int) default: 300 sec |

#### ssl_options

| Option | Description |
|--------|-------------|
| `ca_certs` | The file path to the concatenated list of CA certificates (str) |
| `ca_path` | The directory path to the PEM formatted CA certificates (str) |
| `ca_data` | The PEM encoded CA certificates (str) |
| `protocol` | The ssl `PROTOCOL_*` enum integer value. Default: `2` for `PROTOCOL_TLS` (int) |
| `certfile` | The file path to the PEM formatted certificate file (str) |
| `keyfile` | The file path to the certificate private key (str) |
| `password` | The password for decrypting the `keyfile` private key (str) |
| `ciphers` | The set of available ciphers in the OpenSSL cipher list format (str) |

### Consumers

Each consumer entry should be a nested object with a unique name with consumer attributes.

| Option | Description |
|--------|-------------|
| `consumer` | The `package.module.Class` path to the consumer code (str) |
| `connections` | The connections to connect to (list) |
| `qty` | The number of consumers per connection to run (int) |
| `queue` | The RabbitMQ queue name to consume from (str) |
| `ack` | Explicitly acknowledge messages (no_ack = not ack) (bool) |
| `max_errors` | Number of errors encountered before restarting a consumer (int) |
| `sentry_dsn` | If Sentry support is installed, set a consumer specific sentry DSN (str) |
| `drop_exchange` | The exchange to publish a message to when it is dropped (str) |
| `drop_invalid_messages` | Drop a message if the type property doesn't match the specified message type (str) |
| `message_type` | Used to validate the message type of a message before processing (str or list) |
| `error_exchange` | The exchange to publish messages that raise `ProcessingException` to (str) |
| `error_max_retry` | The number of `ProcessingException` raised on a message before dropping it (int) |
| `schema_uri_format` | Avro schema URI format with `{0}` placeholder for message type. Supports `file://` and `http(s)://` schemes. Requires `rejected[avro]` (str) |
| `config` | Free-form key-value configuration section for the consumer (obj) |

#### Consumer Connections

The consumer connections configuration allows for one or more connections to be
made by a single consumer. Connections can be specified as a simple list:

```yaml
Consumer Name:
    connections:
      - connection1
      - connection2
```

Or with structured values for finer control:

```yaml
Consumer Name:
    connections:
      - name: connection1
        consume: True
        publisher_confirmation: False
      - name: connection2
        consume: False
        publisher_confirmation: True
```

Structured connection options:

| Option | Description |
|--------|-------------|
| `name` | The connection name, as specified in the Connections section |
| `consume` | Specify if the connection should consume on the connection (bool) |
| `publisher_confirmation` | Enable publisher confirmations (bool) |

## Logging

Rejected uses `logging.config.dictConfig` to create a flexible method for
configuring the Python standard logging module.

```yaml
version: 1
formatters:
  verbose:
    format: '%(levelname) -10s %(asctime)s %(process)-6d %(processName) -15s %(name) -10s %(funcName) -20s: %(message)s'
    datefmt: '%Y-%m-%d %H:%M:%S'
handlers:
  console:
    class: logging.StreamHandler
    formatter: verbose
    debug_only: True
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

!!! note
    The `debug_only` node of the `Logging > handlers > console` section is not
    part of the standard dictConfig format. When set to `true` and the application
    is not running in the foreground, the handler and all references to it will be
    removed from the logging configuration.

### Troubleshooting

If your application is not logging anything, ensure that you have created a
logger section in your configuration for your consumer package. For example, if
your Consumer instance is named `myconsumer.MyConsumer`, make sure there is a
`myconsumer` logger in the logging configuration.

## Prometheus Metrics

When `stats.prometheus.enabled` is `true` and `rejected[prometheus]` is installed,
rejected exposes a `/metrics` HTTP endpoint on the configured port for Prometheus
to scrape.

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

Metrics created via `Consumer.stats_add_duration`, `Consumer.stats_incr`, and
`Consumer.stats_set_value` are automatically forwarded to Prometheus as
dynamically created metrics:

| Consumer Method | Prometheus Type | Metric Name Pattern |
|----------------|----------------|---------------------|
| `stats_add_duration(key, value)` | Histogram | `rejected_custom_{key}_seconds` |
| `stats_track_duration(key)` | Histogram | `rejected_custom_{key}_seconds` |
| `stats_incr(key, value)` | Counter | `rejected_custom_{key}_total` |
| `stats_set_value(key, value)` | Gauge | `rejected_custom_{key}` |

Metric names are sanitized: any characters not in `[a-zA-Z0-9_]` are replaced
with underscores.
