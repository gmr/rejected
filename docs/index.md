# Rejected

Rejected is an AMQP consumer daemon and message processing framework. It allows
for rapid development of message processing consumers by handling all of the
core functionality of communicating with RabbitMQ and management of consumer
processes.

Rejected runs as a master process with multiple consumer configurations that are
each run in an isolated process. It has the ability to collect statistical
data from the consumer processes and report on it.

## Features

- Async consumers built on `asyncio`
- Automatic exception handling including connection management and consumer restarting
- Smart consumer classes that automatically decode and deserialize message bodies based on message headers
- Concurrent message processing with `FunctionalConsumer`
- Metrics via statsd and/or Prometheus
- Built-in profiling of consumer code
- Avro schema support with file and HTTP schema registries
- YAML and TOML configuration file support

## Installation

```bash
pip install rejected
```

For optional features:

```bash
pip install rejected[avro]        # Avro datum serialization
pip install rejected[html]        # HTML message body support
pip install rejected[msgpack]     # MessagePack support
pip install rejected[prometheus]  # Prometheus metrics exporter
pip install rejected[sentry]      # Sentry error reporting
```

## Quick Start

```python
import logging

import rejected

LOGGER = logging.getLogger(__name__)


class ExampleConsumer(rejected.Consumer):

    async def process(self):
        LOGGER.info(self.body)
```

All interaction with RabbitMQ -- connection management, message handling,
acknowledgements, and rejections -- is automatically handled for you.

## Issues

Please report any issues to the
[GitHub issue tracker](https://github.com/gmr/rejected/issues).

## Source

Rejected source is available on
[GitHub](https://github.com/gmr/rejected).
