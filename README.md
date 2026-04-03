# Rejected

Rejected is an AMQP consumer daemon and message processing framework. It allows
for rapid development of message processing consumers by handling all of the
core functionality of communicating with RabbitMQ and management of consumer
processes.

Rejected runs as a master process with multiple consumer configurations that are
each run in an isolated process. It has the ability to collect statistical
data from the consumer processes and report on it.

[![Version](https://img.shields.io/pypi/v/rejected.svg?)](https://pypi.python.org/pypi/rejected)
[![Python](https://img.shields.io/pypi/pyversions/rejected.svg)](https://pypi.python.org/pypi/rejected)
[![License](https://img.shields.io/pypi/l/rejected.svg?)](https://github.com/gmr/rejected/blob/main/LICENSE)

## Features

- Async consumers built on `asyncio`
- Automatic exception handling including connection management and consumer restarting
- Smart consumer classes that automatically decode and deserialize message bodies based on message headers
- Concurrent message processing with `TransactionConsumer`
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

## Documentation

Full documentation is available at [https://rejected.readthedocs.io](https://rejected.readthedocs.io).

## Example Consumer

```python
from rejected import consumer
import logging

LOGGER = logging.getLogger(__name__)


class Test(consumer.Consumer):

    async def process(self) -> None:
        LOGGER.debug('In Test.process: %s', self.body)
```

For concurrent message processing, use `TransactionConsumer`:

```python
from rejected import consumer, models
import logging

LOGGER = logging.getLogger(__name__)


class Test(consumer.TransactionConsumer):

    async def process(self, ctx: models.ProcessingContext) -> None:
        LOGGER.debug('Processing: %s', ctx.message.body)
```
