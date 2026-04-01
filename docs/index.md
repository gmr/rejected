# Rejected

Rejected is an AMQP consumer daemon and message processing framework. It allows
for rapid development of message processing consumers by handling all of the
core functionality of communicating with RabbitMQ and management of consumer
processes.

Rejected runs as a master process with multiple consumer configurations that are
each run in an isolated process. It has the ability to collect statistical
data from the consumer processes and report on it.

## Features

- Automatic exception handling including connection management and consumer restarting
- Smart consumer classes that can automatically decode and deserialize message bodies based upon message headers
- Metrics logging and submission to statsd and InfluxDB
- Built-in profiling of consumer code
- Ability to write asynchronous code in consumers allowing for parallel communication with external resources

## Installation

```bash
pip install rejected
```

For optional features:

```bash
pip install rejected[html]     # HTML message body support
pip install rejected[msgpack]  # MessagePack support
```

## Quick Start

```python
from rejected import consumer
import logging

LOGGER = logging.getLogger(__name__)


class ExampleConsumer(consumer.Consumer):

    def process(self):
        LOGGER.info(self.body)
```

All interaction with RabbitMQ — connection management, message handling,
acknowledgements, and rejections — is automatically handled for you.

## Issues

Please report any issues to the
[GitHub issue tracker](https://github.com/gmr/rejected/issues).

## Source

Rejected source is available on
[GitHub](https://github.com/gmr/rejected).
