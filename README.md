# Rejected

Rejected is an AMQP consumer daemon and message processing framework. It allows
for rapid development of message processing consumers by handling all of the
core functionality of communicating with RabbitMQ and management of consumer
processes.

Rejected runs as a master process with multiple consumer configurations that are
each run in an isolated process. It has the ability to collect statistical
data from the consumer processes and report on it.

[![Version](https://img.shields.io/pypi/v/rejected.svg?)](https://pypi.python.org/pypi/rejected)
[![License](https://img.shields.io/pypi/l/rejected.svg?)](https://github.com/gmr/rejected/blob/main/LICENSE)

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
pip install rejected[avro]     # Avro support
pip install rejected[html]     # HTML message body support
pip install rejected[msgpack]  # MessagePack support
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
