# Rejected

Rejected is an AMQP consumer daemon and message processing framework. It allows
for rapid development of message processing consumers by handling all of the
core functionality of communicating with RabbitMQ and management of consumer
processes.

Rejected runs as a master process with multiple consumer configurations that are
each run in an isolated process. It has the ability to collect statistical
data from the consumer processes and report on it.

[![Version](https://img.shields.io/pypi/v/rejected.svg?)](https://pypi.python.org/pypi/rejected)
[![Coverage](https://img.shields.io/codecov/c/github/gmr/rejected.svg?)](https://codecov.io/github/gmr/rejected?branch=main)
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

    def process(self, message):
        LOGGER.debug('In Test.process: %s', message.body)
```

## Async Consumer

To make a consumer async, you can use Tornado's `@gen.coroutine` decorator on the
`Consumer.prepare` and `Consumer.process` methods. Asynchronous consumers allow
you to use async clients like Tornado's `AsyncHTTPClient` to perform parallel
tasks when processing a single message.

```python
import logging

from rejected import consumer

from tornado import gen
from tornado import httpclient


class AsyncExampleConsumer(consumer.Consumer):

    @gen.coroutine
    def process(self):
        LOGGER.debug('Message: %r', self.body)
        http_client = httpclient.AsyncHTTPClient()
        results = yield [http_client.fetch('http://www.github.com'),
                         http_client.fetch('http://www.reddit.com')]
        LOGGER.info('Length: %r', [len(r.body) for r in results])
```

## Version History

See [HISTORY.md](HISTORY.md) or the [documentation](https://rejected.readthedocs.io).
