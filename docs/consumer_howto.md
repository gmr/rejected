# Consumer Examples

The following example illustrates a very simple consumer that simply logs each
message body as it's received.

```python
from rejected import consumer
import logging

__version__ = '1.0.0'

LOGGER = logging.getLogger(__name__)


class ExampleConsumer(consumer.Consumer):

    def process(self):
        LOGGER.info(self.body)
```

All interaction with RabbitMQ with regard to connection management and message
handling, including acknowledgements and rejections are automatically handled
for you.

The `__version__` variable provides context in the rejected log files when
consumers are started and can be useful for investigating consumer behaviors in
production.

In this next example, a contrived `ExampleConsumer._connect_to_database` method
is added that will return `False`. When `ExampleConsumer.process` evaluates
if it could connect to the database and finds it can not, it will raise a
`rejected.consumer.ConsumerException` which will requeue the message
in RabbitMQ and increment an error counter. When too many errors occur, rejected
will automatically restart the consumer after a brief quiet period.

```python
from rejected import consumer
import logging

__version__ = '1.0.0'

LOGGER = logging.getLogger(__name__)


class ExampleConsumer(consumer.Consumer):

    def _connect_to_database(self):
        return False

    def process(self):
        if not self._connect_to_database:
            raise consumer.ConsumerException('Database error')

        LOGGER.info(self.body)
```

Some consumers are also publishers. In this next example, the message body will
be republished to a new exchange on the same RabbitMQ connection:

```python
from rejected import consumer
import logging

__version__ = '1.0.0'

LOGGER = logging.getLogger(__name__)


class ExampleConsumer(consumer.Consumer):

    def process(self):
        LOGGER.info(self.body)
        self.publish_message('new-exchange', 'routing-key', {}, self.body)
```

Note that `consumer.Consumer` supports publishing directly — there is no need
to extend a separate `PublishingConsumer` class.
