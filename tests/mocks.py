import mock
from pika import spec
import time

__version__ = '99.99.99'

CHANNEL = mock.Mock('pika.channel.Channel')
METHOD = spec.Basic.Deliver('ctag0', 1, False, 'exchange', 'routing_key')
PROPERTIES = spec.BasicProperties(content_type='application/json',
                                  content_encoding='qux',
                                  headers={'foo': 'bar', 'baz': 1},
                                  delivery_mode=2,
                                  priority=5,
                                  correlation_id='c123',
                                  reply_to='rtrk',
                                  expiration='32768',
                                  message_id='mid123',
                                  timestamp=time.time(),
                                  type='test',
                                  user_id='foo',
                                  app_id='bar')
BODY = '{"qux": true, "foo": "bar", "baz": 1}'


class MockConsumer(object):

    def __init__(self, configuration, process):
        """Creates a new instance of a Mock Consumer class. To perform
        initialization tasks, extend Consumer._initialize
        :param dict configuration: The configuration from rejected
        """
        # Carry the configuration for use elsewhere
        self._configuration = configuration
        self._process = process
