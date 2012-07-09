__author__ = 'gmr'

class MockConsumer(object):

    def __init__(self, configuration):
        """Creates a new instance of a Mock Consumer class. To perform
        initialization tasks, extend Consumer._initialize

        :param dict configuration: The configuration from rejected

        """
        # Carry the configuration for use elsewhere
        self._configuration = configuration



class MockHeader(object):

    ATTRIBUTES = {'content_type': 'text/html',
                  'content_encoding': 'gzip',
                  'headers': {'foo': 'bar', 'boolean': True},
                  'delivery_mode': 1,
                  'priority': 0,
                  'correlation_id': 'abc123',
                  'reply_to': 'def456',
                  'expiration': 1341855318,
                  'message_id': 'ghi789',
                  'timestamp': 1341855300,
                  'type': 'test_message',
                  'user_id': 'nose',
                  'app_id': 'nosetests',
                  'cluster_id': 'mock'}

    def __init__(self):
        for attribute in self.ATTRIBUTES:
            setattr(self, attribute, self.ATTRIBUTES[attribute])


class MockMethod(object):

    consumer_tag = 'tag0'
    delivery_tag = 10
    exchange = 'unittests'
    redelivered = False
    routing_key = 'mock_tests'
