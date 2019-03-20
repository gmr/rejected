"""
Logging Related Things

"""
import logging


class CorrelationFilter(logging.Formatter):
    """Filter records that have a correlation_id"""
    def __init__(self, exists=None):
        super(CorrelationFilter, self).__init__()
        self.exists = exists

    def filter(self, record):
        """Filter returns based upon the combination of self.exists and
        the presence of the correlation_id record attribute.

        :param logging.LogRecord record: The logging record
        :rtype: bool

        """
        if self.exists:
            return hasattr(record, 'correlation_id')
        return not hasattr(record, 'correlation_id')


class CorrelationAdapter(logging.LoggerAdapter):
    """A LoggerAdapter that appends the a correlation ID to the message
    record properties.

    """
    def __init__(self, logger, consumer, **extra):
        self.logger = logger
        self.consumer = consumer
        super(CorrelationAdapter, self).__init__(logger, extra)

    def process(self, msg, kwargs):
        """Process the logging message and keyword arguments passed in to
        a logging call to insert contextual information.

        :param str msg: The message to process
        :param dict kwargs: The kwargs to append
        :rtype: (str, dict)

        """
        kwargs['extra'] = {'correlation_id': self.consumer.correlation_id,
                           'consumer': self.consumer.name}
        return msg, kwargs
