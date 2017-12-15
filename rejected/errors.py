"""
Common Exceptions
=================

"""


class RejectedException(Exception):
    """Base exception for :py:class:`~rejected.consumer.Consumer` related
    exceptions.

    If provided, the metric will be used to automatically record exception
    metric counts using the path
    `[prefix].[consumer-name].exceptions.[exception-type].[metric]`.

    Positional and keyword arguments are used to format the value that is
    passed in when providing the string value of the exception.

    :param str value: An optional value used in string representation
    :param str metric: An optional value for auto-instrumentation of exceptions

    .. versionadded:: 3.19.0

    """
    METRIC_NAME = 'rejected-exception'

    def __init__(self, value=None, metric=None, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.metric = metric
        self.value = value or ''

    def __str__(self):
        return self.value.format(*self.args, **self.kwargs)

    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, str(self))


class RabbitMQException(RejectedException):
    """Raised when an error sent from RabbitMQ, closing the channel. When
    the channel is closed, no more AMQP operations may be performed by the
    consumer for the current message. In addition, if the connection is
    configured with ``no_ack`` set to ``False``, the message is returned to
    the queue.

    .. versionadded:: 4.0.0

    """
    def __init__(self, connection, error_code, error_text, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.connection = connection
        self.error_code = error_code
        self.error_text = error_text

    def __str__(self):
        return '{} Error: ({}) {}'.format(
            self.connection, self.error_code, self.error_text)

    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, str(self))
