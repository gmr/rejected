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

    .. versionadded:: 3.19.0

    :param str value: An optional value used in string representation
    :param str metric: An optional value for auto-instrumentation of exceptions

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


class ConfigurationException(RejectedException):
    """Raised when :py:meth:`~rejected.consumer.Consumer.require_setting` is
    invoked and the specified setting was not configured. When raised, the
    consumer will shutdown.

    .. versionadded:: 4.0.0

    """


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


class ConsumerException(RejectedException):
    """May be called when processing a message to indicate a problem that the
    Consumer may be experiencing that should cause it to stop.

    .. versionchanged:: 3.19.0

    :param str value: An optional value used in string representation
    :param str metric: An optional value for auto-instrumentation of exceptions

    """
    def __init__(self, value=None, metric=None, *args, **kwargs):
        super(ConsumerException, self).__init__(value, metric, *args, **kwargs)


class MessageException(RejectedException):
    """Invoke when a message should be rejected and not re-queued, but not due
    to a processing error that should cause the consumer to stop.

    .. versionchanged:: 3.19.0

    :param str value: An optional value used in string representation
    :param str metric: An optional value for auto-instrumentation of exceptions

    """
    def __init__(self, value=None, metric=None, *args, **kwargs):
        super(MessageException, self).__init__(value, metric, *args, **kwargs)


class ProcessingException(RejectedException):
    """Invoke when a message should be rejected and not re-queued, but not due
    to a processing error that should cause the consumer to stop. This should
    be used for when you want to reject a message which will be republished to
    a retry queue, without anything being stated about the exception.

    .. versionchanged:: 3.19.0

    :param str value: An optional value used in string representation
    :param str metric: An optional value for auto-instrumentation of exceptions

    """
    def __init__(self, value=None, metric=None, *args, **kwargs):
        super(ProcessingException, self).__init__(
            value, metric, *args, **kwargs)


class DropMessage(Exception):
    """Invoked during :meth:`rejected.consumer.Consumer._preprocessing`
    if the message needs to be dropped.

    .. versionadded:: 4.0.0

    """


class ExecutionFinished(Exception):
    """Invoked by :meth:`rejected.consumer.Consumer.finish` to abort further
    processing of a message.

    .. versionadded:: 4.0.0

    """
