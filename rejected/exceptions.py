"""Rejected Exceptions"""


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

    def __init__(self, *args, **kwargs):
        if len(args) > 1:
            self.args = args[1:] if 'value' not in kwargs else args
        else:
            self.args = args
        self.metric = kwargs.pop('metric', None)
        self.value = kwargs.pop('value', '{!r} {!r}' if not args else args[0])
        self.kwargs = kwargs

    def __str__(self):
        if not self.args and not self.kwargs:
            return repr(self)
        return self.value.format(*self.args, **self.kwargs)

    def __repr__(self):
        if not self.args and not self.kwargs:
            return f'{self.__class__.__name__}()'
        return f'{self.__class__.__name__}({self!s})'


class ConsumerException(RejectedException):
    """May be called when processing a message to indicate a problem that the
    Consumer may be experiencing that should cause it to stop.

    :param str value: An optional value used in string representation
    :param str metric: An optional value for auto-instrumentation of exceptions

    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class MessageException(RejectedException):
    """Invoke when a message should be rejected and not re-queued, but not due
    to a processing error that should cause the consumer to stop.

    :param str value: An optional value used in string representation
    :param str metric: An optional value for auto-instrumentation of exceptions

    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class ProcessingException(RejectedException):
    """Invoke when a message should be rejected and not re-queued, but not due
    to a processing error that should cause the consumer to stop. This should
    be used for when you want to reject a message which will be republished to
    a retry queue, without anything being stated about the exception.

    :param str value: An optional value used in string representation
    :param str metric: An optional value for auto-instrumentation of exceptions

    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
