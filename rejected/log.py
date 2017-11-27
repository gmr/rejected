"""
Logging Related Things

"""
import logging
import warnings


class CorrelationFilter(logging.Filter):
    """Filter records based upon the presence of a ``correlation_id`` attribute
    and the ``exists`` setting.

    .. deprecated:: 4.0.0

    """
    def __init__(self, name='', exists=True):
        """Returns an instance of the Filter class. If ``name`` is specified,
        it  names a logger which, together with its children, will have its
        events  allowed through the filter. If name is the empty string, allows
        every event.

        The ``exists`` attribute is used to toggle the inclusion or exclusion
        of records filtered based upon the existence of a ``correlation_id``
        attribute.

        :param str name: The logger name to apply the filter to
        :param bool exists: When true, filters records out if a
            ``correlation_id`` is not present.

        """
        super(CorrelationFilter, self).__init__(name)
        self._exists = exists
        warnings.warn('Deprecated; use CorrelationIDFilter and '
                      'NoCorrelationID filter instead', DeprecationWarning)

    def filter(self, record):
        """Is the specified record to be logged? Returns zero for no,
        nonzero for yes. If deemed appropriate, the record may be modified
        in-place by this method.

        :param logging.LogRecord record: The log record to process
        :rtype: int

        """
        if self._exists:
            return int(getattr(record, 'correlation_id', None) is not None)
        return int(getattr(record, 'correlation_id', None) is None)


class CorrelationIDFilter(logging.Filter):
    """Filter records that have a correlation_id

    .. versionadded:: 4.0.0

    """
    def filter(self, record):
        """Is the specified record to be logged? Returns zero for no,
        nonzero for yes. If deemed appropriate, the record may be modified
        in-place by this method.

        :param logging.LogRecord record: The log record to process
        :rtype: int

        """
        return int(getattr(record, 'correlation_id', None) is not None)


class NoCorrelationIDFilter(logging.Filter):
    """Filter records that do not have a correlation_id

    .. versionadded:: 4.0.0

    """
    def filter(self, record):
        """Is the specified record to be logged? Returns zero for no,
        nonzero for yes. If deemed appropriate, the record may be modified
        in-place by this method.

        :param logging.LogRecord record: The log record to process
        :rtype: int

        """
        return int(getattr(record, 'correlation_id', None) is None)


class CorrelationIDAdapter(logging.LoggerAdapter):
    """A LoggerAdapter that appends the a correlation ID to the message
    record properties.

    .. versionadded:: 4.0.0

    """
    def __init__(self, logger, extra):
        """Create a new instance of the CorrelationIDAdapter class.

        :param logging.Logger logger: The logger to adapt
        :param dict extra: Configuration values to pass in

        """
        super(CorrelationIDAdapter, self).__init__(logger, extra)
        self.logger = logger
        self.parent = extra['parent']

    def process(self, msg, kwargs):
        """Process the logging message and keyword arguments passed in to
        a logging call to insert contextual information.

        :param str msg: The message to process
        :param dict kwargs: The kwargs to append
        :rtype: (str, dict)

        """
        kwargs['extra'] = {
            'correlation_id': self.parent.correlation_id,
            'parent': self.parent.name
        }
        return msg, kwargs


class CorrelationAdapter(CorrelationIDAdapter):
    """A LoggerAdapter that appends the a correlation ID to the message
    record properties.

    .. deprecated:: 4.0.0

    """
    def __init__(self, logger, extra):
        """Create a new instance of the CorrelationAdapter class.

        :param logging.Logger logger: The logger to adapt
        :param dict extra: Configuration values to pass in

        """
        warnings.warn(
            'Deprecated; use CorrelationIDAdapter', DeprecationWarning)
        super(CorrelationAdapter, self).__init__(logger, extra)
