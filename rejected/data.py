"""Rejected data objects.

The pydantic models (:class:`~rejected.models.Properties`,
:class:`~rejected.models.InternalMessage`, :class:`~rejected.models.Result`)
live in :mod:`rejected.models`. This module re-exports the result code
constants for backward compatibility and provides :class:`Measurement`.

"""

import collections
import contextlib
import time

from . import models

# Re-export result codes as module-level constants for backward compat
MESSAGE_ACK = models.Result.MESSAGE_ACK
MESSAGE_DROP = models.Result.MESSAGE_DROP
MESSAGE_REQUEUE = models.Result.MESSAGE_REQUEUE
CONSUMER_EXCEPTION = models.Result.CONSUMER_EXCEPTION
MESSAGE_EXCEPTION = models.Result.MESSAGE_EXCEPTION
PROCESSING_EXCEPTION = models.Result.PROCESSING_EXCEPTION
UNHANDLED_EXCEPTION = models.Result.UNHANDLED_EXCEPTION


def Message(  # noqa: N802
    connection, channel, method, properties, body, returned=False
):
    """Create an InternalMessage from pika callback arguments."""
    return models.InternalMessage.from_pika(
        connection, channel, method, properties, body, returned=returned
    )


def Properties(properties=None):  # noqa: N802
    """Create a Properties model from a pika BasicProperties."""
    return models.Properties.from_pika(properties)


class Measurement:
    """Per-message instrumentation collector.

    Accumulates counters, durations, tags, and values during message
    processing, then submitted to statsd and/or Prometheus by the
    :class:`~rejected.process.Process`.

    .. versionadded:: 3.13.0

    """

    def __init__(self):
        self.durations: dict[str, list[float]] = {}
        self.counters: collections.Counter = collections.Counter()
        self.tags: dict = {}
        self.values: dict = {}

    def decr(self, key: str, value: int = 1) -> None:
        """Decrement a counter.

        :param str key: The key to decrement
        :param int value: The value to decrement by

        """
        self.counters[key] -= value

    def incr(self, key: str, value: int = 1) -> None:
        """Increment a counter.

        :param str key: The key to increment
        :param int value: The value to increment by

        """
        self.counters[key] += value

    def add_duration(self, key: str, value: float) -> None:
        """Add a duration for the specified key.

        :param str key: The value name
        :param float value: The value

        .. versionadded:: 3.19.0

        """
        if key not in self.durations:
            self.durations[key] = []
        self.durations[key].append(value)

    def set_tag(self, key: str, value) -> None:
        """Set a tag for metrics submission.

        :param str key: The tag name
        :param value: The tag value
        :type value: str or bool or int

        """
        self.tags[key] = value

    def set_value(self, key: str, value) -> None:
        """Set a numeric value.

        :param str key: The value name
        :param value: The value
        :type value: int or float

        """
        self.values[key] = value

    @contextlib.contextmanager
    def track_duration(self, key: str):
        """Context manager that records the duration of the wrapped block.

        :param str key: The timing name

        """
        if key not in self.durations:
            self.durations[key] = []
        start_time = time.monotonic()
        try:
            yield
        finally:
            self.durations[key].append(time.monotonic() - start_time)
