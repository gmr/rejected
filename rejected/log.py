"""Logging Related Things"""

from __future__ import annotations

import contextvars
import logging
import typing

# Per-task correlation id. Set for each message in _Consumer._pre_execute;
# because each message is processed in its own asyncio task, the value is
# isolated per task and does not clobber concurrent messages.
correlation_id: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    'correlation_id', default=None
)


class CorrelationFilter(logging.Formatter):
    """Filter records that have a correlation_id"""

    def __init__(self, exists: bool | None = None) -> None:
        super().__init__()
        self.exists = exists

    def filter(self, record: logging.LogRecord) -> bool:
        if self.exists:
            return hasattr(record, 'correlation_id')
        return not hasattr(record, 'correlation_id')


class CorrelationAdapter(logging.LoggerAdapter[logging.Logger]):
    """A LoggerAdapter that appends a correlation ID to the message
    record properties.

    """

    def __init__(
        self, logger: logging.Logger, consumer: typing.Any, **extra: typing.Any
    ) -> None:
        self.logger = logger
        self.consumer = consumer
        super().__init__(logger, extra)

    def process(
        self, msg: str, kwargs: typing.MutableMapping[str, typing.Any]
    ) -> tuple[str, typing.MutableMapping[str, typing.Any]]:
        kwargs['extra'] = {
            'correlation_id': correlation_id.get()
            or self.consumer.correlation_id,
            'consumer': self.consumer.name,
        }
        return msg, kwargs
