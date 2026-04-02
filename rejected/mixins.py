import gc
import logging
import typing

LOGGER = logging.getLogger(__name__)


class GarbageCollectorMixin:
    """Consumer mixin to periodically call ``gc.collect`` in the
    :meth:`on_finish` method.

    By default, ``gc.collect`` is invoked every 10,000 messages.

    To configure frequency of collection, include a
    ``gc_collection_frequency`` setting in the consumer configuration.

    """

    DEFAULT_GC_FREQUENCY: typing.ClassVar[int] = 10000

    def __init__(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        self._collection_cycle: int = kwargs.get('settings', {}).get(
            'gc_collection_frequency', self.DEFAULT_GC_FREQUENCY
        )
        super().__init__(*args, **kwargs)
        self._cycles_left: int = self._collection_cycle

    @property
    def collection_cycle(self) -> int:
        """Call :func:`gc.collect` every this many messages."""
        return self._collection_cycle

    @collection_cycle.setter
    def collection_cycle(self, value: int | None) -> None:
        """Set the number of messages to process before invoking
        ``gc.collect``.

        """
        if value is not None:
            self._collection_cycle = value
            self._cycles_left = min(
                self._cycles_left, self._collection_cycle
            )

    async def on_finish(self) -> None:
        """Used to initiate the garbage collection"""
        if hasattr(super(), 'on_finish'):
            await super().on_finish()  # type: ignore[misc]
        self._cycles_left -= 1
        if self._cycles_left <= 0:
            num_collected = gc.collect()
            self._cycles_left = self._collection_cycle
            LOGGER.debug(
                'garbage collection run, %d objects evicted',
                num_collected,
            )
