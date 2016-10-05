import gc
import logging

LOGGER = logging.getLogger(__name__)


class GarbageCollectorMixin(object):
    """Consumer mixin to periodically call ``gc.collect`` periodically in the
    :meth:`on_finish` method.

    By default, ``gc.collect`` is invoked every 10,000 messages.

    To configure frequency of collection, include a ``gc_collection_frequency``
    setting in the consumer configuration.

    """
    DEFAULT_GC_FREQUENCY = 10000

    def __init__(self, *args, **kwargs):
        self._collection_cycle = \
            kwargs.get('settings', {}).get('gc_collection_frequency',
                                           self.DEFAULT_GC_FREQUENCY)
        super(GarbageCollectorMixin, self).__init__(*args, **kwargs)
        self._cycles_left = self.collection_cycle

    @property
    def collection_cycle(self):
        """Call :func:`gc.collect` every this many messages."""
        return self._collection_cycle

    @collection_cycle.setter
    def collection_cycle(self, value):
        """Set the number of messages to process before invoking ``gc.collect``

        :param int value: Cycle size

        """
        if value is not None:
            self._collection_cycle = value
            self._cycles_left = min(self._cycles_left, self._collection_cycle)

    def on_finish(self):
        """Used to initiate the garbage collection"""
        super(GarbageCollectorMixin, self).on_finish()
        self._cycles_left -= 1
        if self._cycles_left <= 0:
            num_collected = gc.collect()
            self._cycles_left = self.collection_cycle
            LOGGER.debug('garbage collection run, %d objects evicted',
                         num_collected)
