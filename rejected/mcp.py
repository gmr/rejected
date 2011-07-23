"""
Master Control Program

"""
__author__ = 'Gavin M. Roy'
__email__ = 'gmr@myyearbook.com'
__since__ = '2011-07-22'

from tornado import ioloop
import logging
import time

from . import common
from . import consumer


class MasterControlProgram(object):
    """Master Control Program keeps track of consumers"""
    _MAX_SHUTDOWN_WAIT = 30
    _POLL_INTERVAL = 30
    _SHUTDOWN_WAIT = 3

    def __init__(self, config):
        """Initialize the Master Control Program

        :param config: The full content from the YAML config file
        :type config: dict

        """
        self._logger = logging.getLogger('rejected.mcp')

        # Set the IO Loop handle
        self._ioloop = ioloop.IOLoop.instance()

        # Default values
        self._consumers = dict()
        self._config = config
        self._shutdown_start = 0

        # Carry for calculating messages per second
        self._monitoring = common.monitoring_enabled(config)

        # Setup the queue depth polling
        self._poll_interval = common.get_poll_interval(config) or \
                              MasterControlProgram._POLL_INTERVAL

    @property
    def _all_consumers(self):
        """Return a list of all consumers

        :returns: list

        """
        consumers = list()
        for name in self._consumers:
            for consumer_ in self._consumers[name]['consumers']:
                consumers.append(consumer_)
        return consumers

    def _new_consumer(self, consumer_number, consumer_name, connection_name):
        """Create a new consumer instances

        :param consumer_number: The identification number for the consumer
        :type consumer_number: int
        :param consumer_name: The name of the consumer
        :type consumer_name: str
        :param connection_name: The name of the connection
        :type connection_name

        """
        # Create a new consumer
        self._logger.info('Creating a new consumer: %s-%s-%i',
                          consumer_name, connection_name, consumer_number)

        # Create the new consumer
        return consumer.Consumer(self._config,
                                 consumer_number,
                                 consumer_name,
                                 connection_name)

    @property
    def _consumer_count(self):
        """Returns the active consumer count

        :returns: int
        """
        count = 0
        for consumer_ in self._all_consumers:
            if consumer_.state == consumer.Consumer.CONSUMING:
                count += 1
        return count

    def run(self):
        """When the consumer is ready to start running, kick off all of our
        consumer consumers and then loop while we process messages.

        """
        self._logger.debug('Master Control Program Starting Up' )
        consumers = common.get_consumer_config(self._config)
        # Loop through all of the consumers
        for name in consumers:

            # Create the dictionary values for this consumer consumer
            consumer_ = {'min_qty': consumers[name].get('min', 1),
                         'max_qty': consumers[name].get('max', 1),
                         'queue': consumers[name]['queue'],
                         'consumers': list()}

            # Iterate through the connections to create new consumers
            for connection_name in consumers[name]['connections']:
                self._logger.debug('Starting consumers for %s on %s',
                                   name, connection_name)

                # Create the min amount of consumers per connection
                for consumer_number in xrange(0, consumer_['min_qty']):

                    # Create the new consumer
                    consumer_ = self._new_consumer(consumer_number,
                                                   name,
                                                   connection_name)

                    # Append the consumer to the consumer list
                    self._consumers[name]['consumers'].append(consumer_)

        # Start the IO Loop
        self._ioloop.start()

    def _stop_consumers(self):
        """Iterate through all of the consumers shutting them down.
        """
        # See if we need to just stop
        if time.time() - self._shutdown_start > \
           MasterControlProgram._MAX_SHUTDOWN_WAIT:
            self._logger.warn('Max shutdown exceeded, forcibly exiting')
            self._ioloop.stop()
            return

        # Stop if we have no running consumers
        if not self._consumer_count:
            self._logger.info('All consumers have stopped, shutting down')
            self._ioloop.stop()
            return

        # Loop through all of the bindings and try and shutdown consumers
        self._logger.info('Asking %i consumers to stop', self._consumer_count)
        for consumer_ in self._all_consumers:
            if consumer_.state not in [consumer.Consumer.SHUTTING_DOWN,
                                       consumer.Consumer.STOPPED]:
                self._logger.info('Asking %s to stop', consumer_.name)
                consumer_.stop()

        # Call this again in 5 seconds
        self._logger.debug('Waiting %i seconds to check shutdown state',
                           MasterControlProgram._SHUTDOWN_WAIT)
        deadline = time.time() + MasterControlProgram._SHUTDOWN_WAIT
        self._ioloop.add_timeout(deadline, self._stop_consumers)

    def shutdown(self):
        """Graceful shutdown of the MCP means shutting down consumers too"""
        self._logger.info('Master Control Program Shutting Down')

        # Keep track of the fact we're shutting down
        self._shutdown_start = time.time()

        # Call the consumer stop method
        self._stop_consumers()
