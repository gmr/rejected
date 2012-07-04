"""
Master Control Program

"""
import collections
from tornado import ioloop
import logging
import os
import threading
import time

from rejected import consumer

logger = logging.getLogger(__name__)


class MasterControlProgram(object):
    """Master Control Program keeps track of consumers"""
    _MIN_CONSUMERS = 1
    _MAX_CONSUMERS = 2
    _MAX_SHUTDOWN_WAIT = 30
    _POLL_INTERVAL = 30
    _SHUTDOWN_WAIT = 1

    def __init__(self, config):
        """Initialize the Master Control Program

        :param config: The full content from the YAML config file
        :type config: dict

        """
        # Set the IO Loop handle
        self._ioloop = ioloop.IOLoop.instance()

        # Default values
        self._consumers = dict()
        self._config = config
        self._shutdown_start = 0
        self._poll_data = {'time': 0, 'consumers': list()}
        self._last_poll_results = dict()
        self._stats = dict()
        self._timeouts = list()

        # Carry for calculating messages per second
        self._monitoring = config.get('monitor', False)
        logger.debug('Monitoring enabled: %s', self._monitoring)
        self._stats_logging = self._monitoring and config.get('log_stats', True)

        # Setup the queue depth polling
        self._poll_interval = config.get('poll_interval', self._POLL_INTERVAL)

        # Setup the poll timer
        logger.info('Setting poll interval to %i', self._poll_interval)
        self._poll_timer = threading.Timer(self._poll_interval, self._poll)

    def _add_shutdown_timer(self):
        """Add a timer to the IOLoop if the IOLoop is running to shut down in
        self._SHUTDOWN_WAIT seconds.

        """
        if not self._ioloop.running:
            logger.info('Shutdown timer was going to be added but the IOLoop '
                        'has stopped')
            return

        deadline = time.time() + self._SHUTDOWN_WAIT
        logger.debug('Waiting %i seconds to check shutdown state',
                     self._SHUTDOWN_WAIT)
        self._timeouts.append(self._ioloop.add_timeout(deadline,
                                                       self._stop_consumers))

    @property
    def _all_consumers(self):
        """Return a list of all consumers

        :rtype: list

        """
        consumers = list()
        for name in self._consumers:
            for consumer_ in self._consumers[name]['consumers']:
                consumers.append(consumer_)
        return consumers

    def _consumer_stats_counter(self):
        """Return a new consumer stats counter instance.

        :rtype: collections.Counter

        """
        return collections.Counter({consumer.RejectedConsumer.ERROR: 0,
                                    consumer.RejectedConsumer.PROCESSED: 0,
                                    consumer.RejectedConsumer.REDELIVERED: 0,
                                    consumer.RejectedConsumer.TIME_SPENT: 0,
                                    consumer.RejectedConsumer.TIME_WAITED: 0})

    def _calculate_stats(self, data):
        """Calculate the stats data for our process level data.

        :param data: The collected stats data to report on
        :type data: dict

        """
        stats = {}
        timestamp = data['timestamp']
        del data['timestamp']

        # Iterate through the last poll results
        for name in data:
            stats = self._consumer_stats_counter()
            for key in stats:
                stats[key] += data[name]['counts'][key]
        self._stats = {'last_poll': timestamp,
                       'consumers': len(self._all_consumers),
                       'counts': stats}

    def _collect_results(self, data_values):
        """Receive the data from the consumers polled and process it.

        :param dict data_values: The poll data returned from the consumer
        :type data_values: dict

        """
        self._last_poll_results['timestamp'] = self._poll_data['timestamp']

        # Get the name and consumer name and remove it from what is reported
        name = data_values['name']
        del data_values['name']

        # Make sure we have the dict setup properly
        if name not in self._last_poll_results:
            self._last_poll_results[name] = dict()

        # Add it to our last poll global data
        self._last_poll_results[name] = data_values

        # Find the position to remove the consumer from the list
        try:
            position = self._poll_data['consumers'].index(name)
        except ValueError:
            logger.error('Poll data from non-tracked consumer: %s', name)
            position = None

        # Remove the consumer from the list we're waiting for data from
        if position is not None:
            self._poll_data['consumers'].pop(position)

        # Calculate global stats
        if not self._poll_data['consumers']:
            self._calculate_stats(self._last_poll_results)
            if self._stats_logging:
                self._log_stats()

    @property
    def _consumer_count(self):
        """Returns the active consumer count

        :rtype: int
        """
        count = 0
        for consumer_ in self._all_consumers:
            if consumer_.state in [consumer.RejectedConsumer.STATE_INITIALIZING,
                                   consumer.RejectedConsumer.STATE_IDLE,
                                   consumer.RejectedConsumer.STATE_PROCESSING]:
                count += 1
        return count

    def _create_consumer(self, consumer_number, consumer_name, connection_name):
        """Create a new consumer instances

        :param int consumer_number: The identification number for the consumer
        :param str consumer_name: The name of the consumer
        :param str connection_name: The name of the connection

        """
        # Create a new consumer
        logger.info('Creating a new consumer for %s: %s_%i_tag_%i',
                    connection_name, consumer_name,
                    os.getpid(), consumer_number)

        # Create the new consumer
        return consumer.RejectedConsumer(self._config,
                                         consumer_number,
                                         consumer_name,
                                         connection_name)

    def _handle_keyboard_interrupt_shutdowns(self):
        """Recursively loop when we receive exceptions during shutdown."""
        try:
            self._ioloop.start()
        except KeyError as error:
            logger.warn('Received an error while shutting down: %s', error)
            self._stop_consumers()
            self._handle_keyboard_interrupt_shutdowns()

    def _log_stats(self):
        """Output the stats to the logger."""
        logger.info('rejected %i has %i consumers and has processed %i '
                    'messages with %i errors, waiting %.2f seconds and '
                    'spent %.2f seconds processing',
                    os.getpid(),
                    self._stats['consumers'],
                    self._stats['counts']['processed'],
                    self._stats['counts']['failed'],
                    self._stats['counts']['waiting_time'],
                    self._stats['counts']['processing_time'])

    @property
    def _max_shutdown_wait_exceeded(self):
        return time.time() - self._shutdown_start > self._MAX_SHUTDOWN_WAIT

    def _stop_consumers(self):
        """Iterate through all of the consumers shutting them down."""
        self._remove_timeouts()

        # See if we need to just stop
        if self._max_shutdown_wait_exceeded:
            logger.critical('Max shutdown exceeded, forcibly exiting')
            self._ioloop.stop()
            return

        # Stop if we have no running consumers
        if not self._consumer_count:
            logger.info('All consumers have stopped, shutting down')
            self._ioloop.stop()
            return

        # Loop through all of the bindings and try and shutdown consumers
        logger.info('Asking %i consumer(s) to stop', self._consumer_count)
        for consumer_ in self._all_consumers:
            if consumer_.is_running:
                logger.debug('Asking %s to stop', consumer_.name)
                consumer_.stop()

        # Add a shutdown timer to call this method again in n seconds
        self._add_shutdown_timer()

    def _poll(self):
        """Start the poll process by invoking the get_stats method of the
        consumers. If we hit this after another interval without fully
        processing, note it with a warning.


        """
        # Check to see if we have outstanding things to poll
        if self._poll_data['consumers']:
            logger.warn('Poll interval failure for consumer(s): %r',
                              ', '.join(self._poll_data['consumers']))

        # Keep track of running consumers
        non_active_consumers = list()

        # Start our data collection dict
        self._poll_data = {'timestamp': time.time(),
                           'consumers': list()}

        # Iterate through all of the consumers
        for consumer_ in self._all_consumers:
            logger.debug('Checking runtime state of %s', consumer_.name)
            if not consumer_.is_running:
                logger.warn('Found dead consumer %s', consumer_.name)
                non_active_consumers.append(consumer_.name)
            else:
                if self._monitoring:
                    consumer_.stats(self._collect_results)
                    self._poll_data['consumers'].append(consumer_.name)

        # Remove the objects if we have them
        self._prune_non_active_consumers(non_active_consumers)

        # If we don't have any active consumers, shutdown
        if not self._consumer_count:
            logger.info('Did not find any active consumers in poll')
            return self.shutdown()

        # Start the timer again
        self._poll_timer = threading.Timer(self._poll_interval, self._poll)
        self._poll_timer.start()

    def _prune_non_active_consumers(self, consumer_list):
        """Remove non-active consumers from the active stack

        :param list consumer_list: List of consumers to remove

        """
        for consumer_ in consumer_list:
            logger.debug('Pruning non-active consumer: %s', consumer_)
            if not self._remove_consumer(consumer_):
                logger.error('Could not find consumer %s to delete', consumer_)

    def _remove_consumer(self, consumer_name):
        """Remove the specified consumer name from the active consumer list.

        :param str consumer_name: The consumer name to remove
        :rtype: bool

        """
        # Loop through all of the consumers
        for name in self._consumers:
            try:
                offset = self._consumers[name]['consumers'].index(consumer_name)
            except ValueError:
                offset = None

            # Check to see if we have a position from the index
            if offset is not None:
                # Remove the item from the consumer list
                self._consumers[name]['consumers'].pop(offset)
                return True

        return False

    def _remove_timeouts(self):
        """Stop all the active timeouts."""
        # Stop the poll timer if it's running
        if self._poll_timer and self._poll_timer.is_alive:
            logger.debug('Stopping the poll timer')
            self._poll_timer.cancel()

        for timeout in self._timeouts:
            logger.debug('Removing a timeout: %r', timeout)
            self._ioloop.remove_timeout(timeout)

    def run(self):
        """When the consumer is ready to start running, kick off all of our
        consumer consumers and then loop while we process messages.

        """
        logger.debug('Master Control Program Initialized' )

        # Get the consumer section from the config
        if 'Consumers' not in self._config:
            logger.error('Missing Consumers section of configuration, '
                         'aborting: %r', self._config)
            return

        consumers = self._config['Consumers']

        # Loop through all of the consumers
        for name in consumers:

            if 'queue' not in consumers[name]:
                logger.error('Consumer %s is missing a queue, skipping', name)
                continue

            # Create the dictionary values for this consumer consumer
            consumer = consumers[name]
            self._consumers[name] = {'min': consumer.get('min',
                                                         self._MIN_CONSUMERS),
                                     'max': consumer.get('max',
                                                         self._MAX_CONSUMERS),
                                     'queue': consumer['queue'],
                                     'consumers': list()}

            # Iterate through the connections to create new consumers
            for connection_name in consumer['connections']:

                logger.debug('Starting %i consumer(s) for %s on %s',
                             self._consumers[name]['min'],
                             name, connection_name)

                # Create the min amount of consumers per connection
                for consumer_number in xrange(0, self._consumers[name]['min']):

                    # Create the new consumer
                    consumer_ = self._create_consumer(consumer_number,
                                                      name,
                                                      connection_name)

                    # Append the consumer to the consumer list
                    self._consumers[name]['consumers'].append(consumer_)

        # Kick off our poll timer
        self._poll_timer.start()

        # Start the IO Loop
        self._ioloop.start()

        # Stop the poll timer
        self._poll_timer.cancel()

    def shutdown(self):
        """Graceful shutdown of the MCP means shutting down consumers too"""
        logger.info('Master Control Program Shutting Down')

        # Keep track of the fact we're shutting down
        self._shutdown_start = time.time()

        # Call the consumer stop method
        self._stop_consumers()

        # Keep the IOLoop running until we're done
        self._handle_keyboard_interrupt_shutdowns()

        # Post IOLoop message
        logger.info("Exiting the Master Control Program")

