"""
Master Control Program

"""
__author__ = 'Gavin M. Roy'
__email__ = 'gmr@myyearbook.com'
__since__ = '2011-07-22'

from tornado import ioloop
import logging
import threading
import time

from . import common
from . import consumer


class MasterControlProgram(object):
    """Master Control Program keeps track of consumers"""
    _MAX_SHUTDOWN_WAIT = 30
    _POLL_INTERVAL = 30
    _SHUTDOWN_WAIT = 1

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
        self._poll_data = {'time': 0, 'consumers': list()}
        self._last_poll_results = dict()
        self._stats = dict()

        # Carry for calculating messages per second
        self._monitoring = common.monitoring_enabled(config)

        # Setup the queue depth polling
        self._poll_interval = common.get_poll_interval(config) or \
                              MasterControlProgram._POLL_INTERVAL

        # Setup the poll timer
        self._logger.info('Setting poll interval to %i', self._poll_interval)
        self._poll_timer = threading.Timer(self._poll_interval, self._poll)

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
            if consumer_.state in [consumer.Consumer.INITIALIZING,
                                   consumer.Consumer.CONSUMING,
                                   consumer.Consumer.PROCESSING]:
                count += 1
        return count

    def run(self):
        """When the consumer is ready to start running, kick off all of our
        consumer consumers and then loop while we process messages.

        """
        self._logger.debug('Master Control Program Starting Up' )

        # Get the consumer section from the config
        consumers = common.get_consumer_config(self._config)

        # Loop through all of the consumers
        for name in consumers:

            # Create the dictionary values for this consumer consumer
            self._consumers[name] = {'min': consumers[name].get('min', 1),
                                     'max': consumers[name].get('max', 1),
                                     'queue': consumers[name]['queue'],
                                     'consumers': list()}

            # Iterate through the connections to create new consumers
            for connection_name in consumers[name]['connections']:

                self._logger.debug('Starting %i consumers for %s on %s',
                                   self._consumers[name]['min'],
                                   name, connection_name)

                # Create the min amount of consumers per connection
                for consumer_number in xrange(0, self._consumers[name]['min']):

                    # Create the new consumer
                    consumer_ = self._new_consumer(consumer_number,
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
        self._logger.info('Asking %i consumer(s) to stop', self._consumer_count)
        for consumer_ in self._all_consumers:
            if consumer_.state not in [consumer.Consumer.SHUTTING_DOWN,
                                       consumer.Consumer.STOPPED]:
                self._logger.debug('Asking %s to stop', consumer_.name)
                consumer_.stop()

        # Call this again in 5 seconds
        deadline = time.time() + MasterControlProgram._SHUTDOWN_WAIT
        self._logger.debug('Waiting %i seconds to check shutdown state',
                           MasterControlProgram._SHUTDOWN_WAIT)

        # Add the timeout
        self._ioloop.add_timeout(deadline, self._stop_consumers)

    def _loop_during_shutdown(self):
        """Recursively loop when we receive exceptions during shutdown."""
        try:
            self._ioloop.start()
        except KeyError as error:
            self._logger.warn('Received an error while shutting down: %s',
                              error)
            self._loop_during_shutdown()

    def shutdown(self):
        """Graceful shutdown of the MCP means shutting down consumers too"""
        self._logger.info('Master Control Program Shutting Down')

        # Keep track of the fact we're shutting down
        self._shutdown_start = time.time()

        # Stop the timer
        self._poll_timer.cancel()

        # Call the consumer stop method
        self._stop_consumers()

        # Keep the IOLoop running until we're done
        self._loop_during_shutdown()

        # Post IOLoop message
        self._logger.info("Exiting the Master Control Program")

    def _poll(self):
        """Start the poll process by invoking the get_stats method of the
        consumers. If we hit this after another interval without fully
        processing, note it with a warning.


        """
        # Check to see if we have outstanding things to poll
        if self._poll_data['consumers']:
            self._logger.warn('Poll interval failure for consumer(s): ',
                              ', '.join(self._poll_data['consumers']))

        # Keep track of running consumers
        non_active_consumers = list()

        # Start our data collection dict
        self._poll_data = {'timestamp': time.time(),
                           'consumers': list()}

        # Iterate through all of the consumers
        for consumer_ in self._all_consumers:
            self._logger.debug('Polling %s', consumer_.name)
            if consumer_.state == consumer.Consumer.STOPPED:
                self._logger.warn('Found stopped consumer %s', consumer_.name)
                non_active_consumers.append(consumer_.name)
            else:
                consumer_.get_processing_information(self._collect_results)
                self._poll_data['consumers'].append(consumer_.name)

        # Remove the objects if we have them
        self._prune_non_active_consumers(non_active_consumers)

        # If we don't have any active consumers, shutdown
        if not self._consumer_count:
            self._logger.info('Did not find any active consumers in poll')
            return self.shutdown()

        # Start the timer again
        self._poll_timer = threading.Timer(self._poll_interval, self._poll)
        self._poll_timer.start()

    def _prune_non_active_consumers(self, consumer_list):
        """Remove non-active consumers from the active stack

        :param consumer_list: List of consumers to remove
        :type consumer_list: list

        """
        for consumer_ in consumer_list:
            self._logger.debug('Pruning non-active consumer: %s', consumer_)
            if not self._remove_consumer(consumer_):
                self._logger.error('Could not find consumer %s to delete',
                                   consumer_)

    def _remove_consumer(self, consumer_name):
        """Remove the specified consumer name from the active consumer list.

        :param consumer_name: The consumer name to remove
        :type consumer_name: str

        """
        # Loop through all of the consumers
        for name in self._consumers:

            try:
                position = \
                    self._consumers[name]['consumers'].index(consumer_name)
            except ValueError:
                position = None

            # Check to see if we have a position from the index
            if position is not None:
                # Remove the item from the consumer list
                self._consumers[name]['consumers'].pop(position)
                return True

        # We did not find it
        return False

    def _collect_results(self, data_values):
        """Receive the data from the consumers we have polled and process it.

        :param data_values: The poll data returned from the consumer
        :type data_values: dict

        """
        self._logger.debug('Poll data received: %r', data_values)
        self._last_poll_results['timestamp'] = self._poll_data['timestamp']

        # Get the name and consumer name
        name = data_values['name']
        consumer_name = data_values['consumer_name']

        # Remove them from the data we report
        del data_values['name']
        del data_values['consumer_name']

        # Make sure we have the dict setup properly
        if consumer_name not in self._last_poll_results:
            self._last_poll_results[consumer_name] = dict()

        # Add it to our last poll global data
        self._last_poll_results[consumer_name][name] = data_values

        # Find the position to remove the consumer from the list
        try:
            position = self._poll_data['consumers'].index(name)
        except ValueError:
            self._logger.error('Poll data from non-tracked consumer: %s', name)
            position = None

        # Remove the consumer from the list we're waiting for data from
        if position is not None:
            self._poll_data['consumers'].pop(position)

        # Calculate global stats
        if not self._poll_data['consumers']:
            self._calculate_stats(self._last_poll_results)

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

            # Create a dict for our calculations
            stats[name] = {consumer.Consumer.TIME_SPENT: 0,
                           consumer.Consumer.PROCESSED: 0,
                           consumer.Consumer.ERROR: 0,
                           consumer.Consumer.REDELIVERED: 0}

            # Iterate through all of the data points
            for consumer_ in data[name]:

                # Get the counts so we can process them more quickly
                values = data[name][consumer_]['counts']

                # Add the values
                for key in stats[name]:
                    stats[name][key] += values[key]

        # Assign them to a dict so we can poll it from HTTP, etc
        self._stats = {'last_poll': timestamp,
                       'consumers': len(self._all_consumers),
                       'counts': stats}

        # Log it
        self._logger.info('Poll results: %r', self._stats)
