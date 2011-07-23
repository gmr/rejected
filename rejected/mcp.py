"""
mcp.py

"""
__author__ = 'Gavin M. Roy'
__email__ = 'gmr@myyearbook.com'
__since__ = '2011-07-22'

import logging
import threading
import time

from . import common
from . import consumer
from . import utils

# Max length of time to wait for shutdown to finish
_MAX_SHUTDOWN_WAIT = 30
_POLL_INTERVAL = 30

class MasterControlProgram(object):
    """
    Master Control Program keeps track of threads and threading needs

    """

    def __init__(self, config):
        """Initialize the Master Control Program

        :param config: The full content from the YAML config file
        :type config: dict

        """
        self._logger = logging.getLogger('rejected.mcp')

        # Default values
        self._consumers = dict()
        self._config = config

        # Carry for calculating messages per second
        self._monitoring = common.monitoring_enabled(config)

        # Setup the queue depth polling
        self._poll_interval = common.get_poll_interval(config) or _POLL_INTERVAL

    @property
    def _all_threads(self):
        """Return a list of all threads

        :returns: list

        """
        threads = list()
        for name in self._consumers:
            for thread in self._consumers[name]['threads']:
                threads.append(thread)
        return threads

    def _check_thresholds(self):
        pass

    def _poll(self):
        """Check the queue depths via passive Queue.Declare for each binding"""
        # Iterate through the consumers
        self._logger.debug('Active thread Count: %i', threading.active_count())
        self._logger.info('Polling consumers')
        results = dict()
        for name in self._consumers:
            thread = self._consumers[name]['threads'].pop(0)
            results[name] = thread.information
            self._logger.info('%s has %i threads and %i pending messages',
                              name, len(self._consumers[name]['threads']))


        return results

    def _sleep(self, duration):
        """Sleep function that should allow for shutdown during sleep.

        """
        if not duration:
            raise ValueError('No Duration Specified')

        self._logger.debug("Sleeping for %i seconds", duration)
        # Only sleep for 1 second so we can catch signals better
        for iteration in xrange(0, duration):
            if utils.RUNNING:
                time.sleep(1)


    def _start_new_thread(self, consumer_name, connection_name):

        # Create a new thread
        self._logger.info('Creating a new thread for %s connecting to %s',
                          consumer_name, connection_name)

        # Create the new thread
        thread = consumer.Consumer(self._config, consumer_name, connection_name)

        # Start the thread
        thread.start()

        self._logger.debug('After thread start')

        # Make sure it's alive before we return it
        if thread.is_alive():
            return thread

        # Thread had an error
        return None

    @property
    def _thread_count(self):
        """Returns the active thread count

        :returns: int
        """
        count = 0
        for thread in self._all_threads:
            if thread.is_alive():
                count += 1
        return count

    def run(self):
        """When the thread is ready to start running, kick off all of our
        consumer threads and then loop while we process messages.

        """
        self._logger.debug('Master Control Program Starting Up' )
        # Loop through all of the consumers
        consumers = common.get_consumer_config(self._config)
        for consumer_name in consumers:

            # Create the dictionary values for this consumer thread
            consumer_ = {'min_threads': consumers[consumer_name].get('min', 1),
                         'max_threads': consumers[consumer_name].get('max', 1),
                         'queue': consumers[consumer_name]['queue'],
                         'threads': list()}

            # Iterate through the connections to create new threads
            for connection_name in consumers[consumer_name]['connections']:
                self._logger.debug('Starting threads for %s on %s',
                                   consumer_name, connection_name)

                # Create the min amount of consumers per connection
                for thread_number in xrange(0, consumer_['min_threads']):

                    # Create the new thread
                    thread = self._start_new_thread(consumer_name,
                                                    connection_name)

                    # If the thread is alive, add it
                    if thread:
                        consumer_['threads'].append(thread)

            # Append this binding to our binding stack
            self._consumers[consumer_name] = consumer_

        # Loop
        while utils.RUNNING:

            # Check to see what our queue depths are
            self._check_thresholds()

            # Sleep
            self._sleep(self._poll_interval)

            # Poll
            self._poll()

        # Wait for shutdown to finish
        while self._thread_count:
            time.sleep(1)

    def shutdown(self):
        """Graceful shutdown of the MCP means shutting down threads too"""
        self._logger.info('Master Control Program Shutting Down')

        # Keep track of the fact we're shutting down
        shutdown_start = time.time()

        # Loop as long as we have running threads
        while self._thread_count:
            self._logger.debug('Trying to shutdown %i threads',
                               self._thread_count)

            # Loop through all of the bindings and try and shutdown threads
            for thread in self._all_threads:
                self._logger.info('Asking %s to shutdown', thread.name)
                thread.shutdown()

            if time.time() - shutdown_start > _MAX_SHUTDOWN_WAIT:
                self._logger.warn('Max shutdown exceeded, forcibly exiting')
                return

            # If we have any threads left, sleep a second before trying again
            time.sleep(1)
