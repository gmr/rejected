"""
Master Control Program

"""
import logging
import multiprocessing
import os
import Queue
import signal
import threading
import time

from rejected import consumer
from rejected import __version__

logger = logging.getLogger(__name__)


class MasterControlProgram(object):
    """Master Control Program keeps track of consumers"""
    _MIN_CONSUMERS = 1
    _MAX_CONSUMERS = 2
    _MAX_SHUTDOWN_WAIT = 10
    _POLL_INTERVAL = 30
    _POLL_RESULTS_INTERVAL = 1
    _SHUTDOWN_WAIT = 1

    def __init__(self, config):
        """Initialize the Master Control Program

        :param dict config: The full content from the YAML config file

        """
        # Default values
        self._consumers = dict()
        self._config = config
        self._is_running = True
        self._last_poll_results = dict()
        self._poll_data = {'time': 0, 'consumers': list()}
        self._poll_results_timer = None
        self._poll_timer = None
        self._shutdown_start = 0
        self._stats = dict()
        self._stats_queue = multiprocessing.Queue()
        self._timeouts = list()

        # Setup the signal handler for sigabrt
        self._setup_sigabrt()

        # Carry for calculating messages per second
        self._monitoring = config.get('monitor', False)
        logger.debug('Monitoring enabled: %s', self._monitoring)
        self._stats_logging = self._monitoring and config.get('log_stats', True)

        # Setup the queue depth polling
        self._poll_interval = config.get('poll_interval', self._POLL_INTERVAL)
        logger.debug('Set consumer poll interval to %i', self._poll_interval)

    def _add_shutdown_timer(self):
        """Add a timer to the IOLoop if the IOLoop is running to shut down in
        self._SHUTDOWN_WAIT seconds.

        """
        logger.debug('Waiting %i seconds to check shutdown state',
                     self._SHUTDOWN_WAIT)
        timer = threading.Timer(self._SHUTDOWN_WAIT,
                                self._stop_consumers)
        timer.start()
        self._timeouts.append(timer)

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

        :rtype: dict

        """
        return {consumer.RejectedConsumer.ERROR: 0,
                consumer.RejectedConsumer.PROCESSED: 0,
                consumer.RejectedConsumer.REDELIVERED: 0,
                consumer.RejectedConsumer.TIME_SPENT: 0,
                consumer.RejectedConsumer.TIME_WAITED: 0}

    def _calculate_stats(self, data):
        """Calculate the stats data for our process level data.

        :param data: The collected stats data to report on
        :type data: dict

        """
        timestamp = data['timestamp']
        del data['timestamp']

        # Iterate through the last poll results
        stats = self._consumer_stats_counter()
        consumer_stats = dict()
        for consumer_name in data.keys():
            consumer_stats[consumer_name] = self._consumer_stats_counter()
            consumer_stats[consumer_name]['consumers'] = \
                self._consumer_count(consumer_name)
            for process in data[consumer_name].keys():
                for key in stats:
                    value = data[consumer_name][process]['counts'][key]
                    stats[key] += value
                    consumer_stats[consumer_name][key] += value

        return {'last_poll': timestamp,
                'consumers': len(self._all_consumers),
                'consumer_data': consumer_stats,
                'process_data': data,
                'counts': stats}

    def _collect_results(self, data_values):
        """Receive the data from the consumers polled and process it.

        :param dict data_values: The poll data returned from the consumer
        :type data_values: dict

        """
        self._last_poll_results['timestamp'] = self._poll_data['timestamp']

        # Get the name and consumer name and remove it from what is reported
        consumer_name = data_values['consumer_name']
        del data_values['consumer_name']
        process_name = data_values['name']
        del data_values['name']

        # Add it to our last poll global data
        if consumer_name not in self._last_poll_results:
            self._last_poll_results[consumer_name] = dict()
        self._last_poll_results[consumer_name][process_name] = data_values

        # Find the position to remove the consumer from the list
        try:
            position = self._poll_data['consumers'].index(process_name)
        except ValueError:
            logger.error('Poll data from non-tracked consumer: %s',
                         process_name)
            position = None

        # Remove the consumer from the list we're waiting for data from
        if position is not None:
            self._poll_data['consumers'].pop(position)

        # Calculate global stats
        if not self._poll_data['consumers']:
            if self._stats_logging:
                self._stats = self._calculate_stats(self._last_poll_results)
                self._log_stats()

    def _consumer_count(self, name):
        """Returns the active consumer count for the given consumer type

        :rtype: int

        """
        return len([consumer_ for consumer_ in self._all_consumers
                    if consumer_.is_alive() and consumer_.name.find(name) > -1])

    @property
    def _total_consumer_count(self):
        """Returns the active consumer count

        :rtype: int

        """
        return len([consumer_ for consumer_ in self._all_consumers
                    if consumer_.is_alive()])

    def _create_consumer(self, consumer_number, consumer_name, connection_name):
        """Create a new consumer instances

        :param int consumer_number: The identification number for the consumer
        :param str consumer_name: The name of the consumer
        :param str connection_name: The name of the connection
        :rtype: multiprocessing.Process

        """
        process_name = '%s_%s' % (consumer_name, consumer_number)
        logger.debug('Creating a new consumer for %s: %s',
                     connection_name, process_name)
        kwargs = {'config': self._config,
                  'connection_name': connection_name,
                  'consumer_name': consumer_name,
                  'stats_queue': self._stats_queue}
        return consumer.RejectedConsumer(name=process_name,
                                         kwargs=kwargs)

    def _handle_sigabrt(self, signum_unused, frame_unused):
        """Called when SIGABRT is raised.

        :param signum_unused int: The signal number
        :param frame frame_unused: The stack frame when the signal was raised

        """
        if self._is_running:
            logger.warning('SIGABRT received but not shuting down')

    def _log_stats(self):
        """Output the stats to the logger."""
        logger.info('%i total consumers have processed %i  messages with %i '
                    'errors, waiting %.2f seconds and have spent %.2f seconds '
                    'processing messages',
                    self._stats['consumers'],
                    self._stats['counts']['processed'],
                    self._stats['counts']['failed'],
                    self._stats['counts']['waiting_time'],
                    self._stats['counts']['processing_time'])
        for key in self._stats['consumer_data'].keys():
            logger.info('%i %s consumers have processed %i  messages with %i '
                        'errors, waiting %.2f seconds and have spent %.2f '
                        'seconds processing messages',
                        self._stats['consumer_data'][key]['consumers'],
                        key,
                        self._stats['consumer_data'][key]['processed'],
                        self._stats['consumer_data'][key]['failed'],
                        self._stats['consumer_data'][key]['waiting_time'],
                        self._stats['consumer_data'][key]['processing_time'])

    @property
    def _max_shutdown_wait_exceeded(self):
        """Returns True if the maximum shutdown time has been exceeded

        :rtype: bool

        """
        return time.time() - self._shutdown_start > self._MAX_SHUTDOWN_WAIT

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
            if not consumer_.is_alive():
                logger.warning('Found dead consumer %s', consumer_.name)
                non_active_consumers.append(consumer_.name)
            else:
                if self._monitoring:
                    logger.debug('Asking %s for stats', consumer_.name)
                    self._poll_data['consumers'].append(consumer_.name)
                    os.kill(consumer_.pid, signal.SIGPROF)

        # Remove the objects if we have them
        self._prune_non_active_consumers(non_active_consumers)

        # If we don't have any active consumers, shutdown
        if not self._total_consumer_count:
            logger.debug('Did not find any active consumers in poll')
            return self.shutdown()

        # Check to see if any consumers reported back and start timer if not
        if self._monitoring:
            self._poll_results_check()

        # Start the timer again
        self._start_poll_timer()

    def _poll_results_check(self):
        """Check the polling results by checking to see if the stats queue is
        empty. If it is not, try and collect stats. If it is set a timer to
        call ourselves in _POLL_RESULTS_INTERVAL.

        """
        logger.debug('Checking for poll results')
        results = True
        while results:
            try:
                stats = self._stats_queue.get(False)
            except Queue.Empty:
                logger.debug('Stats queue is empty')
                break
            logger.debug('Received stats from %s', stats['name'])
            self._collect_results(stats)

        # If there are pending consumers to get stats for, start the timer
        if self._poll_data['consumers']:
            logger.debug('Starting poll results timer for %i consumer(s)',
                         len(self._poll_data['consumers']))
            self._start_poll_results_timer()

    def _prune_non_active_consumers(self, consumer_list):
        """Remove non-active consumers from the active stack

        :param list consumer_list: List of consumers to remove

        """
        for consumer_ in consumer_list:
            logger.debug('Pruning non-active consumer: %s', consumer_)
            if not self._remove_consumer(consumer_):
                logger.error('Could not find consumer %s to delete', consumer_)
                logger.debug('Consumers: %r', self._consumers)

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
                consumer_ = self._consumers[name]['consumers'].pop(offset)
                del consumer_
                return True

        return False

    def _remove_timeouts(self):
        """Stop all the active timeouts."""
        if self._poll_timer:
            logger.debug('Stopping the poll timer')
            self._poll_timer.cancel()

        if self._poll_results_timer:
            logger.debug('Stopping the poll results timer')
            self._poll_results_timer.cancel()

        for timeout in self._timeouts:
            logger.debug('Removing a timeout: %r', timeout)
            timeout.cancel()
        self._timeouts = list()

    def _start_poll_results_timer(self):
        """Setup a new poll results timer and start it"""
        self._poll_results_timer = threading.Timer(self._POLL_RESULTS_INTERVAL,
                                                   self._poll_results_check)
        self._poll_results_timer.start()

    def _start_poll_timer(self):
        """Setup a new poll timer to check consumer health and stats and
        start it

        """
        self._poll_timer = threading.Timer(self._poll_interval, self._poll)
        self._poll_timer.start()

    def _setup_sigabrt(self):
        """Called on initialization to setup the signal handler for SIGABRT"""
        signal.signal(signal.SIGABRT, self._handle_sigabrt)

    def _stop_consumers(self):
        """Iterate through all of the consumers shutting them down."""
        self._remove_timeouts()

        # See if we need to just stop
        if self._max_shutdown_wait_exceeded:
            logger.critical('Max shutdown exceeded, forcibly exiting')
            consumers_to_remove = list()
            for consumer_ in self._all_consumers:
                if consumer_.is_alive():
                    consumer_.terminate()
                    consumers_to_remove.append(consumer_)
            self._prune_non_active_consumers(consumers_to_remove)
            logger.debug('Consumers all terminated and removed')
            return self._stopped()

        # Stop if we have no running consumers
        if not self._consumer_count:
            logger.info('All consumers have stopped, shutting down')
            return self._stopped()

        # Loop through all of the bindings and try and shutdown consumers
        logger.debug('Asking %i consumer(s) nicely to stop',
                     self._consumer_count)
        for consumer_ in self._all_consumers:
            if consumer_.is_running:
                logger.debug('Sending signal to %s to stop', consumer_.name)
                os.kill(consumer_.pid, signal.SIGABRT)

        # Add a shutdown timer to call this method again in n seconds
        self._add_shutdown_timer()

    def _stopped(self):
        """Called when the consumers have all stopped, to exit out of
        MasterControlProgram.run().

        """
        self._is_running = False
        os.kill(os.getpid(), signal.SIGABRT)

    def run(self):
        """When the consumer is ready to start running, kick off all of our
        consumer consumers and then loop while we process messages.

        """
        logger.info('rejected v%s - i am consumer whore', __version__)
        # Get the consumer section from the config
        if 'Consumers' not in self._config:
            logger.error('Missing Consumers section of configuration, '
                         'aborting: %r', self._config)
            return

        # Loop through all of the consumers
        consumers = self._config['Consumers']
        for name in consumers:

            if 'queue' not in consumers[name]:
                logger.error('Consumer %s is missing a queue, skipping', name)
                continue

            # Create the dictionary values for this consumer consumer
            consumer_ = consumers[name]
            self._consumers[name] = {'min': consumer_.get('min',
                                                          self._MIN_CONSUMERS),
                                     'max': consumer_.get('max',
                                                         self._MAX_CONSUMERS),
                                     'queue': consumer_['queue'],
                                     'consumers': list()}

            # Iterate through the connections to create new consumers
            for connection_name in consumer_['connections']:

                logger.debug('Starting %i consumer(s) for %s on %s',
                             self._consumers[name]['min'],
                             name, connection_name)

                # Create the min amount of consumers per connection
                for consumer_number in xrange(0, self._consumers[name]['min']):

                    # Create the new consumer process
                    process = self._create_consumer(consumer_number,
                                                    name,
                                                    connection_name)

                    # Append the consumer to the consumer list
                    self._consumers[name]['consumers'].append(process)

                    # Start the consumer
                    process.start()

        # Kick off our poll timer
        self._start_poll_timer()

        # Loop while we're running, waking on alarms
        while self._is_running:
            logger.debug('Unpaused')
            signal.pause()

    def shutdown(self):
        """Graceful shutdown of the MCP means shutting down consumers too"""
        logger.debug('Master Control Program Shutting Down')
        self._shutdown_start = time.time()
        self._stop_consumers()
        signal.pause()
        logger.info('Exiting Master Control Program with up to a %i '
                    'second delay', self._MAX_SHUTDOWN_WAIT)
