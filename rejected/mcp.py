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
        self.is_running = True
        self._last_poll_results = dict()
        self._poll_data = {'time': 0, 'processes': list()}
        self._poll_results_timer = None
        self._poll_timer = None
        self._stats = dict()
        self._stats_queue = multiprocessing.Queue()

        # Setup the signal handler for sigabrt
        self._setup_sigabrt()

        # Carry for calculating messages per second
        self._monitoring = config.get('monitor', False)
        logger.debug('Monitoring enabled: %s', self._monitoring)
        self._stats_logging = self._monitoring and config.get('log_stats', True)

        # Setup the queue depth polling
        self._poll_interval = config.get('poll_interval', self._POLL_INTERVAL)
        logger.debug('Set consumer poll interval to %i', self._poll_interval)

    @property
    def _active_processes(self):
        """Return a list of all active consumers, pruning dead ones

        :rtype: list

        """
        processes = list()
        dead_processes = list()
        for name in self._consumers:
            for process_name in self._consumers[name]['processes']:
                process = self._process(name, process_name)
                if process.pid == os.getpid():
                    logger.warning('Process %s should be %s and is reporting '
                                   'MCP pid', process.name, process_name)
                    continue
                if process.is_alive():
                    processes.append(process)
                else:
                    dead_processes.append(process_name)
        if dead_processes:
            logger.debug('Found %i dead processes to remove',
                         len(dead_processes))
            for process_name in dead_processes:
                self._remove_process(process_name)
        return processes

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
            consumer_stats[consumer_name]['processes'] = \
                self._consumer_count(consumer_name)
            for process in data[consumer_name].keys():
                for key in stats:
                    value = data[consumer_name][process]['counts'][key]
                    stats[key] += value
                    consumer_stats[consumer_name][key] += value

        return {'last_poll': timestamp,
                'processes': len(self._active_processes),
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
            position = self._poll_data['processes'].index(process_name)
        except ValueError:
            logger.error('Poll data from non-tracked consumer: %s',
                         process_name)
            position = None

        # Remove the consumer from the list we're waiting for data from
        if position is not None:
            self._poll_data['processes'].pop(position)

        # Calculate global stats
        if not self._poll_data['processes']:
            if self._stats_logging:
                self._stats = self._calculate_stats(self._last_poll_results)
                self._log_stats()

    def _consumer_count(self, name):
        """Returns the active consumer process count for the given consumer type

        :rtype: int

        """
        return len([process for process in self._active_processes
                    if process.name.find(name) > -1])

    def _create_consumer(self, consumer_number, consumer_name, connection_name):
        """Create a new consumer instances

        :param int consumer_number: The identification number for the consumer
        :param str consumer_name: The name of the consumer
        :param str connection_name: The name of the connection
        :return tuple: (str, consumer.RejectedConsumer)

        """
        process_name = '%s_%s' % (consumer_name, consumer_number)
        logger.debug('Creating a new consumer for %s: %s',
                     connection_name, process_name)
        kwargs = {'config': self._config,
                  'connection_name': connection_name,
                  'consumer_name': consumer_name,
                  'stats_queue': self._stats_queue}
        return process_name, consumer.RejectedConsumer(name=process_name,
                                                       kwargs=kwargs)

    def _handle_sigabrt(self, signum_unused, frame_unused):
        """Called when SIGABRT is raised.

        :param int signum_unused: The signal number
        :param frame frame_unused: The stack frame when the signal was raised

        """
        if self.is_running:
            logger.warning('SIGABRT received but not shuting down')

    def _kill_consumers(self):
        """Gets called by the timer when too much time has gone by, calling
        the terminate method instead of nicely asking for the consumers to
        stop.

        """
        logger.critical('Max shutdown exceeded, forcibly exiting')
        for process in self._active_processes:
            name = process.name
            if process.pid == os.getpid():
                logger.warning('Process %s and is reporting MCP pid',
                               process.name)
                continue
            if process.is_alive():
                process.terminate()
            for consumer_name in self._consumers:
                if name in self._consumers[consumer_name]['processes']:
                    del self._consumers[consumer_name]['processes'][name]
                del self._consumers[consumer_name]
        self._consumers = dict()
        logger.debug('Consumers all terminated and removed')
        return self._stopped()

    def _log_stats(self):
        """Output the stats to the logger."""
        logger.info('%i total consumers have processed %i  messages with %i '
                    'errors, waiting %.2f seconds and have spent %.2f seconds '
                    'processing messages',
                    self._stats['processes'],
                    self._stats['counts']['processed'],
                    self._stats['counts']['failed'],
                    self._stats['counts']['waiting_time'],
                    self._stats['counts']['processing_time'])
        for key in self._stats['consumer_data'].keys():
            logger.info('%i %s consumers have processed %i  messages with %i '
                        'errors, waiting %.2f seconds and have spent %.2f '
                        'seconds processing messages',
                        self._stats['consumer_data'][key]['processes'],
                        key,
                        self._stats['consumer_data'][key]['processed'],
                        self._stats['consumer_data'][key]['failed'],
                        self._stats['consumer_data'][key]['waiting_time'],
                        self._stats['consumer_data'][key]['processing_time'])

    def _poll(self):
        """Start the poll process by invoking the get_stats method of the
        consumers. If we hit this after another interval without fully
        processing, note it with a warning.

        """
        # Check to see if we have outstanding things to poll
        if self._poll_data['processes']:
            logger.warn('Poll interval failure for consumer(s): %r',
                         ', '.join(self._poll_data['processes']))

        # Keep track of running consumers
        dead_processes = list()

        # Start our data collection dict
        self._poll_data = {'timestamp': time.time(),
                           'processes': list()}

        # Iterate through all of the consumers
        for process in self._active_processes:
            logger.debug('Checking runtime state of %s', process.name)
            if not process.is_alive():
                logger.warning('Found dead consumer %s', process.name)
                dead_processes.append(process.name)
            else:
                if self._monitoring:
                    logger.debug('Asking %s for stats', process.name)
                    self._poll_data['processes'].append(process.name)
                    os.kill(process.pid, signal.SIGPROF)

        # Remove the objects if we have them
        for process_name in dead_processes:
            self._remove_process(process_name)

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
        if self._poll_data['processes']:
            logger.debug('Starting poll results timer for %i consumer(s)',
                         len(self._poll_data['processes']))
            self._start_poll_results_timer()

    def _process(self, consumer_name, process_name):
        """Return the process handle for the given consumer name and process name.

        :param str consumer_name: The consumer name from config
        :param str process_name: The automatically assigned process name
        :rtype: consumer.RejectedConsumer

        """
        return self._consumers[consumer_name]['processes'][process_name]

    def _remove_process(self, process_name):
        """Remove the specified consumer process

        :param str process_name: The process name to remove

        """
        for consumer_name in self._consumers:
            if process_name in self._consumers[consumer_name]['processes']:
                logger.debug('Removing %s from %s', process_name, consumer_name)
                del self._consumers[consumer_name]['processes'][process_name]
                return
        logger.warning('Could not find process %s to remove', process_name)

    def _remove_timeouts(self):
        """Stop all the active timeouts."""
        if self._poll_timer:
            logger.debug('Stopping the poll timer')
            self._poll_timer.cancel()

        if self._poll_results_timer:
            logger.debug('Stopping the poll results timer')
            self._poll_results_timer.cancel()

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

        # Stop if we have no running consumers
        if not self._active_processes:
            logger.info('All consumers have stopped, shutting down')
            return self._stopped()

        # Loop through all of the bindings and try and shutdown consumers
        for process in self._active_processes:
            logger.debug('Sending signal to %s (%i) to stop',
                         process.name, process.pid)
            os.kill(process.pid, signal.SIGABRT)

        # Add a shutdown timer to call this method again in n seconds
        #self._add_shutdown_timer()

    def _stopped(self):
        """Called when the consumers have all stopped, to exit out of
        MasterControlProgram.run().

        """
        self.is_running = False
        os.kill(os.getpid(), signal.SIGALRM)

    @property
    def _total_consumer_count(self):
        """Returns the active consumer count

        :rtype: int

        """
        return len(self._active_processes)

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
                                     'processes': dict()}

            # Iterate through the connections to create new consumers
            for connection_name in consumer_['connections']:

                logger.debug('Starting %i consumer(s) for %s on %s',
                             self._consumers[name]['min'],
                             name, connection_name)

                # Create the min amount of consumers per connection
                for consumer_number in xrange(0, self._consumers[name]['min']):

                    # Create the new consumer process
                    proc_name, process = self._create_consumer(consumer_number,
                                                               name,
                                                               connection_name)

                    # Append the consumer to the consumer list
                    self._consumers[name]['processes'][proc_name] = process

                    # Start the consumer
                    process.start()

        # Kick off our poll timer
        self._start_poll_timer()
        while self.is_running:
            time.sleep(0.2)
        logger.debug('Exiting')

    def shutdown(self):
        """Graceful shutdown of the MCP means shutting down consumers too"""
        logger.debug('Master Control Program Shutting Down')
        self._stop_consumers()
        while self._active_processes:
            time.sleep(0.2)
        logger.info('Exiting Master Control Program')
