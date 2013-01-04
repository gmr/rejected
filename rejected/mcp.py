"""
Master Control Program

"""
import logging
import multiprocessing
import os
import Queue
import signal
import sys
import threading
import time

from rejected import process
from rejected import state
from rejected import __version__

LOGGER = logging.getLogger(__name__)


class MasterControlProgram(state.State):
    """Master Control Program keeps track of and manages consumer processes."""
    _MIN_CONSUMERS = 1
    _MAX_CONSUMERS = 2
    _MAX_SHUTDOWN_WAIT = 10
    _POLL_INTERVAL = 30.0
    _POLL_RESULTS_INTERVAL = 3.0
    _SHUTDOWN_WAIT = 1

    def __init__(self, config, consumer=None):
        """Initialize the Master Control Program

        :param dict config: The full content from the YAML config file
        :param str consumer: If specified, only run processes for this consumer

        """
        self._set_process_name()
        LOGGER.info('rejected v%s initializing', __version__)
        super(MasterControlProgram, self).__init__()

        # Default values
        self._consumer = consumer
        self._consumers = dict()
        self._config = config
        self._last_poll_results = dict()
        self._poll_data = {'time': 0, 'processes': list()}
        self._poll_timer = None
        self._results_timer = None
        self._stats = dict()
        self._stats_queue = multiprocessing.Queue()

        # Carry for logging internal stats collection data
        self._log_stats_enabled = config.get('log_stats', False)
        LOGGER.debug('Stats logging enabled: %s', self._log_stats_enabled)

        # Setup the poller related threads
        self._poll_interval = config.get('poll_interval', self._POLL_INTERVAL)
        LOGGER.debug('Set process poll interval to %.2f', self._poll_interval)

    @property
    def _active_processes(self):
        """Return a list of all active processes, pruning dead ones

        :rtype: list

        """
        dead_processes = list()
        active_processes = multiprocessing.active_children()
        for process in active_processes:
            if not process.is_alive():
                dead_processes.append(process)
        if dead_processes:
            LOGGER.debug('Found %i dead processes to remove',
                         len(dead_processes))
            for process in dead_processes:
                self._remove_process(process.name)
                active_processes.remove(process)
        return active_processes

    def _calculate_stats(self, data):
        """Calculate the stats data for our process level data.

        :param data: The collected stats data to report on
        :type data: dict

        """
        timestamp = data['timestamp']
        del data['timestamp']
        LOGGER.debug('Calculating stats for data timestamp: %i', timestamp)

        # Iterate through the last poll results
        stats = self._consumer_stats_counter()
        consumer_stats = dict()
        for name in data.keys():
            consumer_stats[name] = self._consumer_stats_counter()
            consumer_stats[name]['processes'] = \
                self._process_count_by_consumer(name)
            for process in data[name].keys():
                for key in stats:
                    value = data[name][process]['counts'][key]
                    stats[key] += value
                    consumer_stats[name][key] += value

        # Return a data structure that can be used in reporting out the stats
        stats['processes'] = len(self._active_processes)
        return {'last_poll': timestamp,
                'consumers': consumer_stats,
                'process_data': data,
                'counts': stats}

    def _calculate_velocity(self, counts):
        """Calculate the message velocity to determine how many messages are
        processed per second.

        :param dict counts: The count dictionary to use for calculation
        :rtype: float

        """
        total_time = counts['idle_time'] + counts['processing_time']
        if total_time == 0 or counts['processed'] == 0:
            LOGGER.debug('Returning 0')
        return float(counts['processed']) / float(total_time)

    def _check_consumer_process_counts(self):
        """Check for the minimum consumer process levels and start up new
        processes needed.

        """
        LOGGER.debug('Checking minimum consumer process levels')
        for name in self._consumers:
            for connection in self._consumers[name]['connections']:

                processes_needed = self._process_spawn_qty(name, connection)
                if processes_needed:
                    LOGGER.debug('Need to spawn %i processes for %s on %s',
                                 processes_needed, name, connection)
                    self._start_processes(name, connection, processes_needed)

    def _consumer_dict(self, configuration):
        """Return a consumer dict for the given name and configuration.

        :param dict configuration: The consumer configuration
        :rtype: dict

        """
        # Keep a dict that has a list of processes by connection
        connections = dict()
        for connection in configuration['connections']:
            connections[connection] = list()
        return {'connections': connections,
                'min': configuration.get('min', self._MIN_CONSUMERS),
                'max': configuration.get('max', self._MAX_CONSUMERS),
                'last_proc_num': 0,
                'queue': configuration['queue'],
                'processes': dict()}

    def _consumer_keyword(self, counts):
        """Return consumer or consumers depending on the process count.

        :param dict counts: The count dictionary to use process count
        :rtype: str

        """
        LOGGER.debug('Received %r', counts)
        return 'consumer' if counts['processes'] == 1 else 'consumers'

    def _consumer_stats_counter(self):
        """Return a new consumer stats counter instance.

        :rtype: dict

        """
        return {process.Process.ERROR: 0,
                process.Process.PROCESSED: 0,
                process.Process.REDELIVERED: 0,
                process.Process.TIME_SPENT: 0,
                process.Process.TIME_WAITED: 0}

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
            LOGGER.error('Poll data from unexpected process: %s', process_name)
            position = None

        # Remove the consumer from the list we're waiting for data from
        if position is not None:
            self._poll_data['processes'].pop(position)

        # Calculate global stats
        if self._poll_data['processes']:
            LOGGER.debug('Still waiting on %i processes for stats',
                         len(self._poll_data['processes']))
            return

        # Calculate the stats
        self._stats = self._calculate_stats(self._last_poll_results)

        # If stats logging is enabled, log the stats
        if self._log_stats_enabled:
            self._log_stats()

    def _kill_processes(self):
        """Gets called on shutdown by the timer when too much time has gone by,
        calling the terminate method instead of nicely asking for the consumers
        to stop.

        """
        LOGGER.critical('Max shutdown exceeded, forcibly exiting')

        for process in multiprocessing.active_children():
            process.terminate()
        LOGGER.info('All processes should have terminated')
        time.sleep(0.5)
        while multiprocessing.active_children():
            for process in multiprocessing.active_children():
                LOGGER.warning('Killing %s (%s)', process.name, process.pid)
                os.kill(int(process.pid), signal.SIGKILL)
            time.sleep(1)

        return self._set_state(self.STATE_STOPPED)

    def _log_stats(self):
        """Output the stats to the LOGGER."""
        LOGGER.info('%i total %s have processed %i  messages with %i '
                    'errors, waiting %.2f seconds and have spent %.2f seconds '
                    'processing messages with an overall velocity of %.2f '
                    'messages per second.',
                    self._stats['counts']['processes'],
                    self._consumer_keyword(self._stats['counts']),
                    self._stats['counts']['processed'],
                    self._stats['counts']['failed'],
                    self._stats['counts']['idle_time'],
                    self._stats['counts']['processing_time'],
                    self._calculate_velocity(self._stats['counts']))
        for key in self._stats['consumers'].keys():
            LOGGER.info('%i %s for %s have processed %i messages with %i '
                        'errors, waiting %.2f seconds and have spent %.2f '
                        'seconds processing messages with an overall velocity '
                        'of %.2f messages per second.',
                        self._stats['consumers'][key]['processes'],
                        self._consumer_keyword(self._stats['consumers'][key]),
                        key,
                        self._stats['consumers'][key]['processed'],
                        self._stats['consumers'][key]['failed'],
                        self._stats['consumers'][key]['idle_time'],
                        self._stats['consumers'][key]['processing_time'],
                        self._calculate_velocity(self._stats['consumers'][key]))
        if self._poll_data['processes']:
            LOGGER.warning('%i process(es) did not respond with stats in '
                           'time: %r',
                           len(self._poll_data['processes']),
                           self._poll_data['processes'])

    def _new_process(self, consumer_name, connection_name):
        """Create a new consumer instances

        :param str consumer_name: The name of the consumer
        :param str connection_name: The name of the connection
        :return tuple: (str, process.Process)

        """
        process_name = '%s_%s' % (consumer_name,
                                  self._new_process_number(consumer_name))
        LOGGER.debug('Creating a new process for %s: %s',
                     connection_name, process_name)
        kwargs = {'config': self._config,
                  'connection_name': connection_name,
                  'consumer_name': consumer_name,
                  'stats_queue': self._stats_queue}
        return process_name, process.Process(name=process_name, kwargs=kwargs)

    def _new_process_number(self, name):
        """Increment the counter for the process id number for a given consumer
        configuration.

        :param str name: Consumer name
        :rtype: int

        """
        self._consumers[name]['last_proc_num'] += 1
        return self._consumers[name]['last_proc_num']

    def _poll(self):
        """Start the poll process by invoking the get_stats method of the
        consumers. If we hit this after another interval without fully
        processing, note it with a warning.

        """
        # Check to see if we have outstanding things to poll
        #if self._poll_data['processes']:
        #    LOGGER.warn('Poll interval failure for consumer(s): %r',
        #                 ', '.join(self._poll_data['processes']))

        # Keep track of running consumers
        dead_processes = list()

        # Start our data collection dict
        self._poll_data = {'timestamp': time.time(),
                           'processes': list()}

        # Iterate through all of the consumers
        for process in self._active_processes:
            LOGGER.debug('Checking runtime state of %s', process.name)
            if process == multiprocessing.current_process():
                LOGGER.debug('Matched current process in active_processes')
                continue

            if not process.is_alive():
                LOGGER.critical('Found dead consumer %s', process.name)
                dead_processes.append(process.name)
            else:
                LOGGER.debug('Asking %s for stats', process.name)
                #self._poll_data['processes'].append(process.name)
                os.kill(process.pid, signal.SIGPROF)

        # Remove the objects if we have them
        for process_name in dead_processes:
            self._remove_process(process_name)

        # Check if we need to start more processes
        self._check_consumer_process_counts()

        # If we don't have any active consumers, shutdown
        if not self.total_process_count:
            LOGGER.debug('Did not find any active consumers in poll')
            return self._set_state(self.STATE_STOPPED)

        # Check to see if any consumers reported back and start timer if not
        #self._poll_results_check()

        # Start the timer again
        self._start_poll_timer()

    @property
    def _poll_duration_exceeded(self):
        """Return true if the poll time has been exceeded.
        :rtype: bool

        """
        return  (time.time() -
                 self._poll_data['timestamp']) >= self._poll_interval

    def _poll_results_check(self):
        """Check the polling results by checking to see if the stats queue is
        empty. If it is not, try and collect stats. If it is set a timer to
        call ourselves in _POLL_RESULTS_INTERVAL.

        """
        LOGGER.debug('Checking for poll results')
        results = True
        while results:
            try:
                stats = self._stats_queue.get(False)
            except Queue.Empty:
                LOGGER.debug('Stats queue is empty')
                break
            LOGGER.debug('Received stats from %s', stats['name'])
            self._collect_results(stats)

        # If there are pending consumers to get stats for, start the timer
        if self._poll_data['processes']:

            if self._poll_duration_exceeded and self._log_stats_enabled:
                self._log_stats()
            LOGGER.debug('Starting poll results timer for %i consumer(s)',
                         len(self._poll_data['processes']))
            self._start_poll_results_timer()

    def _process(self, consumer_name, process_name):
        """Return the process handle for the given consumer name and process
        name.

        :param str consumer_name: The consumer name from config
        :param str process_name: The automatically assigned process name
        :rtype: rejected.process.Process

        """
        return self._consumers[consumer_name]['processes'][process_name]

    def _process_count(self, name, connection):
        """Return the process count for the given consumer name and connection.

        :param str name: The consumer name
        :param str connection: The connection name
        :rtype: int

        """
        return len(self._consumers[name]['connections'][connection])

    def _process_count_by_consumer(self, name):
        """Return the process count by consumer only.

        :param str name: The consumer name
        :rtype: int

        """
        count = 0
        for connection in self._consumers[name]['connections']:
            count += len(self._consumers[name]['connections'][connection])
        return count

    def _process_spawn_qty(self, name, connection):
        """Return the number of processes to spawn for the given consumer name
        and connection.

        :param str name: The consumer name
        :param str connection: The connection name
        :rtype: int

        """
        return self._consumers[name]['min'] - self._process_count(name,
                                                                  connection)

    def _remove_process(self, process):
        """Remove the specified consumer process

        :param str process: The process name to remove

        """
        for consumer_name in self._consumers:
            if process in self._consumers[consumer_name]['processes']:
                del self._consumers[consumer_name]['processes'][process]
                self._remove_process_from_list(consumer_name, process)
                LOGGER.debug('Removed %s from %s\'s process list',
                             process, consumer_name)
                return
        LOGGER.warning('Could not find process %s to remove', process)

    def _remove_process_from_list(self, name, process):
        """Remove the process from the consumer connections list.

        :param str name: The consumer name
        :param str process: The process name

        """
        for conn in self._consumers[name]['connections']:
            if process in self._consumers[name]['connections'][conn]:
                idx = self._consumers[name]['connections'][conn].index(process)
                self._consumers[name]['connections'][conn].pop(idx)
                LOGGER.debug('Process %s removed from %s connections',
                             process, name)
                return
        LOGGER.warning('Could not find %s in %s\'s process list', process, name)

    def _set_process_name(self):
        """Set the process name for the top level process so that it shows up
        in logs in a more trackable fasion.

        """
        process = multiprocessing.current_process()
        for offset in xrange(0, len(sys.argv)):
            if sys.argv[offset] == '-c':
                name = sys.argv[offset + 1].split('/')[-1]
                process.name = name.split('.')[0]
                break

    def _start_poll_results_timer(self):
        """Start the poll results timer to see if there are results from the
        last poll yet.
        """
        self._results_timer = self._start_timer(self._results_timer,
                                                'poll_results_timer',
                                                self._POLL_RESULTS_INTERVAL,
                                                self._poll_results_check)

    def _start_poll_timer(self):
        """Start the poll timer to fire the polling at the next interval"""
        self._poll_timer = self._start_timer(self._poll_timer, 'poll_timer',
                                             self._poll_interval, self._poll)

    def _start_process(self, name, connection):
        """Start a new consumer process for the given consumer & connection name

        :param str name: The consumer name
        :param str connection: The connection name

        """
        LOGGER.info('Spawning new consumer process for %s to %s',
                    name, connection)

        # Create the new consumer process
        process_name, process = self._new_process(name, connection)

        # Append the process to the consumer process list
        self._consumers[name]['processes'][process_name] = process
        self._consumers[name]['connections'][connection].append(process_name)

        # Start the process
        process.start()

    def _start_processes(self, name, connection, quantity):
        """Start the specified quantity of consumer processes for the given
        consumer and connection.

        :param str name: The consumer name
        :param str connection: The connection name
        :param int quantity: The quantity of processes to start

        """
        for process in xrange(0, quantity):
            self._start_process(name, connection)

    def _start_timer(self, timer, name, duration, callback):
        """Start a timer for the given object, name, duration and callback.

        :param threading.Timer timer: The previous timer instance
        :param str name: The timer name
        :param int or float duration: The timer duration
        :param method callback: The method to call when timer fires
        :rtype: threading.Timer

        """
        if timer and timer.is_alive():
            timer.cancel()
            LOGGER.debug('Cancelled live timer: %s', name)

        timer = threading.Timer(duration, callback)
        timer.name = name
        timer.start()
        LOGGER.debug('Started %s timer for %.2f seconds calling back %r',
                     name, duration, callback)
        return timer

    def _setup_consumers(self):
        """Iterate through each consumer in the configuration and kick off the
        minimal amount of processes, setting up the runtime data as well.

        """
        for name in self._config['Consumers']:

            # Hold the config as a shortcut
            config = self._config['Consumers'][name]

            # If queue is not configured, report the error but skip processes
            if 'queue' not in config:
                LOGGER.critical('Consumer %s is missing a queue, skipping',
                                name)
                continue

            # Create the dictionary values for this process
            self._consumers[name] = self._consumer_dict(config)

            # Iterate through the connections to create new consumer processes
            for connection in self._consumers[name]['connections']:
                self._start_processes(name, connection,
                                      self._consumers[name]['min'])

    def _stop_process(self, process):
        """Stop the specified process

        :param multiprocessing.Process process: The process to stop

        """
        if not process.is_alive():
            return

        LOGGER.debug('Sending signal to %s (%i) to stop',
                     process.name, process.pid)
        os.kill(int(process.pid), signal.SIGABRT)

    def _stop_timers(self):
        """Stop all the active timeouts."""
        if self._poll_timer and self._poll_timer.is_alive():
            LOGGER.debug('Stopping the poll timer')
            self._poll_timer.cancel()

        if self._results_timer and self._results_timer.is_alive():
            LOGGER.debug('Stopping the poll results timer')
            self._results_timer.cancel()

    def stop_processes(self):
        """Iterate through all of the consumer processes shutting them down."""
        self._set_state(self.STATE_SHUTTING_DOWN)
        LOGGER.debug('Stopping consumer processes')
        self._stop_timers()

        active_processes = multiprocessing.active_children()

        # Stop if we have no running consumers
        if not active_processes:
            LOGGER.info('All consumer processes have stopped')
            return self._set_state(self.STATE_STOPPED)

        # Iterate through all of the bindings and try and shutdown processes
        for process in active_processes:
            self._stop_process(process)

        # Wait for them to shutdown cleanly
        time.sleep(2)

        iterations = 0
        while multiprocessing.active_children():
            LOGGER.debug('Waiting on %i active processes to shut down',
                         self.total_process_count)
            time.sleep(1)
            iterations += 1

            # If the shutdown process waited long enough, kill the consumers
            if iterations == self._MAX_SHUTDOWN_WAIT:
                self._kill_processes()
                break

        LOGGER.debug('All consumer processes stopped')
        self._set_state(self.STATE_STOPPED)

    def run(self):
        """When the consumer is ready to start running, kick off all of our
        consumer consumers and then loop while we process messages.

        """
        # Set the state to active
        self._set_state(self.STATE_ACTIVE)

        # Get the consumer section from the config
        if 'Consumers' not in self._config:
            LOGGER.error('Missing Consumers section of configuration, '
                         'aborting: %r', self._config)
            return self._set_state(self.STATE_STOPPED)

        # Strip consumers if a consumer is specified
        if self._consumer:
            consumers = self._config['Consumers'].keys()
            for consumer in consumers:
                if consumer != self._consumer:
                    LOGGER.debug('Removing %s for %s only processing',
                                 consumer, self._consumer)
                    del self._config['Consumers'][consumer]

        # Setup consumers and start the processes
        self._setup_consumers()

        # Kick off the poll timer
        self._start_poll_timer()

        # Loop for the lifetime of the app, sleeping 0.2 seconds at a time
        while self.is_running:
            time.sleep(0.2)

        # Note we're exiting run
        LOGGER.info('Exiting Master Control Program')

    @property
    def total_process_count(self):
        """Returns the active consumer process count

        :rtype: int

        """
        return len(self._active_processes)
