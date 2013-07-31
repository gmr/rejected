"""
Master Control Program

"""
import clihelper
import logging
import multiprocessing
import os
import psutil
import Queue
import signal
import sys
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
    _POLL_INTERVAL = 60.0
    _POLL_RESULTS_INTERVAL = 3.0
    _SHUTDOWN_WAIT = 1

    def __init__(self, config, consumer=None, profile=None, quantity=None):
        """Initialize the Master Control Program

        :param dict config: The full content from the YAML config file
        :param str consumer: If specified, only run processes for this consumer
        :param str profile: Optional profile output directory to
                            enable profiling

        """
        self.set_process_name()
        LOGGER.info('rejected v%s initializing', __version__)
        super(MasterControlProgram, self).__init__()

        # Default values
        self._consumer = consumer
        self._consumers = dict()
        self._config = config
        self._last_poll_results = dict()
        self._poll_data = {'time': 0, 'processes': list()}
        self._poll_timer = None
        self._profile = profile
        self._quantity = quantity if consumer else None
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
    def active_processes(self):
        """Return a list of all active processes, pruning dead ones

        :rtype: list

        """
        active_processes, dead_processes = list(), list()
        for consumer in self._consumers:
            for name in self._consumers[consumer]['processes']:
                child = self.get_consumer_process(consumer, name)
                if int(child.pid) == os.getpid():
                    continue
                try:
                    process = psutil.Process(child.pid)
                except psutil.NoSuchProcess:
                    dead_processes.append((consumer, name))
                    continue

                if self.is_a_dead_or_zombie_process(process):
                    dead_processes.append((consumer, name))
                else:
                    active_processes.append(child)

        if dead_processes:
            LOGGER.info('Removing %i dead process(es)', len(dead_processes))
            for process in dead_processes:
                self.remove_consumer_process(*process)
        return active_processes

    def is_a_dead_or_zombie_process(self, process):
        try:
            if process.status in [psutil.STATUS_DEAD, psutil.STATUS_ZOMBIE]:
                try:
                    LOGGER.info('Found dead or zombie process with '
                                '%s fds',
                                process.get_num_fds())
                except psutil.AccessDenied as error:
                    LOGGER.error('Found dead or zombie process, '
                                 'could not get fd count: %s', error)
                    return True
        except psutil.NoSuchProcess:
            LOGGER.info('Found dead or zombie process')
            return True

        return False

    def get_consumer_process(self, consumer, name):
        return self._consumers[consumer]['processes'].get(name)

    def remove_consumer_process(self, consumer, name):
        for conn in self._consumers[consumer]['connections']:
            if name in self._consumers[consumer]['connections'][conn]:
                self._consumers[consumer]['connections'][conn].remove(name)
        if name in self._consumers[consumer]['processes']:
            try:
                self._consumers[consumer]['processes'][name].terminate()
            except OSError:
                pass
            del self._consumers[consumer]['processes'][name]

    def calculate_stats(self, data):
        """Calculate the stats data for our process level data.

        :param data: The collected stats data to report on
        :type data: dict

        """
        timestamp = data['timestamp']
        del data['timestamp']
        LOGGER.debug('Calculating stats for data timestamp: %i', timestamp)

        # Iterate through the last poll results
        stats = self.consumer_stats_counter()
        consumer_stats = dict()
        for name in data.keys():
            consumer_stats[name] = self.consumer_stats_counter()
            consumer_stats[name]['processes'] = \
                self.process_count_by_consumer(name)
            for process in data[name].keys():
                for key in stats:
                    value = data[name][process]['counts'][key]
                    stats[key] += value
                    consumer_stats[name][key] += value

        # Return a data structure that can be used in reporting out the stats
        stats['processes'] = len(self.active_processes)
        return {'last_poll': timestamp,
                'consumers': consumer_stats,
                'process_data': data,
                'counts': stats}

    def calculate_velocity(self, counts):
        """Calculate the message velocity to determine how many messages are
        processed per second.

        :param dict counts: The count dictionary to use for calculation
        :rtype: float

        """
        total_time = counts['idle_time'] + counts['processing_time']
        if total_time == 0 or counts['processed'] == 0:
            LOGGER.debug('Returning 0')
        return float(counts['processed']) / float(total_time)

    def check_consumer_process_counts(self):
        """Check for the minimum consumer process levels and start up new
        processes needed.

        """
        LOGGER.debug('Checking minimum consumer process levels')
        for name in self._consumers:
            for connection in self._consumers[name]['connections']:
                processes_needed = self.process_spawn_qty(name, connection)
                if processes_needed:
                    LOGGER.debug('Need to spawn %i processes for %s on %s',
                                 processes_needed, name, connection)
                    self.start_processes(name, connection, processes_needed)

    def consumer_dict(self, configuration):
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

    def consumer_keyword(self, counts):
        """Return consumer or consumers depending on the process count.

        :param dict counts: The count dictionary to use process count
        :rtype: str

        """
        LOGGER.debug('Received %r', counts)
        return 'consumer' if counts['processes'] == 1 else 'consumers'

    def consumer_stats_counter(self):
        """Return a new consumer stats counter instance.

        :rtype: dict

        """
        return {process.Process.ERROR: 0,
                process.Process.PROCESSED: 0,
                process.Process.REDELIVERED: 0,
                process.Process.TIME_SPENT: 0,
                process.Process.TIME_WAITED: 0}

    def collect_results(self, data_values):
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
        self._stats = self.calculate_stats(self._last_poll_results)

        # If stats logging is enabled, log the stats
        if self._log_stats_enabled:
            self.log_stats()

    def kill_processes(self):
        """Gets called on shutdown by the timer when too much time has gone by,
        calling the terminate method instead of nicely asking for the consumers
        to stop.

        """
        LOGGER.critical('Max shutdown exceeded, forcibly exiting')
        processes = True
        while processes:
            processes = self.active_processes
            for process in processes:
                if int(process.pid) != int(os.getpid()):
                    LOGGER.warning('Killing %s (%s)', process.name, process.pid)
                    os.kill(int(process.pid), signal.SIGKILL)
                else:
                    LOGGER.warning('Cowardly refusing kill self (%s, %s)',
                                   process.pid, os.getpid())
            time.sleep(0.5)

        LOGGER.info('Killed all children')
        return self.set_state(self.STATE_STOPPED)

    def log_stats(self):
        """Output the stats to the LOGGER."""
        LOGGER.info('%i total %s have processed %i  messages with %i '
                    'errors, waiting %.2f seconds and have spent %.2f seconds '
                    'processing messages with an overall velocity of %.2f '
                    'messages per second.',
                    self._stats['counts']['processes'],
                    self.consumer_keyword(self._stats['counts']),
                    self._stats['counts']['processed'],
                    self._stats['counts']['failed'],
                    self._stats['counts']['idle_time'],
                    self._stats['counts']['processing_time'],
                    self.calculate_velocity(self._stats['counts']))
        for key in self._stats['consumers'].keys():
            LOGGER.info('%i %s for %s have processed %i messages with %i '
                        'errors, waiting %.2f seconds and have spent %.2f '
                        'seconds processing messages with an overall velocity '
                        'of %.2f messages per second.',
                        self._stats['consumers'][key]['processes'],
                        self.consumer_keyword(self._stats['consumers'][key]),
                        key,
                        self._stats['consumers'][key]['processed'],
                        self._stats['consumers'][key]['failed'],
                        self._stats['consumers'][key]['idle_time'],
                        self._stats['consumers'][key]['processing_time'],
                        self.calculate_velocity(self._stats['consumers'][key]))
        if self._poll_data['processes']:
            LOGGER.warning('%i process(es) did not respond with stats in '
                           'time: %r',
                           len(self._poll_data['processes']),
                           self._poll_data['processes'])

    def new_process(self, consumer_name, connection_name):
        """Create a new consumer instances

        :param str consumer_name: The name of the consumer
        :param str connection_name: The name of the connection
        :return tuple: (str, process.Process)

        """
        process_name = '%s_%s' % (consumer_name,
                                  self.new_process_number(consumer_name))
        LOGGER.debug('Creating a new process for %s: %s',
                     connection_name, process_name)
        kwargs = {'config': self._config['Application'],
                  'connection_name': connection_name,
                  'consumer_name': consumer_name,
                  'profile': self._profile,
                  'daemon': False,
                  'stats_queue': self._stats_queue,
                  'logging_config': self._config['Logging']}
        return process_name, process.Process(name=process_name, kwargs=kwargs)

    def new_process_number(self, name):
        """Increment the counter for the process id number for a given consumer
        configuration.

        :param str name: Consumer name
        :rtype: int

        """
        self._consumers[name]['last_proc_num'] += 1
        return self._consumers[name]['last_proc_num']

    def poll(self, unusued_signal, unused_frame):
        """Start the poll process by invoking the get_stats method of the
        consumers. If we hit this after another interval without fully
        processing, note it with a warning.

        """
        self.set_state(self.STATE_ACTIVE)

        # If we don't have any active consumers, shutdown
        if not self.total_process_count:
            LOGGER.debug('Did not find any active consumers in poll')
            return self.set_state(self.STATE_STOPPED)

        # Start our data collection dict
        self._poll_data = {'timestamp': time.time(),
                           'processes': list()}

        # Iterate through all of the consumers
        for process in self.active_processes:
            LOGGER.debug('Checking runtime state of %s', process.name)
            if process == multiprocessing.current_process():
                LOGGER.debug('Matched current process in active_processes')
                continue

            # Send the profile signal
            os.kill(int(process.pid), signal.SIGPROF)

        # Check if we need to start more processes
        #self.check_consumer_process_counts()

        # Check to see if any consumers reported back and start timer if not
        #self.start_poll_results_timer()

        # Start the timer again
        self.sleep()

    @property
    def poll_duration_exceeded(self):
        """Return true if the poll time has been exceeded.
        :rtype: bool

        """
        return (time.time() -
                self._poll_data['timestamp']) >= self._poll_interval

    def poll_results_check(self):
        """Check the polling results by checking to see if the stats queue is
        empty. If it is not, try and collect stats. If it is set a timer to
        call ourselves in _POLL_RESULTS_INTERVAL.

        """
        LOGGER.info('Checking for poll results')
        results = True
        while results:
            try:
                stats = self._stats_queue.get(False)
            except Queue.Empty:
                LOGGER.debug('Stats queue is empty')
                break
            LOGGER.debug('Received stats from %s', stats['name'])
            self.collect_results(stats)

        # If there are pending consumers to get stats for, start the timer
        if self._poll_data['processes']:

            if self.poll_duration_exceeded and self._log_stats_enabled:
                self.log_stats()
            LOGGER.debug('Starting poll results timer for %i consumer(s)',
                         len(self._poll_data['processes']))

            self.start_poll_results_timer()

    def process(self, consumer_name, process_name):
        """Return the process handle for the given consumer name and process
        name.

        :param str consumer_name: The consumer name from config
        :param str process_name: The automatically assigned process name
        :rtype: rejected.process.Process

        """
        return self._consumers[consumer_name]['processes'][process_name]

    def process_count(self, name, connection):
        """Return the process count for the given consumer name and connection.

        :param str name: The consumer name
        :param str connection: The connection name
        :rtype: int

        """
        return len(self._consumers[name]['connections'][connection])

    def process_count_by_consumer(self, name):
        """Return the process count by consumer only.

        :param str name: The consumer name
        :rtype: int

        """
        count = 0
        for connection in self._consumers[name]['connections']:
            count += len(self._consumers[name]['connections'][connection])
        return count

    def process_spawn_qty(self, name, connection):
        """Return the number of processes to spawn for the given consumer name
        and connection.

        :param str name: The consumer name
        :param str connection: The connection name
        :rtype: int

        """
        return self._consumers[name]['min'] - self.process_count(name,
                                                                 connection)

    def set_process_name(self):
        """Set the process name for the top level process so that it shows up
        in logs in a more trackable fasion.

        """
        process = multiprocessing.current_process()
        for offset in xrange(0, len(sys.argv)):
            if sys.argv[offset] == '-c':
                name = sys.argv[offset + 1].split('/')[-1]
                process.name = name.split('.')[0]
                break

    def sleep(self):
        """Setup the next alarm to fire and then wait for it to fire."""
        # Make sure that the application is not shutting down before sleeping
        if self.is_shutting_down:
            LOGGER.debug('Not sleeping, application is trying to shutdown')
            return

        # Set the signal timer
        signal.setitimer(signal.ITIMER_REAL, self._poll_interval, 0)

        # Toggle that we are running
        self.set_state(self.STATE_SLEEPING)

    def start_process(self, name, connection):
        """Start a new consumer process for the given consumer & connection name

        :param str name: The consumer name
        :param str connection: The connection name

        """
        LOGGER.info('Spawning new consumer process for %s to %s',
                    name, connection)

        process_name, process = self.new_process(name, connection)

        # Append the process to the consumer process list
        self._consumers[name]['processes'][process_name] = process
        self._consumers[name]['connections'][connection].append(process_name)

        # Start the process
        process.start()

    def start_processes(self, name, connection, quantity):
        """Start the specified quantity of consumer processes for the given
        consumer and connection.

        :param str name: The consumer name
        :param str connection: The connection name
        :param int quantity: The quantity of processes to start

        """
        for process in xrange(0, quantity):
            self.start_process(name, connection)

    def setup_consumers(self):
        """Iterate through each consumer in the configuration and kick off the
        minimal amount of processes, setting up the runtime data as well.

        """
        for name in self._config['Application']['Consumers']:

            # Hold the config as a shortcut
            config = self._config['Application']['Consumers'][name]

            # If queue is not configured, report the error but skip processes
            if 'queue' not in config:
                LOGGER.critical('Consumer %s is missing a queue, skipping',
                                name)
                continue

            # Create the dictionary values for this process
            self._consumers[name] = self.consumer_dict(config)

            if self._quantity:
                self._consumers[name]['min'] = self._quantity

            # Iterate through the connections to create new consumer processes
            for connection in self._consumers[name]['connections']:
                self.start_processes(name, connection,
                                     self._consumers[name]['min'])

    def stop_processes(self):
        """Iterate through all of the consumer processes shutting them down."""
        self.set_state(self.STATE_SHUTTING_DOWN)
        LOGGER.info('Stopping consumer processes')

        signal.signal(signal.SIGALRM, signal.SIG_IGN)
        signal.signal(signal.SIGCHLD, signal.SIG_IGN)
        signal.signal(signal.SIGPROF, signal.SIG_IGN)
        signal.setitimer(signal.ITIMER_REAL, 0, 0)

        # Send SIGABRT
        LOGGER.info('Sending SIGABRT to active children')
        for process in multiprocessing.active_children():
            if int(process.pid) != os.getpid():
                os.kill(int(process.pid), signal.SIGABRT)

        # Wait for them to finish up to MAX_SHUTDOWN_WAIT
        iterations = 0
        processes = self.total_process_count
        while processes:
            LOGGER.info('Waiting on %i active processes to shut down',
                        processes)
            try:
                time.sleep(0.5)
            except KeyboardInterrupt:
                LOGGER.info('Caught CTRL-C, Killing Children')
                self.kill_processes()
                self.set_state(self.STATE_STOPPED)
                return

            iterations += 1
            if iterations == self._MAX_SHUTDOWN_WAIT:
                self.kill_processes()
                break
            processes = self.total_process_count

        LOGGER.debug('All consumer processes stopped')
        self.set_state(self.STATE_STOPPED)

    def run(self):
        """When the consumer is ready to start running, kick off all of our
        consumer consumers and then loop while we process messages.

        """
        # Set the state to active
        self.set_state(self.STATE_ACTIVE)

        # Get the consumer section from the config
        if 'Consumers' not in self._config['Application']:
            LOGGER.error('Missing Consumers section of configuration, '
                         'aborting: %r', self._config['Application'])
            return self.set_state(self.STATE_STOPPED)

        # Strip consumers if a consumer is specified
        if self._consumer:
            consumers = self._config['Application']['Consumers'].keys()
            for consumer in consumers:
                if consumer != self._consumer:
                    LOGGER.debug('Removing %s for %s only processing',
                                 consumer, self._consumer)
                    del self._config['Application']['Consumers'][consumer]

        # Setup consumers and start the processes
        self.setup_consumers()

        # Remove root logger handlers due to tornado
        root_logger = logging.getLogger()
        handlers = root_logger.handlers
        for handler in handlers:
            root_logger.removeHandler(handler)

        # Set the ALRM handler for poll interval
        signal.signal(signal.SIGALRM, self.poll)

        # Kick off the poll timer
        signal.setitimer(signal.ITIMER_REAL, self._poll_interval, 0)

        # Loop for the lifetime of the app, pausing for a signal to pop up
        while self.is_running and self.total_process_count:
            self.set_state(self.STATE_SLEEPING)
            signal.pause()

        # Note we're exiting run
        LOGGER.info('Exiting Master Control Program')

    @property
    def total_process_count(self):
        """Returns the active consumer process count

        :rtype: int

        """
        return len(self.active_processes)
