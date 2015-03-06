"""
Master Control Program

"""
import logging
import multiprocessing
import os
import psutil
import Queue
import signal
import sys
import time

from rejected import common
from rejected import process
from rejected import __version__

LOGGER = logging.getLogger(__name__)


class MasterControlProgram(common.State):
    """Master Control Program keeps track of and manages consumer processes."""
    _DEFAULT_CONSUMER_QTY = 1
    _MAX_SHUTDOWN_WAIT = 10
    _POLL_INTERVAL = 60.0
    _POLL_RESULTS_INTERVAL = 3.0
    _SHUTDOWN_WAIT = 1

    def __init__(self, config, consumer=None, profile=None, quantity=None):
        """Initialize the Master Control Program

        :param config: The full content from the YAML config file
        :type config: helper.config.Config
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
        self._polled = False

        # Carry for logging internal stats collection data
        self._log_stats_enabled = config.application.get('log_stats', False)
        LOGGER.debug('Stats logging enabled: %s', self._log_stats_enabled)

        # Setup the poller related threads
        self._poll_interval = config.application.get('poll_interval',
                                                     self._POLL_INTERVAL)
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
                    proc = psutil.Process(child.pid)
                except psutil.NoSuchProcess:
                    dead_processes.append((consumer, name))
                    continue

                if self.is_a_dead_or_zombie_process(proc):
                    dead_processes.append((consumer, name))
                else:
                    active_processes.append(child)

        if dead_processes:
            LOGGER.debug('Removing %i dead process(es)', len(dead_processes))
            for proc in dead_processes:
                self.remove_consumer_process(*proc)
        return active_processes

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
            for proc in data[name].keys():
                for key in stats:
                    value = data[name][proc]['counts'][key]
                    stats[key] += value
                    consumer_stats[name][key] += value

        # Return a data structure that can be used in reporting out the stats
        stats['processes'] = len(self.active_processes)
        return {'last_poll': timestamp,
                'consumers': consumer_stats,
                'process_data': data,
                'counts': stats}

    @staticmethod
    def calculate_velocity(counts):
        """Calculate the message velocity to determine how many messages are
        processed per second.

        :param dict counts: The count dictionary to use for calculation
        :rtype: float

        """
        total_time = counts['idle_time'] + counts['processing_time']
        if total_time and counts['processed']:
            return float(counts['processed'] / float(total_time))
        return 0

    def check_process_counts(self):
        """Check for the minimum consumer process levels and start up new
        processes needed.

        """
        LOGGER.debug('Checking minimum consumer process levels')
        for name in self._consumers:
            for connection in self._consumers[name]['connections']:
                processes_needed = self.process_spawn_qty(name, connection)
                LOGGER.debug('Need to spawn %i processes for %s on %s',
                             processes_needed, name, connection)
                if processes_needed:
                    self.start_processes(name, connection, processes_needed)

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

        # Calculate the stats
        self._stats = self.calculate_stats(self._last_poll_results)

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
                'qty': configuration.get('qty', self._DEFAULT_CONSUMER_QTY),
                'last_proc_num': 0,
                'queue': configuration['queue'],
                'processes': dict()}

    @staticmethod
    def consumer_keyword(counts):
        """Return consumer or consumers depending on the process count.

        :param dict counts: The count dictionary to use process count
        :rtype: str

        """
        return 'consumer' if counts['processes'] == 1 else 'consumers'

    @staticmethod
    def consumer_stats_counter():
        """Return a new consumer stats counter instance.

        :rtype: dict

        """
        return {process.Process.ERROR: 0,
                process.Process.PROCESSED: 0,
                process.Process.REDELIVERED: 0,
                process.Process.TIME_SPENT: 0,
                process.Process.TIME_WAITED: 0}

    def get_consumer_process(self, consumer, name):
        """Get the process object for the specified consumer and process name.

        :param str consumer: The consumer name
        :param str name: The process name
        :returns: multiprocessing.Process

        """
        return self._consumers[consumer]['processes'].get(name)

    @staticmethod
    def is_a_dead_or_zombie_process(process):
        """Checks to see if the specified process is a zombie or dead.

        :param psutil.Process: The process to check
        :rtype: bool

        """
        try:
            if process.status in [psutil.STATUS_DEAD, psutil.STATUS_ZOMBIE]:
                try:
                    LOGGER.debug('Found dead or zombie process with '
                                 '%s fds',
                                 process.get_num_fds())
                except psutil.AccessDenied as error:
                    LOGGER.debug('Found dead or zombie process, '
                                 'could not get fd count: %s', error)
                    return True
        except psutil.NoSuchProcess:
            LOGGER.debug('Found dead or zombie process')
            return True
        return False

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
        if not self._stats.get('counts'):
            LOGGER.info('Did not receive any stats data from children')
            return

        LOGGER.info('%i total %s have processed %i messages with %i '
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
        process_name = '%s-%s' % (consumer_name,
                                  self.new_process_number(consumer_name))
        LOGGER.debug('Creating a new process for %s: %s',
                     connection_name, process_name)
        kwargs = {'config': self._config.application,
                  'connection_name': connection_name,
                  'consumer_name': consumer_name,
                  'profile': self._profile,
                  'daemon': False,
                  'stats_queue': self._stats_queue,
                  'logging_config': self._config.logging}
        return process_name, process.Process(name=process_name, kwargs=kwargs)

    def new_process_number(self, name):
        """Increment the counter for the process id number for a given consumer
        configuration.

        :param str name: Consumer name
        :rtype: int

        """
        self._consumers[name]['last_proc_num'] += 1
        return self._consumers[name]['last_proc_num']

    def on_timer(self, signum, unused_frame):
        LOGGER.debug('Timer fired')
        if not self._polled:
            self.poll()
            self._polled = True
            self.set_timer(5)  # Wait 5 seconds for results
        else:
            self._polled = False
            self.poll_results_check()
            self.set_timer(self._poll_interval)  # Wait poll interval duration

            # If stats logging is enabled, log the stats
            if self._log_stats_enabled:
                self.log_stats()

    def poll(self):
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
        for proc in self.active_processes:
            LOGGER.debug('Checking runtime state of %s', proc.name)
            if proc == multiprocessing.current_process():
                LOGGER.debug('Matched current process in active_processes')
                continue

            # Send the profile signal
            os.kill(int(proc.pid), signal.SIGPROF)
            self._poll_data['processes'].append(proc.name)

        # Check if we need to start more processes
        self.check_process_counts()

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
        LOGGER.debug('Checking for poll results')
        while True:
            try:
                stats = self._stats_queue.get(False)
            except Queue.Empty:
                break
            try:
                self._poll_data['processes'].remove(stats['name'])
            except ValueError:
                pass
            self.collect_results(stats)

        if self._poll_data['processes']:
            LOGGER.warning('Did not receive results from %r',
                           self._poll_data['processes'])

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
        return self._consumers[name]['qty'] - self.process_count(name,
                                                                 connection)

    def remove_consumer_process(self, consumer, name):
        """Remove all details for the specified consumer and process name.

        :param str consumer: The consumer name
        :param str name: The process name

        """
        for conn in self._consumers[consumer]['connections']:
            if name in self._consumers[consumer]['connections'][conn]:
                self._consumers[consumer]['connections'][conn].remove(name)
        if name in self._consumers[consumer]['processes']:
            try:
                self._consumers[consumer]['processes'][name].terminate()
            except OSError:
                pass
            del self._consumers[consumer]['processes'][name]

    def run(self):
        """When the consumer is ready to start running, kick off all of our
        consumer consumers and then loop while we process messages.

        """
        # Set the state to active
        self.set_state(self.STATE_ACTIVE)

        # Get the consumer section from the config
        if 'Consumers' not in self._config.application:
            LOGGER.error('Missing Consumers section of configuration, '
                         'aborting: %r', self._config.application)
            return self.set_state(self.STATE_STOPPED)

        # Strip consumers if a consumer is specified
        if self._consumer:
            consumers = self._config.application.Consumers.keys()
            for consumer in consumers:
                if consumer != self._consumer:
                    LOGGER.debug('Removing %s for %s only processing',
                                 consumer, self._consumer)
                    del self._config.application.Consumers[consumer]

        # Setup consumers and start the processes
        self.setup_consumers()

        # Set the SIGALRM handler for poll interval
        signal.signal(signal.SIGALRM, self.on_timer)

        # Kick off the poll timer
        signal.setitimer(signal.ITIMER_REAL, self._poll_interval, 0)

        # Loop for the lifetime of the app, pausing for a signal to pop up
        while self.is_running and self.total_process_count:
            if self._state != self.STATE_SLEEPING:
                self.set_state(self.STATE_SLEEPING)
            signal.pause()

        # Note we're exiting run
        LOGGER.info('Exiting Master Control Program')

    @staticmethod
    def set_process_name():
        """Set the process name for the top level process so that it shows up
        in logs in a more trackable fasion.

        """
        process = multiprocessing.current_process()
        for offset in xrange(0, len(sys.argv)):
            if sys.argv[offset] == '-c':
                name = sys.argv[offset + 1].split('/')[-1]
                process.name = name.split('.')[0]
                break

    def set_timer(self, duration):
        """Setup the next alarm to fire and then wait for it to fire.

        :param int duration: How long to sleep

        """
        # Make sure that the application is not shutting down before sleeping
        if self.is_shutting_down:
            LOGGER.debug('Not sleeping, application is trying to shutdown')
            return

        # Set the signal timer
        signal.setitimer(signal.ITIMER_REAL, duration, 0)

    def start_process(self, name, connection):
        """Start a new consumer process for the given consumer & connection name

        :param str name: The consumer name
        :param str connection: The connection name

        """
        process_name, process = self.new_process(name, connection)

        LOGGER.info('Spawning %s process for %s to %s',
                    process_name, name, connection)

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
        for name in self._config.application.Consumers:

            # Hold the config as a shortcut
            config = self._config.application.Consumers[name]

            # If queue is not configured, report the error but skip processes
            if 'queue' not in config:
                LOGGER.critical('Consumer %s is missing a queue, skipping',
                                name)
                continue

            # Create the dictionary values for this process
            self._consumers[name] = self.consumer_dict(config)

            if self._quantity:
                self._consumers[name]['qty'] = self._quantity

            # Iterate through the connections to create new consumer processes
            for connection in self._consumers[name]['connections']:
                self.start_processes(name, connection,
                                     self._consumers[name]['qty'])

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

    @property
    def total_process_count(self):
        """Returns the active consumer process count

        :rtype: int

        """
        return len(self.active_processes)
