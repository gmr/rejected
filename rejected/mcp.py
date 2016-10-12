"""
Master Control Program

"""
import collections
import logging
import multiprocessing
import os
import psutil
try:
    import Queue as queue
except ImportError:
    import queue
import signal
import sys
import time

from rejected import state, process, __version__

LOGGER = logging.getLogger(__name__)


class Consumer(object):
    """Class used for keeping track of each consumer type being managed by
    the MCP

    """

    def __init__(self, connections, last_proc_num, processes, qty, queue):
        self.connections = connections
        self.last_proc_num = last_proc_num
        self.processes = processes
        self.qty = qty
        self.queue = queue


class MasterControlProgram(state.State):
    """Master Control Program keeps track of and manages consumer processes."""

    DEFAULT_CONSUMER_QTY = 1
    MAX_SHUTDOWN_WAIT = 10
    MAX_UNRESPONSIVE_COUNT = 3
    POLL_INTERVAL = 60.0
    POLL_RESULTS_INTERVAL = 3.0
    SHUTDOWN_WAIT = 1

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
        self._active_cache = None
        self.consumer_cfg = self.get_consumer_cfg(config, consumer, quantity)
        self.consumers = dict()
        self.config = config
        self.last_poll_results = dict()
        self.poll_data = {'time': 0, 'processes': list()}
        self.poll_timer = None
        self.profile = profile
        self.results_timer = None
        self.stats = dict()
        self.stats_queue = multiprocessing.Queue()
        self.polled = False
        self.unresponsive = collections.Counter()

        # Flag to indicate child creation error
        self.child_abort = False

        # Carry for logging internal stats collection data
        self.log_stats_enabled = config.application.get('stats', {}).get(
            'log', config.application.get('log_stats', False))
        LOGGER.debug('Stats logging enabled: %s', self.log_stats_enabled)

        # Setup the poller related threads
        self.poll_interval = config.application.get('poll_interval',
                                                    self.POLL_INTERVAL)
        LOGGER.debug('Set process poll interval to %.2f', self.poll_interval)

    def active_processes(self, use_cache=True):
        """Return a list of all active processes, pruning dead ones

        :rtype: list

        """
        if use_cache and self._active_cache and \
                self._active_cache[0] > time.time() - self.poll_interval:
            return self._active_cache[1]
        active_processes, dead_processes = list(), list()
        for consumer in self.consumers:
            for name in self.consumers[consumer].processes:
                child = self.get_consumer_process(consumer, name)
                if int(child.pid) == os.getpid():
                    continue
                try:
                    proc = psutil.Process(child.pid)
                except psutil.NoSuchProcess:
                    dead_processes.append((consumer, name))
                    continue

                if self.unresponsive[name] >= self.MAX_UNRESPONSIVE_COUNT:
                    LOGGER.info('Killing unresponsive consumer %s (%i): '
                                '%i misses',
                                name, proc.pid, self.unresponsive[name])
                    try:
                        os.kill(child.pid, signal.SIGABRT)
                    except OSError:
                        pass
                    dead_processes.append((consumer, name))
                elif self.is_dead(proc, name):
                    dead_processes.append((consumer, name))
                else:
                    active_processes.append(child)

        if dead_processes:
            LOGGER.debug('Removing %i dead process(es)', len(dead_processes))
            for proc in dead_processes:
                self.remove_consumer_process(*proc)
        self._active_cache = time.time(), active_processes
        return active_processes

    def calculate_stats(self, data):
        """Calculate the stats data for our process level data.

        :param data: The collected stats data to report on
        :type data: dict

        """
        timestamp = data['timestamp']
        del data['timestamp']

        # Iterate through the last poll results
        stats = self.consumer_stats_counter()
        consumer_stats = dict()
        for name in data.keys():
            consumer_stats[name] = self.consumer_stats_counter()
            consumer_stats[name]['processes'] = \
                self.process_count_by_consumer(name)
            for proc in data[name].keys():
                for key in stats:
                    value = data[name][proc]['counts'].get(key, 0)
                    stats[key] += value
                    consumer_stats[name][key] += value

        # Return a data structure that can be used in reporting out the stats
        stats['processes'] = len(self.active_processes())
        return {
            'last_poll': timestamp,
            'consumers': consumer_stats,
            'process_data': data,
            'counts': stats
        }

    def check_process_counts(self):
        """Check for the minimum consumer process levels and start up new
        processes needed.

        """
        LOGGER.debug('Checking minimum consumer process levels')
        for name in self.consumers:
            for connection in self.consumers[name].connections:
                processes_needed = self.process_spawn_qty(name, connection)
                if processes_needed:
                    LOGGER.info('Need to spawn %i processes for %s on %s',
                                processes_needed, name, connection)
                    self.start_processes(name, connection, processes_needed)

    def collect_results(self, data_values):
        """Receive the data from the consumers polled and process it.

        :param dict data_values: The poll data returned from the consumer
        :type data_values: dict

        """
        self.last_poll_results['timestamp'] = self.poll_data['timestamp']

        # Get the name and consumer name and remove it from what is reported
        consumer_name = data_values['consumer_name']
        del data_values['consumer_name']
        process_name = data_values['name']
        del data_values['name']

        # Add it to our last poll global data
        if consumer_name not in self.last_poll_results:
            self.last_poll_results[consumer_name] = dict()
        self.last_poll_results[consumer_name][process_name] = data_values

        # Calculate the stats
        self.stats = self.calculate_stats(self.last_poll_results)

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
        return {
            process.Process.ERROR: 0,
            process.Process.PROCESSED: 0,
            process.Process.REDELIVERED: 0
        }

    def get_consumer_process(self, consumer, name):
        """Get the process object for the specified consumer and process name.

        :param str consumer: The consumer name
        :param str name: The process name
        :returns: multiprocessing.Process

        """
        return self.consumers[consumer].processes.get(name)

    @staticmethod
    def get_consumer_cfg(config, only, qty):
        """Get the consumers config, possibly filtering the config if only
        or qty is set.

        :param config: The consumers config section
        :type config: helper.config.Config
        :param str only: When set, filter to run only this consumer
        :param int qty: When set, set the consumer qty to this value
        :rtype: dict

        """
        consumers = dict(config.application.Consumers)
        if only:
            for key in consumers.keys():
                if key != only:
                    del consumers[key]
            if qty:
                consumers[only]['qty'] = qty
        return consumers

    def is_dead(self, proc, name):
        """Checks to see if the specified process is dead.

        :param psutil.Process proc: The process to check
        :param str name: The name of consumer
        :rtype: bool

        """
        status = proc.status()
        LOGGER.debug('Process %s (%s) status: %r (Unresponsive Count: %s)',
                     name, proc.pid, status, self.unresponsive[name])
        try:
            if status in [psutil.STATUS_RUNNING,
                          psutil.STATUS_SLEEPING]:
                return False
            if status == psutil.STATUS_ZOMBIE:
                proc.terminate()
                return True
            elif status == psutil.STATUS_STOPPED:
                return True
            elif status == psutil.STATUS_DEAD:
                return True
        except psutil.NoSuchProcess:
            LOGGER.debug('Process is dead and does not exist')
            return True
        return False

    def kill_processes(self):
        """Gets called on shutdown by the timer when too much time has gone by,
        calling the terminate method instead of nicely asking for the consumers
        to stop.

        """
        LOGGER.critical('Max shutdown exceeded, forcibly exiting')
        processes = self.active_processes(False)
        while processes:
            for proc in self.active_processes(False):
                if int(proc.pid) != int(os.getpid()):
                    LOGGER.warning('Killing %s (%s)', proc.name, proc.pid)
                    try:
                        os.kill(int(proc.pid), signal.SIGKILL)
                    except OSError:
                        pass
                else:
                    LOGGER.warning('Cowardly refusing kill self (%s, %s)',
                                   proc.pid, os.getpid())
            time.sleep(0.5)
            processes = self.active_processes(False)

        LOGGER.info('Killed all children')
        return self.set_state(self.STATE_STOPPED)

    def log_stats(self):
        """Output the stats to the LOGGER."""
        if not self.stats.get('counts'):
            LOGGER.info('Did not receive any stats data from children')
            return

        if self.poll_data['processes']:
            LOGGER.warning('%i process(es) did not respond with stats: %r',
                           len(self.poll_data['processes']),
                           self.poll_data['processes'])

        if self.stats['counts']['processes'] > 1:
            LOGGER.info('%i consumers processed %i messages with %i errors',
                        self.stats['counts']['processes'],
                        self.stats['counts']['processed'],
                        self.stats['counts']['failed'])

        for key in self.stats['consumers'].keys():
            LOGGER.info('%i %s %s processed %i messages with %i errors',
                        self.stats['consumers'][key]['processes'], key,
                        self.consumer_keyword(self.stats['consumers'][key]),
                        self.stats['consumers'][key]['processed'],
                        self.stats['consumers'][key]['failed'])

    def new_consumer(self, config):
        """Return a consumer dict for the given name and configuration.

        :param dict config: The consumer configuration
        :rtype: dict

        """
        return Consumer(dict([(c, []) for c in config['connections']]), 0,
                        dict(), config.get('qty', self.DEFAULT_CONSUMER_QTY),
                        config['queue'])

    def new_process(self, consumer_name, connection_name):
        """Create a new consumer instances

        :param str consumer_name: The name of the consumer
        :param str connection_name: The name of the connection
        :return tuple: (str, process.Process)

        """
        process_name = '%s-%s' % (consumer_name,
                                  self.new_process_number(consumer_name))
        kwargs = {
            'config': self.config.application,
            'connection_name': connection_name,
            'consumer_name': consumer_name,
            'profile': self.profile,
            'daemon': False,
            'stats_queue': self.stats_queue,
            'logging_config': self.config.logging
        }
        return process_name, process.Process(name=process_name, kwargs=kwargs)

    def new_process_number(self, name):
        """Increment the counter for the process id number for a given consumer
        configuration.

        :param str name: Consumer name
        :rtype: int

        """
        self.consumers[name].last_proc_num += 1
        return self.consumers[name].last_proc_num

    def on_abort(self, _signum, _unused_frame):
        LOGGER.debug('Abort signal received from child')
        time.sleep(1)
        if not self.active_processes():
            LOGGER.info('Stopping with no active processes and child error')
            self.set_state(self.STATE_STOPPED)

    def on_timer(self, _signum, _unused_frame):
        if self.is_shutting_down:
            LOGGER.debug('Polling timer fired while shutting down')
            return
        if not self.polled:
            self.poll()
            self.polled = True
            self.set_timer(5)  # Wait 5 seconds for results
        else:
            self.polled = False
            self.poll_results_check()
            self.set_timer(self.poll_interval)  # Wait poll interval duration

            # If stats logging is enabled, log the stats
            if self.log_stats_enabled:
                self.log_stats()

            # Increment the unresponsive children
            for proc_name in self.poll_data['processes']:
                self.unresponsive[proc_name] += 1

            # Remove counters for processes that came back to life
            for proc_name in list(self.unresponsive.keys()):
                if proc_name not in self.poll_data['processes']:
                    del self.unresponsive[proc_name]

    def poll(self):
        """Start the poll process by invoking the get_stats method of the
        consumers. If we hit this after another interval without fully
        processing, note it with a warning.

        """
        self.set_state(self.STATE_ACTIVE)

        # If we don't have any active consumers, spawn new ones
        if not self.total_process_count:
            LOGGER.debug('Did not find any active consumers in poll')
            return self.check_process_counts()

        # Start our data collection dict
        self.poll_data = {'timestamp': time.time(), 'processes': list()}

        # Iterate through all of the consumers
        for proc in list(self.active_processes()):
            if proc == multiprocessing.current_process():
                continue

            # Send the profile signal
            os.kill(int(proc.pid), signal.SIGPROF)
            self.poll_data['processes'].append(proc.name)

        # Check if we need to start more processes
        self.check_process_counts()

    @property
    def poll_duration_exceeded(self):
        """Return true if the poll time has been exceeded.
        :rtype: bool

        """
        return (time.time() - self.poll_data['timestamp']) >= self.poll_interval

    def poll_results_check(self):
        """Check the polling results by checking to see if the stats queue is
        empty. If it is not, try and collect stats. If it is set a timer to
        call ourselves in _POLL_RESULTS_INTERVAL.

        """
        LOGGER.debug('Checking for poll results')
        while True:
            try:
                stats = self.stats_queue.get(False)
            except queue.Empty:
                break
            try:
                self.poll_data['processes'].remove(stats['name'])
            except ValueError:
                pass
            self.collect_results(stats)

        if self.poll_data['processes']:
            LOGGER.warning('Did not receive results from %r',
                           self.poll_data['processes'])

    def process(self, name, process_name):
        """Return the process handle for the given consumer name and process
        name.

        :param str name: The consumer name from config
        :param str process_name: The automatically assigned process name
        :rtype: rejected.process.Process

        """
        return self.consumers[name].processes[process_name]

    def process_count(self, name, connection):
        """Return the process count for the given consumer name and connection.

        :param str name: The consumer name
        :param str connection: The connection name
        :rtype: int

        """
        return len(self.consumers[name].connections[connection])

    def process_count_by_consumer(self, name):
        """Return the process count by consumer only.

        :param str name: The consumer name
        :rtype: int

        """
        count = 0
        for connection in self.consumers[name].connections:
            count += len(self.consumers[name].connections.get(connection))
        return count

    def process_spawn_qty(self, name, connection):
        """Return the number of processes to spawn for the given consumer name
        and connection.

        :param str name: The consumer name
        :param str connection: The connection name
        :rtype: int

        """
        return self.consumers[name].qty - self.process_count(name, connection)

    def remove_consumer_process(self, consumer, name):
        """Remove all details for the specified consumer and process name.

        :param str consumer: The consumer name
        :param str name: The process name

        """
        my_pid = os.getpid()
        for conn in self.consumers[consumer].connections:
            if name in self.consumers[consumer].connections[conn]:
                self.consumers[consumer].connections[conn].remove(name)
        if name in self.consumers[consumer].processes:
            child = self.consumers[consumer].processes[name]
            if child.is_alive():
                if child.pid != my_pid:
                    try:
                        child.terminate()
                    except OSError:
                        pass
                else:
                    LOGGER.debug('Child has my pid? %r, %r', my_pid, child.pid)
            del self.consumers[consumer].processes[name]

    def run(self):
        """When the consumer is ready to start running, kick off all of our
        consumer consumers and then loop while we process messages.

        """
        self.set_state(self.STATE_ACTIVE)
        self.setup_consumers()

        # Set the SIGABRT handler for child creation errors
        signal.signal(signal.SIGABRT, self.on_abort)

        # Set the SIGALRM handler for poll interval
        signal.signal(signal.SIGALRM, self.on_timer)

        # Kick off the poll timer
        signal.setitimer(signal.ITIMER_REAL, self.poll_interval, 0)

        # Loop for the lifetime of the app, pausing for a signal to pop up
        while self.is_running:
            if not self.is_sleeping:
                self.set_state(self.STATE_SLEEPING)
            signal.pause()

        # Note we're exiting run
        LOGGER.info('Exiting Master Control Program')

    @staticmethod
    def set_process_name():
        """Set the process name for the top level process so that it shows up
        in logs in a more trackable fashion.

        """
        proc = multiprocessing.current_process()
        for offset in range(0, len(sys.argv)):
            if sys.argv[offset] == '-c':
                name = sys.argv[offset + 1].split('/')[-1]
                proc.name = name.split('.')[0]
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
        process_name, proc = self.new_process(name, connection)
        LOGGER.info('Spawning %s process for %s to %s', process_name, name,
                    connection)

        # Append the process to the consumer process list
        self.consumers[name].processes[process_name] = proc
        self.consumers[name].connections[connection].append(process_name)

        # Start the process
        proc.start()

    def start_processes(self, name, connection, quantity):
        """Start the specified quantity of consumer processes for the given
        consumer and connection.

        :param str name: The consumer name
        :param str connection: The connection name
        :param int quantity: The quantity of processes to start

        """
        [self.start_process(name, connection) for _i in range(0, quantity)]

    def setup_consumers(self):
        """Iterate through each consumer in the configuration and kick off the
        minimal amount of processes, setting up the runtime data as well.

        """
        for name in self.consumer_cfg.keys():
            self.consumers[name] = self.new_consumer(self.consumer_cfg[name])
            for connection in self.consumers[name].connections:
                self.start_processes(name, connection, self.consumers[name].qty)

    def stop_processes(self):
        """Iterate through all of the consumer processes shutting them down."""
        self.set_state(self.STATE_SHUTTING_DOWN)
        LOGGER.info('Stopping consumer processes')

        signal.signal(signal.SIGABRT, signal.SIG_IGN)
        signal.signal(signal.SIGALRM, signal.SIG_IGN)
        signal.signal(signal.SIGCHLD, signal.SIG_IGN)
        signal.signal(signal.SIGPROF, signal.SIG_IGN)
        signal.setitimer(signal.ITIMER_REAL, 0, 0)

        # Send SIGABRT
        LOGGER.info('Sending SIGABRT to active children')
        for proc in multiprocessing.active_children():
            if int(proc.pid) != os.getpid():
                os.kill(int(proc.pid), signal.SIGABRT)

        # Wait for them to finish up to MAX_SHUTDOWN_WAIT
        for iteration in range(0, self.MAX_SHUTDOWN_WAIT):
            processes = len(self.active_processes(False))
            if not processes:
                break

            LOGGER.info('Waiting on %i active processes to shut down (%i/%i)',
                        processes, iteration, self.MAX_SHUTDOWN_WAIT)
            try:
                time.sleep(0.5)
            except KeyboardInterrupt:
                break

        if len(self.active_processes(False)):
            self.kill_processes()

        LOGGER.debug('All consumer processes stopped')
        self.set_state(self.STATE_STOPPED)

    @property
    def total_process_count(self):
        """Returns the active consumer process count

        :rtype: int

        """
        return len(self.active_processes(False))
