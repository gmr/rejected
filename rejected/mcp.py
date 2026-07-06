"""
Master Control Program

"""

import collections
import logging
import multiprocessing
import os
import queue
import signal
import sys
import time
import types
import typing

import psutil

from . import __version__, models, process, prometheus, state

LOGGER = logging.getLogger(__name__)

_PROCESS_RUNNING = [psutil.STATUS_RUNNING, psutil.STATUS_SLEEPING]
_PROCESS_STOPPED_OR_DEAD = [psutil.STATUS_STOPPED, psutil.STATUS_DEAD]


class Consumer:
    """Class used for keeping track of each consumer type being managed by
    the MCP

    """

    def __init__(
        self,
        last_proc_num: int,
        processes: dict[str, process.Process],
        qty: int,
        queue: str,
    ) -> None:
        self.last_proc_num = last_proc_num
        self.processes = processes
        self.qty = qty
        self.queue = queue


class MasterControlProgram(state.State):
    """Master Control Program keeps track of and manages consumer processes."""

    DEFAULT_CONSUMER_QTY: typing.ClassVar[int] = 1
    MAX_SHUTDOWN_WAIT: typing.ClassVar[int] = 10
    MAX_UNRESPONSIVE_COUNT: typing.ClassVar[int] = 3
    POLL_INTERVAL: typing.ClassVar[float] = 60.0
    POLL_RESULTS_INTERVAL: typing.ClassVar[float] = 3.0
    SHUTDOWN_WAIT: typing.ClassVar[int] = 1

    def __init__(
        self,
        config: models.Config,
        consumer: str | None = None,
        profile: str | None = None,
        quantity: int | None = None,
        max_messages: int | None = None,
    ) -> None:
        """Initialize the Master Control Program

        :param config: The full content from the YAML config file
        :param str consumer: If specified, only run processes for this consumer
        :param str profile: Optional profile output directory to
                            enable profiling
        :param int quantity: Optional quantity override

        """
        self.set_process_name()
        LOGGER.info('rejected v%s initializing', __version__)
        super().__init__()

        # Default values
        self._active_cache: tuple[float, list[process.Process]] | None = None
        self.consumer_cfg: dict[str, models.ConsumerConfig] = (
            self.get_consumer_cfg(config, consumer, quantity)
        )
        self.consumers: dict[str, Consumer] = {}
        self.config: models.Config = config
        self.consumer_baselines: dict[str, collections.Counter[str]] = {}
        self.last_poll_results: dict[str, typing.Any] = {}
        self.poll_data: dict[str, typing.Any] = {'time': 0, 'processes': []}
        self.poll_timer: float | None = None
        self.profile: str | None = profile
        self.results_timer: float | None = None
        self.stats: dict[str, typing.Any] = {}
        self.stats_queue: multiprocessing.Queue[dict[str, typing.Any]] = (
            multiprocessing.Queue()
        )
        self.polled: bool = False
        self.unresponsive: collections.Counter[str] = collections.Counter()

        # Flag to indicate child creation error
        self.child_abort: bool = False

        # Flag set by the controller to request the run loop to exit
        self.stop_requested: bool = False

        # Carry for logging internal stats collection data
        self.log_stats_enabled: bool = config.stats.log
        LOGGER.debug('Stats logging enabled: %s', self.log_stats_enabled)

        # Setup the poller related threads
        self.max_messages: int | None = max_messages
        self.poll_interval: float = config.poll_interval
        LOGGER.debug('Set process poll interval to %.2f', self.poll_interval)

    def active_processes(
        self, use_cache: bool = True
    ) -> list[process.Process]:
        """Return a list of all active processes, pruning dead ones

        :rtype: list

        """
        LOGGER.debug('Checking active processes (cache: %s)', use_cache)
        if (
            use_cache
            and self._active_cache
            and self._active_cache[0] > time.time() - self.poll_interval
        ):
            return self._active_cache[1]
        active_processes: list[process.Process] = []
        dead_processes: list[tuple[str, str]] = []
        for consumer in self.consumers:
            for name in list(self.consumers[consumer].processes.keys()):
                child = self.get_consumer_process(consumer, name)
                if child is None:
                    continue
                if child.pid is None:
                    dead_processes.append((consumer, name))
                    continue
                elif int(child.pid) == os.getpid():
                    continue
                try:
                    proc = psutil.Process(child.pid)
                except (FileNotFoundError, psutil.NoSuchProcess):
                    dead_processes.append((consumer, name))
                    continue

                if self.unresponsive[name] >= self.MAX_UNRESPONSIVE_COUNT:
                    LOGGER.info(
                        'Killing unresponsive consumer %s (%i): %i misses',
                        name,
                        proc.pid,
                        self.unresponsive[name],
                    )
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
            for dead_proc in dead_processes:
                self.remove_consumer_process(*dead_proc)
        self._active_cache = time.time(), active_processes
        return active_processes

    def calculate_stats(
        self, data: dict[str, typing.Any]
    ) -> dict[str, typing.Any]:
        """Calculate the stats data for our process level data.

        :param data: The collected stats data to report on
        :type data: dict

        """
        # Read the timestamp without mutating the caller's data
        timestamp = data.get('timestamp')

        # Iterate through the last poll results
        stats = self.consumer_stats_counter()
        consumer_stats: dict[str, dict[str, typing.Any]] = {}
        for name in data.keys():
            if name == 'timestamp':
                continue
            consumer_stats[name] = self.consumer_stats_counter()
            consumer_stats[name]['processes'] = self.process_count(name)
            # Fold in counts retired from pruned dead processes so the
            # per-consumer totals stay monotonic for Prometheus deltas
            baseline = self.consumer_baselines.get(name, {})
            for proc in data[name].keys():
                for key in stats:
                    value = data[name][proc]['counts'].get(key, 0)
                    stats[key] += value
                    consumer_stats[name][key] += value
            for key in stats:
                value = baseline.get(key, 0)
                stats[key] += value
                consumer_stats[name][key] += value

        # Return a data structure that can be used in reporting out the stats
        stats['processes'] = len(self.active_processes())
        return {
            'last_poll': timestamp,
            'consumers': consumer_stats,
            'process_data': {
                name: procs
                for name, procs in data.items()
                if name != 'timestamp'
            },
            'counts': stats,
        }

    def check_process_counts(self) -> None:
        """Check for the minimum consumer process levels and start up new
        processes needed.

        """
        if not self.is_running:
            LOGGER.debug('Not checking process counts, not running')
            return
        if self.max_messages:
            LOGGER.debug(
                'Skipping process respawn (max_messages=%i)', self.max_messages
            )
            return
        LOGGER.debug('Checking minimum consumer process levels')
        for name in self.consumers:
            processes_needed = self.process_spawn_qty(name)
            if processes_needed:
                LOGGER.info(
                    'Need to spawn %i processes for %s', processes_needed, name
                )
                self.start_processes(name, processes_needed)

    def collect_results(self, data_values: dict[str, typing.Any]) -> None:
        """Receive the data from the consumers polled and process it.

        :param dict data_values: The poll data returned from the consumer
        :type data_values: dict

        """
        self.last_poll_results['timestamp'] = self.poll_data['timestamp']

        # Get the name and consumer name and remove it from what is reported
        consumer_name: str = data_values['consumer_name']
        del data_values['consumer_name']
        process_name: str = data_values['name']
        del data_values['name']

        # Forward per-message observations to Prometheus
        prometheus.observe(
            consumer_name,
            data_values.pop('durations', []),
            data_values.pop('message_ages', []),
            data_values.pop('custom_durations', {}),
            data_values.pop('custom_counters', {}),
            data_values.pop('custom_gauges', {}),
        )

        # Add it to our last poll global data
        if consumer_name not in self.last_poll_results:
            self.last_poll_results[consumer_name] = {}
        self.last_poll_results[consumer_name][process_name] = data_values

        # Calculate the stats
        self.stats = self.calculate_stats(self.last_poll_results)
        prometheus.update(self.stats)

    @staticmethod
    def consumer_keyword(counts: dict[str, typing.Any]) -> str:
        """Return consumer or consumers depending on the process count.

        :param dict counts: The count dictionary to use process count
        :rtype: str

        """
        return 'consumer' if counts['processes'] == 1 else 'consumers'

    @staticmethod
    def consumer_stats_counter() -> dict[str, int]:
        """Return a new consumer stats counter instance.

        :rtype: dict

        """
        return {
            process.Process.ACKED: 0,
            process.Process.CONSUMER_EXCEPTION: 0,
            process.Process.DROPPED: 0,
            process.Process.ERROR: 0,
            process.Process.MESSAGE_EXCEPTION: 0,
            process.Process.NACKED: 0,
            process.Process.PROCESSED: 0,
            process.Process.PROCESSING_EXCEPTION: 0,
            process.Process.REDELIVERED: 0,
            process.Process.REQUEUED: 0,
            process.Process.TIME_SPENT: 0,
            process.Process.UNHANDLED_EXCEPTION: 0,
        }

    def get_consumer_process(
        self, consumer: str, name: str
    ) -> process.Process | None:
        """Get the process object for the specified consumer and process name.

        :param str consumer: The consumer name
        :param str name: The process name
        :returns: multiprocessing.Process

        """
        return self.consumers[consumer].processes.get(name)

    @staticmethod
    def get_consumer_cfg(
        config: models.Config, only: str | None, qty: int | None
    ) -> dict[str, models.ConsumerConfig]:
        """Get the consumers config, possibly filtering the config if only
        or qty is set.

        :param config: The full application config
        :param str only: When set, filter to run only this consumer
        :param int qty: When set, set the consumer qty to this value
        :rtype: dict

        """
        consumers = dict(config.consumers)
        if only:
            for key in list(consumers.keys()):
                if key != only:
                    del consumers[key]
            if qty and only in consumers:
                consumers[only] = consumers[only].model_copy(
                    update={'qty': qty}
                )
        return consumers

    def is_dead(self, proc: psutil.Process, name: str) -> bool:
        """Checks to see if the specified process is dead.

        :param psutil.Process proc: The process to check
        :param str name: The name of consumer
        :rtype: bool

        """
        LOGGER.debug('Checking %s (%r)', name, proc)
        try:
            status = proc.status()
        except psutil.NoSuchProcess:
            LOGGER.debug('NoSuchProcess: %s (%r)', name, proc)
            return True

        LOGGER.debug(
            'Process %s (%s) status: %r (Unresponsive Count: %s)',
            name,
            proc.pid,
            status,
            self.unresponsive[name],
        )
        if status in _PROCESS_RUNNING:
            return False
        elif status == psutil.STATUS_ZOMBIE:
            try:
                proc.wait(0.1)
            except psutil.TimeoutExpired:
                pass
            try:
                proc.terminate()
                status = proc.status()
            except psutil.NoSuchProcess:
                LOGGER.debug('NoSuchProcess: %s (%r)', name, proc)
                return True
        return status in _PROCESS_STOPPED_OR_DEAD

    def kill_processes(self) -> None:
        """Gets called on shutdown by the timer when too much time has gone by,
        calling the terminate method instead of nicely asking for the consumers
        to stop.

        """
        LOGGER.critical('Max shutdown exceeded, forcibly exiting')
        processes = self.active_processes(False)
        while processes:
            for proc in self.active_processes(False):
                if proc.pid is None or proc.pid == os.getpid():
                    continue
                LOGGER.warning('Killing %s (%s)', proc.name, proc.pid)
                try:
                    os.kill(proc.pid, signal.SIGKILL)
                except OSError:
                    pass
            time.sleep(0.5)
            processes = self.active_processes(False)

        LOGGER.info('Killed all children')
        return self.set_state(self.STATE_STOPPED)

    def log_stats(self) -> None:
        """Output the stats to the LOGGER."""
        if not self.stats.get('counts'):
            LOGGER.info('Did not receive any stats data from children')
            return

        if self.poll_data['processes']:
            LOGGER.warning(
                '%i process(es) did not respond with stats: %r',
                len(self.poll_data['processes']),
                self.poll_data['processes'],
            )

        if self.stats['counts']['processes'] > 1:
            LOGGER.info(
                '%i consumers processed %i messages with %i errors',
                self.stats['counts']['processes'],
                self.stats['counts']['processed'],
                self.stats['counts']['failed'],
            )

        for key in self.stats['consumers'].keys():
            LOGGER.info(
                '%i %s %s processed %i messages with %i errors',
                self.stats['consumers'][key]['processes'],
                key,
                self.consumer_keyword(self.stats['consumers'][key]),
                self.stats['consumers'][key]['processed'],
                self.stats['consumers'][key]['failed'],
            )

    def new_consumer(
        self, config: models.ConsumerConfig, consumer_name: str
    ) -> Consumer:
        """Return a consumer dict for the given name and configuration.

        :param config: The consumer configuration
        :param str consumer_name: The consumer name
        :rtype: Consumer

        """
        return Consumer(0, {}, config.qty, config.queue or consumer_name)

    def new_process(self, consumer_name: str) -> tuple[str, process.Process]:
        """Create a new consumer instances

        :param str consumer_name: The name of the consumer
        :return tuple: (str, process.Process)

        """
        proc_num = self.new_process_number(consumer_name)
        process_name = f'{consumer_name}-{proc_num}'
        kwargs: dict[str, typing.Any] = {
            'config': self.config,
            'consumer_name': consumer_name,
            'profile': self.profile,
            'daemon': False,
            'stats_queue': self.stats_queue,
            'logging_config': self.config.logging,
            'max_messages': self.max_messages,
        }
        return process_name, process.Process(name=process_name, kwargs=kwargs)

    def new_process_number(self, name: str) -> int:
        """Increment the counter for the process id number for a given consumer
        configuration.

        :param str name: Consumer name
        :rtype: int

        """
        self.consumers[name].last_proc_num += 1
        return self.consumers[name].last_proc_num

    def on_sigchld(
        self, _signum: int, _unused_frame: types.FrameType | None
    ) -> None:
        """Invoked when a child sends up an SIGCHLD signal.

        :param int _signum: The signal that was invoked
        :param frame _unused_frame: The frame that was interrupted

        """
        LOGGER.info('SIGCHLD received from child')

        # active_processes prunes any children that have exited
        if self.active_processes(False):
            return

        # Only tear the daemon down when we are not meant to keep running:
        # a child failed to spawn, max_messages mode has drained, or we are
        # already shutting down. Otherwise let the next poll respawn.
        if self.child_abort or self.max_messages or self.is_shutting_down:
            LOGGER.info('Stopping with no active processes')
            signal.setitimer(signal.ITIMER_REAL, 0, 0)
            self.set_state(self.STATE_STOPPED)
        else:
            LOGGER.info('All children exited; next poll will respawn')

    def on_timer(
        self, _signum: int, _unused_frame: types.FrameType | None
    ) -> None:
        """Invoked by the Poll timer signal.

        :param int _signum: The signal that was invoked
        :param frame _unused_frame: The frame that was interrupted

        """
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

    def poll(self) -> None:
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
        self.poll_data = {'timestamp': time.time(), 'processes': []}

        # Iterate through all of the consumers
        for proc in list(self.active_processes()):
            if proc == multiprocessing.current_process():
                continue

            # Send the profile signal
            if proc.pid is None:
                continue
            try:
                os.kill(proc.pid, signal.SIGPROF)
            except ProcessLookupError as error:
                LOGGER.warning(
                    'Error sending SIGPROF to %s: %s', proc.pid, error
                )
            else:
                self.poll_data['processes'].append(proc.name)

        # Check if we need to start more processes
        self.check_process_counts()

    @property
    def poll_duration_exceeded(self) -> bool:
        """Return true if the poll time has been exceeded.
        :rtype: bool

        """
        return bool(
            (time.time() - self.poll_data['timestamp']) >= self.poll_interval
        )

    def poll_results_check(self) -> None:
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
            LOGGER.warning(
                'Did not receive results from %r', self.poll_data['processes']
            )

    def process(self, name: str, process_name: str) -> process.Process:
        """Return the process handle for the given consumer name and process
        name.

        :param str name: The consumer name from config
        :param str process_name: The automatically assigned process name
        :rtype: rejected.process.Process

        """
        return self.consumers[name].processes[process_name]

    def process_count(self, name: str) -> int:
        """Return the process count for the given consumer name.

        :param str name: The consumer name
        :rtype: int

        """
        return len(self.consumers[name].processes)

    def process_spawn_qty(self, name: str) -> int:
        """Return the number of processes to spawn for the given consumer name.

        :param str name: The consumer name
        :rtype: int

        """
        return self.consumers[name].qty - self.process_count(name)

    def retire_poll_results(self, consumer: str, name: str) -> None:
        """Fold a dead process's final counts into the consumer baseline and
        drop its per-process poll entry so ``last_poll_results`` stays bounded
        while the per-consumer totals remain monotonic for Prometheus deltas.

        :param str consumer: The consumer name
        :param str name: The process name

        """
        proc_results = self.last_poll_results.get(consumer, {}).pop(name, None)
        if not proc_results:
            return
        baseline = self.consumer_baselines.setdefault(
            consumer, collections.Counter()
        )
        baseline.update(proc_results.get('counts', {}))

    def remove_consumer_process(self, consumer: str, name: str) -> None:
        """Remove all details for the specified consumer and process name.

        :param str consumer: The consumer name
        :param str name: The process name

        """
        my_pid = os.getpid()
        self.retire_poll_results(consumer, name)
        if name in self.consumers[consumer].processes.keys():
            try:
                child = self.consumers[consumer].processes[name]
            except KeyError:
                return
            try:
                alive = child.is_alive()
            except AssertionError:
                LOGGER.debug(
                    'Tried to test non-child process (%r to %r)',
                    os.getpid(),
                    child.pid,
                )
            else:
                if child.pid == my_pid:
                    LOGGER.debug('Child has my pid? %r, %r', my_pid, child.pid)
                elif alive:
                    try:
                        child.terminate()
                    except OSError:
                        pass
            try:
                del self.consumers[consumer].processes[name]
            except KeyError:
                pass

    def run(self) -> None:
        """When the consumer is ready to start running, kick off all of our
        consumer consumers and then loop while we process messages.

        """
        self.set_state(self.STATE_ACTIVE)
        self.setup_consumers()

        if self.config.stats.prometheus.enabled:
            prometheus.start(self.config.stats.prometheus.port)

        # Set the SIGCHLD handler for child creation errors
        signal.signal(signal.SIGCHLD, self.on_sigchld)

        # Set the SIGALRM handler for poll interval
        signal.signal(signal.SIGALRM, self.on_timer)

        # Kick off the poll timer
        signal.setitimer(signal.ITIMER_REAL, self.poll_interval, 0)

        # Loop for the lifetime of the app. Use a bounded sleep rather than
        # signal.pause() so a stop request or signal delivered between the
        # loop check and the wait cannot wedge us forever (lost wakeup).
        while self.is_running and not self.stop_requested:
            if not self.is_sleeping:
                self.set_state(self.STATE_SLEEPING)
            time.sleep(1)

        # Note we're exiting run
        LOGGER.info('Exiting Master Control Program')

    @staticmethod
    def set_process_name() -> None:
        """Set the process name for the top level process so that it shows up
        in logs in a more trackable fashion.

        """
        proc = multiprocessing.current_process()
        for offset in range(0, len(sys.argv)):
            if sys.argv[offset] == '-c':
                name = sys.argv[offset + 1].split('/')[-1]
                proc.name = name.split('.')[0]
                break

    def set_timer(self, duration: float) -> None:
        """Setup the next alarm to fire and then wait for it to fire.

        :param int duration: How long to sleep

        """
        # Make sure that the application is not shutting down before sleeping
        if self.is_shutting_down:
            LOGGER.debug('Not sleeping, application is trying to shutdown')
            return

        # Set the signal timer
        signal.setitimer(signal.ITIMER_REAL, duration, 0)

    def setup_consumers(self) -> None:
        """Iterate through each consumer in the configuration and kick off the
        minimal amount of processes, setting up the runtime data as well.

        """
        for name in self.consumer_cfg.keys():
            self.consumers[name] = self.new_consumer(
                self.consumer_cfg[name], name
            )
            self.start_processes(name, self.consumers[name].qty)

    def start_process(self, name: str) -> None:
        """Start a new consumer process for the given consumer name

        :param str name: The consumer name

        """
        process_name, proc = self.new_process(name)
        LOGGER.info('Spawning %s process for %s', process_name, name)

        # Append the process to the consumer process list
        self.consumers[name].processes[process_name] = proc

        # Start the process
        try:
            proc.start()
        except OSError as error:
            LOGGER.critical(
                'Failed to start %s for %s: %r', process_name, name, error
            )
            self.child_abort = True
            try:
                del self.consumers[name].processes[process_name]
            except AttributeError as error:
                LOGGER.warning('Could not cleanup consumer process: %s', error)

    def start_processes(self, name: str, quantity: int) -> None:
        """Start the specified quantity of consumer processes for the given
        consumer.

        :param str name: The consumer name
        :param int quantity: The quantity of processes to start

        """
        if not self.is_running:
            LOGGER.debug('Not starting processes, not running')
            return
        for _i in range(0, quantity or 0):
            self.start_process(name)

    def stop_processes(self) -> None:
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
            if proc.pid is not None and proc.pid != os.getpid():
                try:
                    os.kill(proc.pid, signal.SIGABRT)
                except OSError:
                    pass

        # Wait for them to finish up to MAX_SHUTDOWN_WAIT
        for iteration in range(0, self.MAX_SHUTDOWN_WAIT):
            processes = len(self.active_processes(False))
            if not processes:
                break

            LOGGER.info(
                'Waiting on %i active processes to shut down (%i/%i)',
                processes,
                iteration,
                self.MAX_SHUTDOWN_WAIT,
            )
            try:
                time.sleep(0.5)
            except KeyboardInterrupt:
                break

        if len(self.active_processes(False)):
            self.kill_processes()

        LOGGER.debug('All consumer processes stopped')
        self.set_state(self.STATE_STOPPED)

    @property
    def total_process_count(self) -> int:
        """Returns the active consumer process count

        :rtype: int

        """
        return len(self.active_processes(False))
