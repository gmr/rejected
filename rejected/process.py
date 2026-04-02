"""
Consumer process management. Imports consumer code, manages RabbitMQ
connection state and collects stats about the consuming process.

"""

import asyncio
import collections
import logging
import logging.config
import math
import multiprocessing
import os
import profile
import signal
import time
import typing
from os import path

try:
    import sentry_sdk
    from sentry_sdk import logging as sentry_logging
except ImportError:
    sentry_sdk, sentry_logging = None, None

from . import __version__, connection, models, state, statsd, utils

LOGGER = logging.getLogger(__name__)


class Process(multiprocessing.Process, state.State):
    """Core process class that manages the consumer object and communicates
    with RabbitMQ.

    """

    AMQP_APP_ID = f'rejected/{__version__}'

    # Additional State constants
    STATE_PROCESSING = 0x04
    STATES: typing.ClassVar[dict[int, str]] = {
        **state.State.STATES,
        STATE_PROCESSING: 'Processing',
    }

    # Counter constants
    ACKED = 'acked'
    CLOSED_ON_COMPLETE = 'closed_on_complete'
    DROPPED = 'dropped'
    ERROR = 'failed'
    FAILURES = 'failures_until_stop'
    NACKED = 'nacked'
    PROCESSED = 'processed'
    REQUEUED = 'requeued'
    REDELIVERED = 'redelivered'
    TIME_SPENT = 'processing_time'
    TIME_WAITED = 'idle_time'

    CONSUMER_EXCEPTION = 'consumer_exception'
    MESSAGE_EXCEPTION = 'message_exception'
    PROCESSING_EXCEPTION = 'processing_exception'
    UNHANDLED_EXCEPTION = 'unhandled_exception'

    QOS_PREFETCH_COUNT = 1
    MAX_ERROR_COUNT = 5
    MAX_ERROR_WINDOW = 60
    MAX_SHUTDOWN_WAIT = 5

    def __init__(
        self, group=None, target=None, name=None, args=(), kwargs=None
    ):
        if kwargs is None:
            kwargs = {}
        super().__init__(group, target, name, args, kwargs)
        self.active_message = None
        self.callbacks = models.Callbacks(
            on_ready=self.on_connection_ready,
            on_connection_failure=self.on_connection_failure,
            on_closed=self.on_connection_closed,
            on_blocked=self.on_connection_blocked,
            on_unblocked=self.on_connection_unblocked,
            on_confirmation=self.on_confirmation,
            on_delivery=self.on_delivery,
            on_return=self.on_returned,
        )
        self.connections = {}
        self.consumer = None
        self.consumer_lock = None
        self.consumer_version = None
        self.counters = collections.Counter()

        self.delivery_time = None
        self.last_failure = 0
        self.last_stats_time = None
        self.measurement = None
        self.message_connection_id = None
        self.pending = collections.deque()
        self.prepend_path = None
        self.previous = None
        self._duration_observations: list[float] = []
        self._message_age_observations: list[float] = []
        self._custom_durations: dict[str, list[float]] = {}
        self._custom_counters: dict[str, int] = {}
        self._custom_gauges: dict[str, float] = {}
        self.sentry_client = None
        self.state = self.STATE_INITIALIZING
        self._tasks: set[asyncio.Task] = set()
        self.state_start = time.time()
        self.statsd = None

    def ack_message(self, message: models.Message) -> None:
        """Acknowledge the message on the broker and log the ack

        :param message: The message to acknowledge
        :type message: rejected.data.Message

        """
        if not self.connections[message.connection].is_running:
            LOGGER.warning('Can not ack message, disconnected from RabbitMQ')
            self.counters[self.CLOSED_ON_COMPLETE] += 1
            self.connections[message.connection].shutdown()
            return

        LOGGER.debug('Acking %s', message.delivery_tag)
        message.channel.basic_ack(delivery_tag=message.delivery_tag)
        self.counters[self.ACKED] += 1
        self.measurement.set_tag(self.ACKED, True)

    def calc_velocity(self, values):
        """Return the message consuming velocity for the process.

        :param dict values: The dict with velocity data
        :rtype: float

        """
        processed = values['counts'].get(self.PROCESSED, 0) - values[
            'previous'
        ].get(self.PROCESSED, 0)
        duration = time.time() - self.last_stats_time

        # If there were no messages, do not calculate, use the base
        if not processed or not duration:
            return 0

        # Calculate the velocity as the basis for the calculation
        velocity = float(processed) / float(duration)
        LOGGER.debug('Message processing velocity: %.2f/s', velocity)
        return velocity

    def create_connections(self):
        """Create and start the RabbitMQ connections, assigning the connection
        object to the connections dict.

        """
        self.set_state(self.STATE_CONNECTING)
        for conn in self.consumer_config.connections:
            name = conn.name
            confirm = conn.confirm

            name, confirm, consume = conn, False, True
            if isinstance(conn, models.ConnectionRef):
                name = conn.name
                confirm = conn.confirm
                consume = conn.consume

            if name not in self.config.connections:
                LOGGER.critical(
                    'Connection "%s" for %s not found',
                    name,
                    self.consumer_name,
                )
                continue

            self.connections[name] = connection.Connection(
                name,
                self.config.connections[name],
                self.consumer_name,
                consume,
                confirm,
                self.callbacks,
            )

    @staticmethod
    def get_config(cfg, number, name, connection):
        """Initialize a new consumer thread, setting defaults and config values

        :param dict cfg: Consumer config section from YAML File
        :param int number: The identification number for the consumer
        :param str name: The name of the consumer
        :param str connection: The name of the connection):
        :rtype: dict

        """
        return {
            'connection': cfg.connections[connection],
            'consumer_name': name,
            'process_name': f'{name}_{os.getpid()}_tag_{number}',
        }

    def get_consumer(self, cfg):
        """Import and create a new instance of the configured message consumer.

        :param dict cfg: The named consumer section of the configuration
        :rtype: instance
        :raises: ImportError

        """
        try:
            handle, version = utils.import_consumer(cfg.consumer)
        except ImportError as error:
            LOGGER.exception(
                'Error importing the consumer %s: %s', cfg.consumer, error
            )
            return

        if version:
            LOGGER.info('Creating consumer %s v%s', cfg.consumer, version)
            self.consumer_version = version
        else:
            LOGGER.info('Creating consumer %s', cfg.consumer)

        settings = dict(cfg.config)
        settings['_import_module'] = '.'.join(cfg.consumer.split('.')[0:-1])

        kwargs = {
            'settings': config_module.Settings(settings),
            'process': self,
            'drop_exchange': cfg.drop_exchange,
            'drop_invalid_messages': cfg.drop_invalid_messages,
            'message_type': cfg.message_type,
            'error_exchange': cfg.error_exchange,
            'error_max_retry': cfg.error_max_retry,
        }

        try:
            return handle(**kwargs)
        except Exception as error:
            LOGGER.exception(
                'Error creating the consumer "%s": %s', cfg.consumer, error
            )

    async def invoke_consumer(self, message):
        """Wrap the actual processor processing bits

        :param rejected.data.Message message: The message to process

        """
        # Only allow for a single message to be processed at a time
        async with self.consumer_lock:
            if self.is_idle:
                self.set_state(self.STATE_PROCESSING)
                self.delivery_time = time.time()
                start_time = time.monotonic()
                self.active_message = message

                self.measurement = data.Measurement()

                if message.method.redelivered:
                    self.counters[self.REDELIVERED] += 1
                    self.measurement.set_tag(self.REDELIVERED, True)

                try:
                    result = await self.consumer.execute(
                        message, self.measurement
                    )
                except Exception as error:
                    LOGGER.exception(
                        'Unhandled exception from consumer in '
                        'process. This should not happen. %s',
                        error,
                    )
                    result = data.MESSAGE_REQUEUE

                LOGGER.debug('Finished processing message: %r', result)
                self.on_processed(message, result, start_time)
            elif self.is_waiting_to_shutdown:
                LOGGER.info(
                    'Requeueing pending message due to pending shutdown'
                )
                self.reject(message, True)
                self.shutdown_connections()
            elif self.is_shutting_down:
                LOGGER.info('Requeueing pending message due to shutdown')
                self.reject(message, True)
                self.on_ready_to_stop()
            else:
                LOGGER.warning(
                    'Exiting invoke_consumer without processing, '
                    'this should not happen. State: %s',
                    self.state_description,
                )
        if self.pending:
            self._schedule(self.invoke_consumer(self.pending.popleft()))

    def _schedule(self, coro):
        """Schedule a coroutine as a fire-and-forget task, keeping a reference
        to prevent it from being garbage-collected before completion.
        """
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    @property
    def is_processing(self):
        """Returns a bool specifying if the consumer is currently processing

        :rtype: bool

        """
        return self.state in [self.STATE_PROCESSING, self.STATE_STOP_REQUESTED]

    def maybe_submit_measurement(self):
        """Check for configured instrumentation backends and if found, submit
        the message measurement info.

        """
        if self.statsd:
            self.submit_statsd_measurements()

    def on_connection_closed(self, name):
        if self.is_running:
            LOGGER.warning('Connection %s was closed, reconnecting', name)
            return self.connections[name].connect()

        ready = all(c.is_closed for c in self.connections.values())
        if (self.is_shutting_down or self.is_waiting_to_shutdown) and ready:
            self.on_ready_to_stop()

    def on_connection_failure(self, *args, **kwargs):
        ready = all(c.is_closed for c in self.connections.values())
        LOGGER.warning(
            'Connection failure while %s - Ready to stop: %r',
            self.state_description,
            ready,
        )
        if (
            self.is_connecting
            or self.is_idle
            or self.is_shutting_down
            or self.is_waiting_to_shutdown
        ) and ready:
            self.on_ready_to_stop()

    def on_connection_ready(self, name):
        LOGGER.debug('Connection %s indicated it is ready', name)
        self.consumer.set_channel(name, self.connections[name].channel)
        if all(c.is_idle for c in self.connections.values()):
            for key in self.connections.keys():
                if self.connections[key].should_consume:
                    self.connections[key].consume(
                        self.queue_name, self.no_ack, self.qos_prefetch
                    )
            if self.is_connecting:
                self.set_state(self.STATE_IDLE)

    def on_connection_blocked(self, name):
        LOGGER.warning('Connection %s blocked', name)
        if self.is_processing:
            self._schedule(self.consumer.on_blocked(name))

    def on_connection_unblocked(self, name):
        LOGGER.info('Connection %s unblocked', name)
        if self.is_processing:
            self._schedule(self.consumer.on_unblocked(name))

    def on_confirmation(self, name, delivered, delivery_tag):
        """Invoked on delivery confirmation

        :param str name: The RabbitMQ connection that confirmed the delivery
        :param bool delivered: Was the message was successfully delivered
        :param str delivery_tag: The delivery tag for the message

        """
        if self.is_processing:
            self.consumer.on_confirmation(name, delivered, delivery_tag)

    def on_delivery(self, name, channel, method, properties, body):
        """Process a message from Rabbit

        :param str name: The connection name
        :param pika.channel.Channel channel: The message's delivery channel
        :param pika.frames.MethodFrame method: The method frame
        :param pika.spec.BasicProperties properties: The message properties
        :param str body: The message body

        """
        message = models.Message(name, channel, method, properties, body)
        if self.is_processing:
            self.pending.append(message)
        else:
            self._schedule(self.invoke_consumer(message))

    def on_returned(self, name, channel, method, properties, body):
        """Send a message to the consumer that was returned by RabbitMQ

        :param str name: The connection name
        :param channel: The channel the message was returned on
        :type channel: pika.channel.Channel channel:
        :param pika.frames.MethodFrame method: The method frame
        :param pika.spec.BasicProperties properties: The message properties
        :param str body: The message body

        """
        message = data.Message(
            name, channel, method, properties, body, returned=True
        )
        if self.is_processing:
            self.pending.append(message)
        else:
            self._schedule(self.invoke_consumer(message))

    def on_processed(self, message, result, start_time):
        """Invoked after a message is processed by the consumer and
        implements the logic for how to deal with a message based upon
        the result.

        :param rejected.data.Message message: The message that was processed
        :param int result: The result of the processing of the message
        :param float start_time: When the message was received

        """
        duration = time.monotonic() - start_time
        self.counters[self.TIME_SPENT] += duration
        self.measurement.add_duration(self.TIME_SPENT, duration)
        self._duration_observations.append(duration)

        if result == data.MESSAGE_DROP:
            LOGGER.debug('Rejecting message due to drop return from consumer')
            self.reject(message, False)
            self.counters[self.DROPPED] += 1

        elif result == data.MESSAGE_EXCEPTION:
            LOGGER.debug('Rejecting message due to MessageException')
            self.reject(message, False)
            self.counters[self.MESSAGE_EXCEPTION] += 1

        elif result == data.PROCESSING_EXCEPTION:
            LOGGER.debug('Rejecting message due to ProcessingException')
            if self.consumer.ACK_PROCESSING_EXCEPTIONS:
                self.ack_message(message)
            else:
                self.reject(message, False)
            self.counters[self.PROCESSING_EXCEPTION] += 1

        elif result == data.CONSUMER_EXCEPTION:
            LOGGER.debug('Re-queueing message due to ConsumerException')
            self.reject(message, True)
            self.on_processing_error()
            self.counters[self.CONSUMER_EXCEPTION] += 1

        elif result == data.UNHANDLED_EXCEPTION:
            LOGGER.debug('Re-queueing message due to UnhandledException')
            self.reject(message, True)
            self.on_processing_error()
            self.counters[self.UNHANDLED_EXCEPTION] += 1

        elif result == data.MESSAGE_REQUEUE:
            LOGGER.debug('Re-queueing message due Consumer request')
            self.reject(message, True)
            self.counters[self.REQUEUED] += 1

        elif result == data.MESSAGE_ACK and not self.no_ack:
            self.ack_message(message)

        self.counters[self.PROCESSED] += 1
        self.measurement.set_tag(self.PROCESSED, True)
        if message.properties.timestamp:
            age = time.time() - message.properties.timestamp
            if age > 0:
                self._message_age_observations.append(age)
        self._collect_custom_measurements()
        self.maybe_submit_measurement()
        self.reset_state()

    def on_processing_error(self):
        """Called when message processing failure happens due to a
        ConsumerException or an unhandled exception.

        """
        duration = time.time() - self.last_failure
        if duration > self.MAX_ERROR_WINDOW:
            LOGGER.info(
                'Resetting failure window, %i seconds since last', duration
            )
            self.reset_error_counter()
        self.counters[self.ERROR] += 1
        self.last_failure = time.time()
        if self.too_many_errors:
            LOGGER.critical(
                'Error threshold exceeded (%i), shutting down',
                self.counters[self.ERROR],
            )
            self.shutdown_connections()

    def on_ready_to_stop(self):
        """Invoked when the consumer is ready to stop."""
        LOGGER.debug('Ready to stop')

        # Set the state to shutting down if it wasn't set as that during loop
        self.set_state(self.STATE_SHUTTING_DOWN)

        # Reset any signal handlers
        signal.signal(signal.SIGABRT, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGPROF, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)

        # Allow the consumer to gracefully stop and then stop the IOLoop
        if self.consumer:
            self.stop_consumer()

        # Clear IOLoop constructs
        self.consumer_lock = None

        # Stop the event loop
        if self.ioloop:
            LOGGER.debug('Stopping event loop')
            self.ioloop.stop()

        # Note that shutdown is complete and set the state accordingly
        self.set_state(self.STATE_STOPPED)
        LOGGER.info('Shutdown complete')

    def on_sigprof(self, _unused_signum, _unused_frame):
        """Called when SIGPROF is sent to the process, will dump the stats, in
        future versions, queue them for the master process to get data.

        :param int _unused_signum: The signal number
        :param frame _unused_frame: The python frame the signal was received at

        """
        self.stats_queue.put(self.report_stats(), True)
        self.last_stats_time = time.time()
        signal.siginterrupt(signal.SIGPROF, False)

    def on_startup_error(self, error):
        """Invoked when a pre-condition for starting the consumer has failed.
        Log the error and then exit the process.

        """
        LOGGER.critical('Could not start %s: %s', self.consumer_name, error)
        self.set_state(self.STATE_STOPPED)

    def reject(self, message, requeue=True):
        """Reject the message on the broker and log it.

        :param message: The message to reject
        :type message: rejected.Data.message
        :param bool requeue: Specify if the message should be re-queued or not

        """
        if self.no_ack:
            raise RuntimeError('Can not rejected messages when ack is False')

        if not self.connections[message.connection].is_running:
            LOGGER.warning('Can not nack message, disconnected from RabbitMQ')
            self.counters[self.CLOSED_ON_COMPLETE] += 1
            self.connections[message.connection].shutdown()
            return

        LOGGER.warning(
            'Rejecting message %s %s requeue',
            message.delivery_tag,
            'with' if requeue else 'without',
        )
        message.channel.basic_nack(
            delivery_tag=message.delivery_tag, requeue=requeue
        )
        self.measurement.set_tag(self.NACKED, True)
        self.measurement.set_tag(self.REQUEUED, requeue)

    def _collect_custom_measurements(self):
        """Accumulate per-message Measurement data for Prometheus."""
        if not self.measurement:
            return
        # Custom durations (excluding processing_time, already tracked)
        for key, values in self.measurement.durations.items():
            if key == self.TIME_SPENT:
                continue
            self._custom_durations.setdefault(key, []).extend(values)
        # Custom counters
        for key, value in self.measurement.counters.items():
            self._custom_counters[key] = (
                self._custom_counters.get(key, 0) + value
            )
        # Custom gauges (values dict on Measurement)
        for key, value in self.measurement.values.items():
            self._custom_gauges[key] = value

    def report_stats(self):
        """Create the dict of stats data for the MCP stats queue"""
        if not self.previous:
            self.previous = {}
            for key in self.counters:
                self.previous[key] = 0
        values = {
            'name': self.name,
            'consumer_name': self.consumer_name,
            'counts': dict(self.counters),
            'previous': dict(self.previous),
            'durations': list(self._duration_observations),
            'message_ages': list(self._message_age_observations),
            'custom_durations': {
                k: list(v) for k, v in self._custom_durations.items()
            },
            'custom_counters': dict(self._custom_counters),
            'custom_gauges': dict(self._custom_gauges),
        }
        self.previous = dict(self.counters)
        self._duration_observations.clear()
        self._message_age_observations.clear()
        self._custom_durations.clear()
        self._custom_counters.clear()
        self._custom_gauges.clear()
        return values

    def reset_error_counter(self):
        """Reset the error counter to 0"""
        LOGGER.debug('Resetting the error counter')
        self.counters[self.ERROR] = 0

    def reset_state(self):
        """Reset the runtime state after processing a message to either idle
        or shutting down based upon the current state.

        """
        self.active_message = None
        self.measurement = None
        if self.is_waiting_to_shutdown:
            self.set_state(self.STATE_SHUTTING_DOWN)
            self.shutdown_connections()
        elif self.is_processing:
            self.set_state(self.STATE_IDLE)
        elif self.is_idle or self.is_connecting or self.is_shutting_down:
            pass
        else:
            LOGGER.critical('Unexepected state: %s', self.state_description)
        LOGGER.debug(
            'State reset to %s (%s in pending)',
            self.state_description,
            len(self.pending),
        )

    def run(self):
        """Start the consumer"""
        if self.profile_file:
            LOGGER.info('Profiling to %s', self.profile_file)
            profile.runctx(
                'self._run()', globals(), locals(), self.profile_file
            )
        else:
            self._run()
        LOGGER.debug(
            'Exiting %s (%i, %i)', self.name, os.getpid(), os.getppid()
        )

    def _run(self):
        """Run method that can be profiled"""
        self.set_state(self.STATE_INITIALIZING)
        self.ioloop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.ioloop)
        self.consumer_lock = asyncio.Lock()

        try:
            self.setup()
        except (AttributeError, ImportError) as error:
            LOGGER.exception('Setup failure: %s', error)
            return self.on_startup_error(
                f'Failed to import the Python module for {self.consumer_name}'
            )

        self.sentry_client = self.setup_sentry(
            self._kwargs['config'], self.consumer_name
        )

        if not self.is_stopped:
            try:
                self.ioloop.run_forever()
            except KeyboardInterrupt:
                LOGGER.warning('CTRL-C while waiting for clean shutdown')

    def send_exception_to_sentry(self, exc_info):
        """Send an exception to Sentry if enabled.

        :param tuple exc_info: exception information as returned from
            :func:`sys.exc_info`

        """
        if not self.sentry_client:
            LOGGER.debug('No sentry_client, aborting')
            return

        message = dict(self.active_message)
        try:
            duration = math.ceil(time.time() - self.delivery_time) * 1000
        except TypeError:
            duration = 0
        LOGGER.debug('Sending exception to sentry')
        with sentry_sdk.new_scope() as scope:
            scope.set_extra('consumer_name', self.consumer_name)
            scope.set_extra(
                'env',
                {
                    k: v
                    for k, v in os.environ.items()
                    if not any(
                        s in k.upper()
                        for s in (
                            'KEY',
                            'SECRET',
                            'TOKEN',
                            'PASSWORD',
                            'DSN',
                            'CREDENTIAL',
                            'AUTH',
                            'PRIVATE',
                        )
                    )
                },
            )
            scope.set_extra('message', message)
            scope.set_extra('time_spent', duration)
            sentry_sdk.capture_exception(exc_info, scope=scope)

    def setup(self):
        """Initialize the consumer, setting up needed attributes and connecting
        to RabbitMQ.

        """
        if self.logging_config:
            logging.config.dictConfig(self.logging_config)
        LOGGER.info('Initializing for %s', self.name)
        if not self.consumer_config.consumer:
            return self.on_startup_error(
                '"consumer" not specified in configuration'
            )

        self.consumer = self.get_consumer(self.consumer_config)

        if not self.consumer:
            return self.on_startup_error(
                'Could not import "{}"'.format(
                    self.consumer_config.consumer or 'unconfigured consumer'
                )
            )

        self.setup_instrumentation()
        self.reset_error_counter()
        self.setup_sighandlers()
        self.create_connections()

    def setup_instrumentation(self):
        """Configure statsd instrumentation for per-message measurements."""
        if self.config.stats.statsd.enabled:
            self.statsd = statsd.Client(
                self.consumer_name,
                self.config.stats.statsd.model_dump(),
                self.stop,
            )
            LOGGER.debug('statsd measurements configured')

    def setup_sentry(self, cfg, consumer_name):
        # Setup Sentry if configured and sentry_sdk is installed
        sentry_dsn = self.consumer_config.sentry_dsn or cfg.sentry_dsn
        if not sentry_sdk or not sentry_dsn:
            return False
        kwargs = {
            'dsn': sentry_dsn,
            'send_default_pii': False,
            'ignore_errors': [
                'rejected.consumer.ConsumerException',
                'rejected.consumer.MessageException',
                'rejected.consumer.ProcessingException',
            ],
            'integrations': [
                sentry_logging.LoggingIntegration(level=None, event_level=None)
            ],
        }
        if os.environ.get('ENVIRONMENT'):
            kwargs['environment'] = os.environ['ENVIRONMENT']
        if self.consumer_version:
            kwargs['release'] = self.consumer_version
        sentry_sdk.init(**kwargs)
        return True

    def setup_sighandlers(self):
        """Setup the stats and stop signal handlers."""
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)

        signal.signal(signal.SIGPROF, self.on_sigprof)
        signal.signal(signal.SIGABRT, self.stop)

        signal.siginterrupt(signal.SIGPROF, False)
        signal.siginterrupt(signal.SIGABRT, False)
        LOGGER.debug('Signal handlers setup')

    def shutdown_connections(self):
        """This method closes the connections to RabbitMQ."""
        if not self.is_shutting_down:
            self.set_state(self.STATE_SHUTTING_DOWN)
        for name in self.connections:
            if self.connections[name].is_running:
                self.connections[name].shutdown()

    def stop(self, signum=None, _unused=None):
        """Stop the consumer from consuming by calling BasicCancel and setting
        our state.

        :param int signum: The signal received
        :param frame _unused: The stack frame from when the signal was called

        """
        LOGGER.debug('Stop called in state: %s', self.state_description)
        if self.is_stopped:
            LOGGER.warning('Stop requested but consumer is already stopped')
            return
        elif self.is_shutting_down:
            LOGGER.warning('Stop requested, consumer is already shutting down')
            return
        elif self.is_waiting_to_shutdown:
            LOGGER.warning('Stop requested but already waiting to shut down')
            return

        # Stop consuming and close AMQP connections
        self.shutdown_connections()

        # Wait until the consumer has finished processing to shutdown
        if self.is_processing:
            LOGGER.info('Waiting for consumer to finish processing')
            self.set_state(self.STATE_STOP_REQUESTED)
            if signum == signal.SIGTERM:
                signal.siginterrupt(signal.SIGTERM, False)
            return

    def stop_consumer(self):
        """Stop the consumer object and allow it to do a clean shutdown if it
        has the ability to do so.

        """
        try:
            LOGGER.info('Shutting down the consumer')
            self.consumer.shutdown()
        except AttributeError:
            LOGGER.debug('Consumer does not have a shutdown method')

    def submit_statsd_measurements(self):
        """Submit a measurement for a message to statsd as individual items."""
        for key, value in self.measurement.counters.items():
            self.statsd.incr(key, value)
        for key, values in self.measurement.durations.items():
            for value in values:
                self.statsd.add_timing(key, value)
        for key, value in self.measurement.values.items():
            self.statsd.set_gauge(key, value)
        for key, value in self.measurement.tags.items():
            if isinstance(value, bool):
                if value:
                    self.statsd.incr(key)
            elif isinstance(value, str):
                if value:
                    self.statsd.incr(f'{key}.{value}')
            elif isinstance(value, int):
                self.statsd.incr(key, value)
            else:
                LOGGER.warning(
                    'The %s value type of %s is unsupported', key, type(value)
                )

    @property
    def active_consumers(self):
        return len(
            [
                c
                for c in self.connections.values()
                if c.should_consume and c.is_active()
            ]
        )

    @property
    def config(self):
        return self._kwargs['config']

    @property
    def consumer_config(self):
        return self.config.consumers.get(
            self.consumer_name, config_module.ConsumerConfig()
        )

    @property
    def consumer_name(self):
        return self._kwargs['consumer_name']

    @property
    def expected_consumers(self):
        return len([c for c in self.connections.values() if c.should_consume])

    @property
    def logging_config(self):
        return self._kwargs['logging_config']

    @property
    def max_error_count(self):
        return int(self.consumer_config.max_errors)

    @property
    def no_ack(self):
        return not self.consumer_config.ack

    @property
    def profile_file(self):
        """Return the full path to write the cProfile data

        :return: str

        """
        if not self._kwargs['profile']:
            return None
        if os.path.exists(self._kwargs['profile']) and os.path.isdir(
            self._kwargs['profile']
        ):
            return (
                f'{path.normpath(self._kwargs["profile"])}'
                f'/{os.getpid()}-{self._kwargs["consumer_name"]}.prof'
            )
        return None

    @property
    def qos_prefetch(self):
        """Return the base, configured QoS prefetch value.

        :rtype: int

        """
        return self.consumer_config.qos_prefetch

    @property
    def queue_name(self):
        return self.consumer_config.queue or self.consumer_name

    @property
    def stats_queue(self):
        return self._kwargs['stats_queue']

    @property
    def too_many_errors(self):
        """Return a bool if too many errors have occurred.

        :rtype: bool

        """
        return self.counters[self.ERROR] >= self.max_error_count
