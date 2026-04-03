"""
Consumer process management. Imports consumer code, manages RabbitMQ
connection state and collects stats about the consuming process.

"""

import asyncio
import datetime
import logging
import logging.config
import math
import multiprocessing
import os
import profile
import signal
import time
import types
import typing
from os import path

import pika
import pika.channel
import pika.spec

try:
    import sentry_sdk
    from sentry_sdk import logging as sentry_logging
except ImportError:
    sentry_sdk, sentry_logging = None, None

from . import (
    __version__,
    codecs,
    connection,
    measurement,
    models,
    state,
    statsd,
    utils,
)
from . import config as config_module

LOGGER = logging.getLogger(__name__)


class Process(multiprocessing.Process, state.State):
    """Core process class that manages the consumer object and communicates
    with RabbitMQ.

    """

    AMQP_APP_ID: typing.ClassVar[str] = f'rejected/{__version__}'

    # Additional State constants
    STATE_PROCESSING: typing.ClassVar[int] = 0x09
    STATES: typing.ClassVar[dict[int, str]] = {
        **state.State.STATES,
        STATE_PROCESSING: 'Processing',
    }

    # Counter constants
    ACKED: typing.ClassVar[str] = 'acked'
    CLOSED_ON_COMPLETE: typing.ClassVar[str] = 'closed_on_complete'
    DROPPED: typing.ClassVar[str] = 'dropped'
    ERROR: typing.ClassVar[str] = 'failed'
    FAILURES: typing.ClassVar[str] = 'failures_until_stop'
    NACKED: typing.ClassVar[str] = 'nacked'
    PROCESSED: typing.ClassVar[str] = 'processed'
    REQUEUED: typing.ClassVar[str] = 'requeued'
    REDELIVERED: typing.ClassVar[str] = 'redelivered'
    TIME_SPENT: typing.ClassVar[str] = 'processing_time'
    TIME_WAITED: typing.ClassVar[str] = 'idle_time'

    CONSUMER_EXCEPTION: typing.ClassVar[str] = 'consumer_exception'
    MESSAGE_EXCEPTION: typing.ClassVar[str] = 'message_exception'
    PROCESSING_EXCEPTION: typing.ClassVar[str] = 'processing_exception'
    UNHANDLED_EXCEPTION: typing.ClassVar[str] = 'unhandled_exception'

    QOS_PREFETCH_COUNT: typing.ClassVar[int] = 1
    MAX_ERROR_COUNT: typing.ClassVar[int] = 5
    MAX_ERROR_WINDOW: typing.ClassVar[int] = 60
    MAX_SHUTDOWN_WAIT: typing.ClassVar[int] = 5

    def __init__(
        self,
        group: None = None,
        target: None = None,
        name: str | None = None,
        args: tuple[typing.Any, ...] = (),
        kwargs: dict[str, typing.Any] | None = None,
    ) -> None:
        if kwargs is None:
            kwargs = {}
        super().__init__(group, target, name, args, kwargs)
        self._kwargs: dict[str, typing.Any]  # set by super().__init__
        self.callbacks: models.Callbacks = models.Callbacks(
            on_ready=self.on_connection_ready,
            on_connection_failure=self.on_connection_failure,
            on_closed=self.on_connection_closed,
            on_blocked=self.on_connection_blocked,
            on_unblocked=self.on_connection_unblocked,
            on_confirmation=self.on_confirmation,
            on_delivery=self.on_message,
            on_return=self.on_message,
        )
        self.connections: dict[str, connection.Connection] = {}
        self.consumer: typing.Any = None
        self.consumer_version: str | None = None

        self.codec: codecs.Codec | None = None
        self.ioloop: asyncio.AbstractEventLoop | None = None
        self.last_failure: float = 0
        self.last_stats_time: float | None = None
        self.prepend_path: str | None = None
        self.sentry_client: bool | None = None
        self.state = self.STATE_INITIALIZING
        self.state_start = time.time()
        self.statsd: statsd.Client | None = None

        # Concurrent message tracking
        self._in_flight: dict[int, models.ProcessingContext] = {}
        self._tasks: set[asyncio.Task[typing.Any]] = set()

        # Cumulative counts for stats reporting
        self._cumulative_counts: dict[str, int | float] = {}
        self._previous_counts: dict[str, int | float] = {}
        self._error_count: int = 0
        self._processed_count: int = 0

        # Per-interval observation buffers for Prometheus / stats
        self._duration_observations: list[float] = []
        self._message_age_observations: list[float] = []
        self._custom_durations: dict[str, list[float]] = {}
        self._custom_counters: dict[str, int | float] = {}
        self._custom_gauges: dict[str, int | float] = {}

    def ack_message(self, ctx: models.ProcessingContext) -> None:
        """Acknowledge the message on the broker and log the ack.

        :param ctx: The processing context containing the message

        """
        if not ctx.connection.is_running:
            LOGGER.warning('Can not ack message, disconnected from RabbitMQ')
            ctx.measurement.set_tag(self.CLOSED_ON_COMPLETE, True)
            ctx.connection.shutdown()
            return

        LOGGER.debug('Acking %s', ctx.message.delivery_tag)
        ctx.channel.basic_ack(delivery_tag=ctx.message.delivery_tag)
        ctx.measurement.set_tag(self.ACKED, True)

    def calc_velocity(self, values: dict[str, typing.Any]) -> float:
        """Return the message consuming velocity for the process.

        :param dict values: The dict with velocity data
        :rtype: float

        """
        processed = values['counts'].get(self.PROCESSED, 0) - values[
            'previous'
        ].get(self.PROCESSED, 0)
        assert self.last_stats_time is not None
        duration = time.time() - self.last_stats_time

        # If there were no messages, do not calculate, use the base
        if not processed or not duration:
            return 0

        # Calculate the velocity as the basis for the calculation
        velocity = float(processed) / float(duration)
        LOGGER.debug('Message processing velocity: %.2f/s', velocity)
        return velocity

    def create_connections(self) -> None:
        """Create and start the RabbitMQ connections, assigning the connection
        object to the connections dict.

        """
        self.set_state(self.STATE_CONNECTING)
        for conn in self.consumer_config.connections:
            if isinstance(conn, str):
                name, consume, confirm = conn, True, False
            else:
                name, consume, confirm = (
                    conn.name,
                    conn.consume,
                    conn.confirm,
                )
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
    def get_config(
        cfg: models.ConsumerConfig, number: int, name: str, conn: int
    ) -> dict[str, str | models.ConnectionRef]:
        """Return the configuration for a single consumer."""
        return {
            'connection': cfg.connections[conn],
            'consumer_name': name,
            'process_name': f'{name}_{os.getpid()}_tag_{number}',
        }

    def get_consumer(self, cfg: models.ConsumerConfig) -> typing.Any:
        """Import and create a new instance of the configured message consumer.

        :param dict cfg: The named consumer section of the configuration
        :rtype: instance
        :raises: ImportError

        """
        if not cfg.consumer:
            return None
        try:
            handle, version = utils.import_consumer(cfg.consumer)
        except ImportError as error:
            LOGGER.exception(
                'Error importing the consumer %s: %s', cfg.consumer, error
            )
            return None

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
        return None

    async def invoke_consumer(self, ctx: models.ProcessingContext) -> None:
        """Process a single message, tracking it as in-flight.

        :param ctx: The processing context for the message

        """
        if self.is_shutting_down or self.is_waiting_to_shutdown:
            LOGGER.info('Dropping message due to shutdown')
            if not self.no_ack:
                self.reject(ctx, True)
            if not self._in_flight:
                self.on_ready_to_stop()
            return

        tag = (
            ctx.message.delivery_tag
            if ctx.message.delivery_tag is not None
            else id(ctx.message)
        )
        if not self._in_flight and self.is_idle:
            self.set_state(self.STATE_PROCESSING)
        self._in_flight[tag] = ctx

        if ctx.message.redelivered:
            ctx.measurement.set_tag(self.REDELIVERED, True)

        # Decode the message body async (avro schemas may need HTTP)
        if self.codec:
            msg = ctx.message
            try:
                msg.body = await self.codec.decode(
                    msg.body,
                    msg.content_type,
                    msg.content_encoding,
                    msg.message_type,
                )
            except codecs.DecodeError as error:
                LOGGER.error('Failed to decode message body: %s', error)
                ctx.result = models.Result.MESSAGE_EXCEPTION
                self.on_processed(ctx)
                return

        try:
            await self.consumer.execute(ctx)
        except Exception as error:
            LOGGER.exception(
                'Unhandled exception from consumer in '
                'process. This should not happen. %s',
                error,
            )
            ctx.result = models.Result.MESSAGE_REQUEUE
        finally:
            self._in_flight.pop(tag, None)

        LOGGER.debug('Finished processing message: %r', ctx.result)
        self.on_processed(ctx)

    def _schedule(
        self, coro: typing.Coroutine[typing.Any, typing.Any, typing.Any]
    ) -> None:
        """Schedule a coroutine as a fire-and-forget task, keeping a reference
        to prevent it from being garbage-collected before completion.
        """
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    @property
    def is_processing(self) -> bool:
        """Returns a bool specifying if the consumer is currently processing

        :rtype: bool

        """
        return self.state in [self.STATE_PROCESSING, self.STATE_STOP_REQUESTED]

    @property
    def is_running(self) -> bool:
        return self.state in [
            self.STATE_IDLE,
            self.STATE_ACTIVE,
            self.STATE_SLEEPING,
            self.STATE_PROCESSING,
        ]

    def on_connection_closed(self, name: str) -> None:
        if self.is_running:
            LOGGER.warning('Connection %s was closed, reconnecting', name)
            self.connections[name].connect()
            return

        ready = all(c.is_closed for c in self.connections.values())
        if (self.is_shutting_down or self.is_waiting_to_shutdown) and ready:
            self.on_ready_to_stop()

    def on_connection_failure(
        self, *args: typing.Any, **kwargs: typing.Any
    ) -> None:
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

    def on_connection_ready(self, name: str) -> None:
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

    def on_connection_blocked(self, name: str) -> None:
        LOGGER.warning('Connection %s blocked', name)
        if self.is_processing:
            self._schedule(self.consumer.on_blocked(name))

    def on_connection_unblocked(self, name: str) -> None:
        LOGGER.info('Connection %s unblocked', name)
        if self.is_processing:
            self._schedule(self.consumer.on_unblocked(name))

    def on_confirmation(
        self, name: str, delivered: bool, delivery_tag: str
    ) -> None:
        """Invoked on delivery confirmation

        :param str name: The RabbitMQ connection that confirmed the delivery
        :param bool delivered: Was the message was successfully delivered
        :param str delivery_tag: The delivery tag for the message

        """
        if self.is_processing:
            self.consumer.on_confirmation(name, delivered, delivery_tag)

    def on_message(
        self,
        name: str,
        channel: pika.channel.Channel,
        method: pika.spec.Basic.Deliver | pika.spec.Basic.Return,
        properties: pika.spec.BasicProperties,
        body: bytes,
    ) -> None:
        """Process a message from Rabbit"""
        timestamp = (
            datetime.datetime.fromtimestamp(
                properties.timestamp, tz=datetime.UTC
            )
            if properties.timestamp
            else None
        )
        if isinstance(method, pika.spec.Basic.Deliver):
            delivery_tag = method.delivery_tag
            redelivered = method.redelivered
            returned = False
        else:
            delivery_tag = None
            redelivered = False
            returned = True

        ctx = models.ProcessingContext(
            connection=self.connections[name],
            channel=channel,
            raw_body=body,
            message=models.Message(
                delivery_tag=delivery_tag,
                exchange=method.exchange,
                routing_key=method.routing_key,
                body=body,  # raw — decoded async in invoke_consumer
                app_id=properties.app_id,
                content_encoding=properties.content_encoding,
                content_type=properties.content_type,
                correlation_id=properties.correlation_id,
                delivery_mode=properties.delivery_mode,
                expiration=properties.expiration,
                headers=(
                    dict(properties.headers) if properties.headers else {}
                ),
                message_id=properties.message_id,
                message_type=properties.type,
                priority=properties.priority,
                redelivered=redelivered,
                reply_to=properties.reply_to,
                returned=returned,
                timestamp=timestamp,
                user_id=properties.user_id,
            ),
        )
        self._schedule(self.invoke_consumer(ctx))

    def on_processed(self, ctx: models.ProcessingContext) -> None:
        """Invoked after a message is processed by the consumer and
        implements the logic for how to deal with a message based upon
        the result.

        :param ctx: The processing context with message and result

        """
        duration = time.monotonic() - ctx.received_at
        ctx.measurement.add_duration(self.TIME_SPENT, duration)
        self._duration_observations.append(duration)

        match ctx.result:
            case models.Result.MESSAGE_DROP:
                LOGGER.debug(
                    'Rejecting message due to drop return from consumer'
                )
                self.reject(ctx, False)
                ctx.measurement.set_tag(self.DROPPED, True)
            case models.Result.MESSAGE_EXCEPTION:
                LOGGER.debug('Rejecting message due to MessageException')
                self.reject(ctx, False)
                ctx.measurement.set_tag(self.MESSAGE_EXCEPTION, True)
            case models.Result.PROCESSING_EXCEPTION:
                LOGGER.debug('Rejecting message due to ProcessingException')
                if self.consumer.ACK_PROCESSING_EXCEPTIONS:
                    self.ack_message(ctx)
                else:
                    self.reject(ctx, False)
                ctx.measurement.set_tag(self.PROCESSING_EXCEPTION, True)
            case models.Result.CONSUMER_EXCEPTION:
                LOGGER.debug('Re-queueing message due to ConsumerException')
                self.reject(ctx, True)
                self._on_processing_error()
                ctx.measurement.set_tag(self.CONSUMER_EXCEPTION, True)
            case models.Result.UNHANDLED_EXCEPTION:
                LOGGER.debug('Re-queueing message due to UnhandledException')
                self.reject(ctx, True)
                self._on_processing_error()
                ctx.measurement.set_tag(self.UNHANDLED_EXCEPTION, True)
            case models.Result.MESSAGE_REQUEUE:
                LOGGER.debug('Re-queueing message due Consumer request')
                self.reject(ctx, True)
                ctx.measurement.set_tag(self.REQUEUED, True)
            case models.Result.MESSAGE_ACK:
                if not self.no_ack:
                    self.ack_message(ctx)
                ctx.measurement.set_tag(self.ACKED, True)
            case _:
                LOGGER.error(
                    'Unexpected result %r for %s',
                    ctx.result,
                    ctx.message.delivery_tag,
                )

        ctx.measurement.set_tag(self.PROCESSED, True)
        self._processed_count += 1

        if ctx.message.timestamp:
            age = (
                datetime.datetime.now(tz=datetime.UTC) - ctx.message.timestamp
            )
            self._message_age_observations.append(age.total_seconds())

        # Accumulate into cumulative counts for stats reporting
        for key, value in ctx.measurement.tags.items():
            if isinstance(value, bool) and value:
                self._cumulative_counts[key] = (
                    self._cumulative_counts.get(key, 0) + 1
                )

        self._collect_custom_measurements(ctx.measurement)

        if self.statsd:
            self._submit_statsd(ctx.measurement)

        # Transition state based on remaining in-flight messages
        if not self._in_flight:
            if self.is_waiting_to_shutdown:
                self.shutdown_connections()
            elif self.is_processing:
                self.set_state(self.STATE_IDLE)

    def _on_processing_error(self) -> None:
        """Called when message processing failure happens due to a
        ConsumerException or an unhandled exception.

        """
        duration = time.time() - self.last_failure
        if duration > self.MAX_ERROR_WINDOW:
            LOGGER.info(
                'Resetting failure window, %i seconds since last', duration
            )
            self._error_count = 0
        self._error_count += 1
        self.last_failure = time.time()
        if self.too_many_errors:
            LOGGER.critical(
                'Error threshold exceeded (%i), shutting down',
                self._error_count,
            )
            self.shutdown_connections()

    def on_ready_to_stop(self) -> None:
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

        # Stop the event loop
        if self.ioloop:
            LOGGER.debug('Stopping event loop')
            self.ioloop.stop()

        # Note that shutdown is complete and set the state accordingly
        self.set_state(self.STATE_STOPPED)
        LOGGER.info('Shutdown complete')

    def on_sigprof(
        self, _unused_signum: int, _unused_frame: types.FrameType | None
    ) -> None:
        """Called when SIGPROF is sent to the process, will dump the stats, in
        future versions, queue them for the master process to get data.

        :param int _unused_signum: The signal number
        :param frame _unused_frame: The python frame the signal was received at

        """
        self.stats_queue.put(self.report_stats(), True)
        self.last_stats_time = time.time()
        signal.siginterrupt(signal.SIGPROF, False)

    def on_startup_error(self, error: str) -> None:
        """Invoked when a pre-condition for starting the consumer has failed.
        Log the error and then exit the process.

        """
        LOGGER.critical('Could not start %s: %s', self.consumer_name, error)
        self.set_state(self.STATE_STOPPED)

    def reject(
        self, ctx: models.ProcessingContext, requeue: bool = True
    ) -> None:
        """Reject the message on the broker and log it.

        :param ctx: The processing context containing the message
        :param requeue: Specify if the message should be re-queued

        """
        if self.no_ack:
            raise RuntimeError('Can not reject messages when ack is False')

        if not ctx.connection.is_running:
            LOGGER.warning('Can not nack message, disconnected from RabbitMQ')
            ctx.measurement.set_tag(self.CLOSED_ON_COMPLETE, True)
            ctx.connection.shutdown()
            return

        LOGGER.warning(
            'Rejecting message %s %s requeue',
            ctx.message.delivery_tag,
            'with' if requeue else 'without',
        )
        ctx.channel.basic_nack(
            delivery_tag=ctx.message.delivery_tag, requeue=requeue
        )
        ctx.measurement.set_tag(self.NACKED, True)
        ctx.measurement.set_tag(self.REQUEUED, requeue)

    def _collect_custom_measurements(self, m: measurement.Measurement) -> None:
        """Accumulate per-message Measurement data for Prometheus."""
        # Custom durations (excluding processing_time, already tracked)
        for key, values in m.durations.items():
            if key == self.TIME_SPENT:
                continue
            self._custom_durations.setdefault(key, []).extend(values)
        # Custom counters
        for counter_key, counter_value in m.counters.items():
            self._custom_counters[counter_key] = (
                self._custom_counters.get(counter_key, 0) + counter_value
            )
        # Custom gauges (values dict on Measurement)
        for gauge_key, gauge_value in m.values.items():
            self._custom_gauges[gauge_key] = gauge_value

    def report_stats(self) -> dict[str, typing.Any]:
        """Create the dict of stats data for the MCP stats queue"""
        counts = dict(self._cumulative_counts)
        counts[self.PROCESSED] = self._processed_count
        counts[self.ERROR] = self._error_count

        values = {
            'name': self.name,
            'consumer_name': self.consumer_name,
            'counts': counts,
            'previous': dict(self._previous_counts),
            'durations': list(self._duration_observations),
            'message_ages': list(self._message_age_observations),
            'custom_durations': {
                k: list(v) for k, v in self._custom_durations.items()
            },
            'custom_counters': dict(self._custom_counters),
            'custom_gauges': dict(self._custom_gauges),
        }
        self._previous_counts = dict(counts)
        self._duration_observations.clear()
        self._message_age_observations.clear()
        self._custom_durations.clear()
        self._custom_counters.clear()
        self._custom_gauges.clear()
        return values

    def run(self) -> None:
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

    def _run(self) -> None:
        """Run method that can be profiled"""
        self.set_state(self.STATE_INITIALIZING)
        self.ioloop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.ioloop)

        try:
            self.setup()
        except (AttributeError, ImportError) as error:
            LOGGER.exception('Setup failure: %s', error)
            self.on_startup_error(
                f'Failed to import the Python module for {self.consumer_name}'
            )
            return

        self.sentry_client = self.setup_sentry(
            self._kwargs['config'], self.consumer_name
        )

        if not self.is_stopped:
            try:
                self.ioloop.run_forever()
            except KeyboardInterrupt:
                LOGGER.warning('CTRL-C while waiting for clean shutdown')

    def send_exception_to_sentry(
        self,
        exc_info: (
            tuple[type[BaseException], BaseException, types.TracebackType]
            | tuple[None, None, None]
        ),
        ctx: models.ProcessingContext | None = None,
    ) -> None:
        """Send an exception to Sentry if enabled.

        :param tuple exc_info: exception information as returned from
            :func:`sys.exc_info`
        :param ctx: optional processing context for the message being handled

        """
        if not self.sentry_client:
            LOGGER.debug('No sentry_client, aborting')
            return

        message = ctx.message.model_dump() if ctx else {}
        duration = (
            math.ceil((time.monotonic() - ctx.received_at) * 1000)
            if ctx
            else 0
        )
        LOGGER.debug('Sending exception to sentry')
        with sentry_sdk.new_scope() as scope:
            scope.set_extra('consumer_name', self.consumer_name)
            scope.set_extra('message', message)
            scope.set_extra('time_spent', duration)
            sentry_sdk.capture_exception(exc_info, scope=scope)

    def setup(self) -> None:
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

        self.codec = codecs.Codec(self.config.schema_registry)
        self.setup_instrumentation()
        self._error_count = 0
        self.setup_sighandlers()
        self.create_connections()

    def setup_instrumentation(self) -> None:
        """Configure statsd instrumentation for per-message measurements."""
        if self.config.stats.statsd.enabled:
            self.statsd = statsd.Client(
                self.consumer_name,
                self.config.stats.statsd.model_dump(),
                self.stop,
            )
            LOGGER.debug('statsd measurements configured')

    def setup_sentry(self, cfg: models.Config, consumer_name: str) -> bool:
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

    def setup_sighandlers(self) -> None:
        """Setup the stats and stop signal handlers."""
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)

        signal.signal(signal.SIGPROF, self.on_sigprof)
        signal.signal(signal.SIGABRT, self.stop)

        signal.siginterrupt(signal.SIGPROF, False)
        signal.siginterrupt(signal.SIGABRT, False)
        LOGGER.debug('Signal handlers setup')

    def shutdown_connections(self) -> None:
        """This method closes the connections to RabbitMQ."""
        if not self.is_shutting_down:
            self.set_state(self.STATE_SHUTTING_DOWN)
        for name in self.connections:
            if self.connections[name].is_running:
                self.connections[name].shutdown()

    def stop(
        self, signum: int | None = None, _unused: types.FrameType | None = None
    ) -> None:
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

    def stop_consumer(self) -> None:
        """Stop the consumer object and allow it to do a clean shutdown if it
        has the ability to do so.

        """
        try:
            LOGGER.info('Shutting down the consumer')
            result = self.consumer.shutdown()
            if asyncio.iscoroutine(result):
                self._schedule(result)
        except AttributeError:
            LOGGER.debug('Consumer does not have a shutdown method')
        if self.codec:
            if self.ioloop and self.ioloop.is_running():
                self._schedule(self.codec.close())
            self.codec = None

    def _submit_statsd(self, m: measurement.Measurement) -> None:
        """Submit a measurement for a message to statsd as individual items."""
        assert self.statsd is not None
        for counter_key, counter_value in m.counters.items():
            self.statsd.incr(counter_key, counter_value)
        for dur_key, dur_values in m.durations.items():
            for dur_value in dur_values:
                self.statsd.add_timing(dur_key, dur_value)
        for gauge_key, gauge_value in m.values.items():
            self.statsd.set_gauge(gauge_key, gauge_value)
        for tag_key, tag_value in m.tags.items():
            if isinstance(tag_value, bool):
                if tag_value:
                    self.statsd.incr(tag_key)
            elif isinstance(tag_value, str):
                if tag_value:
                    self.statsd.incr(f'{tag_key}.{tag_value}')
            elif isinstance(tag_value, int):
                self.statsd.incr(tag_key, tag_value)
            else:
                LOGGER.warning(
                    'The %s value type of %s is unsupported',
                    tag_key,
                    type(tag_value),
                )

    @property
    def active_consumers(self) -> int:
        return len(
            [
                c
                for c in self.connections.values()
                if c.should_consume and c.is_active
            ]
        )

    @property
    def config(self) -> models.Config:
        return typing.cast(models.Config, self._kwargs['config'])

    @property
    def consumer_config(self) -> models.ConsumerConfig:
        return self.config.consumers.get(
            self.consumer_name, models.ConsumerConfig()
        )

    @property
    def consumer_name(self) -> str:
        return typing.cast(str, self._kwargs['consumer_name'])

    @property
    def expected_consumers(self) -> int:
        return len([c for c in self.connections.values() if c.should_consume])

    @property
    def logging_config(self) -> dict[str, typing.Any]:
        return typing.cast(
            dict[str, typing.Any], self._kwargs['logging_config']
        )

    @property
    def max_error_count(self) -> int:
        return int(self.consumer_config.max_errors)

    @property
    def no_ack(self) -> bool:
        return not self.consumer_config.ack

    @property
    def profile_file(self) -> str | None:
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
    def qos_prefetch(self) -> int:
        """Return the base, configured QoS prefetch value.

        :rtype: int

        """
        return self.consumer_config.qos_prefetch

    @property
    def queue_name(self) -> str:
        return self.consumer_config.queue or self.consumer_name

    @property
    def stats_queue(self) -> 'multiprocessing.Queue[dict[str, typing.Any]]':
        return self._kwargs['stats_queue']  # type: ignore[no-any-return]

    @property
    def too_many_errors(self) -> bool:
        """Return a bool if too many errors have occurred.

        :rtype: bool

        """
        return self._error_count >= self.max_error_count
