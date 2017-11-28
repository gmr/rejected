"""
Consumer process management. Imports consumer code, manages RabbitMQ
connection state and collects stats about the consuming process.

"""
import collections
import logging
import math
import multiprocessing
import os
from os import path
try:
    import cProfile as profile
except ImportError:
    import profile
import signal
import time
import warnings

try:
    import sprockets_influxdb as influxdb
except ImportError:
    influxdb = None

from tornado import gen, ioloop, locks
import pika

try:
    import raven
    from raven import breadcrumbs
    from raven.contrib.tornado import AsyncSentryClient
except ImportError:
    breadcrumbs, raven, AsyncSentryClient = None, None, None

from rejected import __version__, connection, data, state, statsd, utils

LOGGER = logging.getLogger(__name__)


Delivery = collections.namedtuple('Delivery', ['connection', 'message'])


class Process(multiprocessing.Process, state.State):
    """Core process class that manages the consumer object and communicates
    with RabbitMQ.

    """
    AMQP_APP_ID = 'rejected/%s' % __version__

    # Additional State constants
    STATE_PROCESSING = 0x04

    # Counter constants
    ACKED = 'acked'
    CLOSED_ON_COMPLETE = 'closed_on_complete'
    CLOSED_ON_START = 'closed_on_start'
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
    RABBITMQ_EXCEPTION = 'rabbitmq_exception'
    UNHANDLED_EXCEPTION = 'unhandled_exception'

    QOS_PREFETCH_COUNT = 1
    MAX_ERROR_COUNT = 5
    MAX_ERROR_WINDOW = 60
    MAX_SHUTDOWN_WAIT = 5

    def __init__(self,
                 group=None,
                 target=None,
                 name=None,
                 args=(),
                 kwargs=None):
        if kwargs is None:  # pragma: nocover
            kwargs = {}
        super(Process, self).__init__(group, target, name, args, kwargs)
        self.active_message = None
        self.callbacks = connection.Callbacks(
            self.on_connection_ready,
            self.on_connection_failure,
            self.on_connection_closed,
            self.on_connection_blocked,
            self.on_connection_unblocked,
            self.on_confirmation,
            self.on_delivery)
        self.connections = {}
        self.consumer = None
        self.consumer_lock = None
        self.consumer_version = None
        self.counters = collections.Counter()

        self.delivery_time = None
        self.influxdb = None
        self.ioloop = None
        self.last_failure = 0
        self.last_stats_time = None
        self.measurement = None
        self.message_connection_id = None
        self.pending = collections.deque()
        self.prepend_path = None
        self.previous = None
        self.sentry_client = None
        self.state = self.STATE_INITIALIZING
        self.state_start = time.time()
        self.statsd = None

        # Override ACTIVE with PROCESSING
        self.STATES[0x04] = 'Processing'

    def ack_message(self, message):
        """Acknowledge the message on the broker and log the ack

        :param message: The message to acknowledge
        :type message: rejected.data.Message

        """
        if message.channel.is_closed:
            LOGGER.warning('Can not ack message, channel is closed')
            self.counters[self.CLOSED_ON_COMPLETE] += 1
            return
        message.channel.basic_ack(delivery_tag=message.delivery_tag)
        self.counters[self.ACKED] += 1
        self.measurement.set_tag(self.ACKED, True)

    def create_connections(self):
        """Create and start the RabbitMQ connections, assigning the connection
        object to the connections dict.

        """
        self.set_state(self.STATE_CONNECTING)
        for conn in self.consumer_config.get('connections', []):
            name, confirm, consume = conn, False, True
            if isinstance(conn, dict):
                name = conn['name']
                confirm = conn.get('publisher_confirmation', False)
                consume = conn.get('consume', True)

            if name not in self.config['Connections']:
                LOGGER.critical('Connection "%s" for %s not found',
                                name, self.consumer_name)
                continue

            self.connections[name] = connection.Connection(
                name, self.config['Connections'][name], self.consumer_name,
                consume, confirm, self.ioloop, self.callbacks)

    @staticmethod
    def get_config(cfg, number, name, connection_name):
        """Initialize a new consumer thread, setting defaults and config values

        :param dict cfg: Consumer config section from YAML File
        :param int number: The identification number for the consumer
        :param str name: The name of the consumer
        :param str connection_name: The name of the connection):
        :rtype: dict

        """
        return {
            'connection': cfg['Connections'][connection_name],
            'consumer_name': name,
            'process_name': '%s_%i_tag_%i' % (name, os.getpid(), number)
        }

    def get_consumer(self, cfg):
        """Import and create a new instance of the configured message consumer.

        :param dict cfg: The named consumer section of the configuration
        :rtype: instance
        :raises: ImportError

        """
        try:
            handle, version = utils.import_consumer(cfg['consumer'])
        except ImportError as error:
            LOGGER.exception('Error importing the consumer %s: %s',
                             cfg['consumer'], error)
            return

        if version:
            LOGGER.info('Creating consumer %s v%s', cfg['consumer'], version)
            self.consumer_version = version
        else:
            LOGGER.info('Creating consumer %s', cfg['consumer'])

        settings = cfg.get('config', dict())
        settings['_import_module'] = '.'.join(cfg['consumer'].split('.')[0:-1])

        kwargs = {
            'settings': settings,
            'process': self,
            'drop_exchange': cfg.get('drop_exchange'),
            'drop_invalid_messages': cfg.get('drop_invalid_messages'),
            'message_type': cfg.get('message_type'),
            'error_exchange': cfg.get('error_exchange'),
            'error_max_retry': cfg.get('error_max_retry')
        }

        try:
            return handle(**kwargs)
        except Exception as error:
            LOGGER.exception('Error creating the consumer "%s": %s',
                             cfg['consumer'], error)

    @gen.engine
    def invoke_consumer(self, message):
        """Wrap the actual processor processing bits

        :param rejected.data.Message message: The message to process

        """
        # Only allow for a single message to be processed at a time
        with (yield self.consumer_lock.acquire()):
            if self.is_idle:
                if message.channel.is_closed:
                    LOGGER.warning('Channel %s is closed on '
                                   'connection "%s", discarding '
                                   'local copy of message %s',
                                   message.channel.channel_number,
                                   message.connection,
                                   utils.message_info(message.exchange,
                                                      message.routing_key,
                                                      message.properties))
                    self.counters[self.CLOSED_ON_START] += 1
                    self.maybe_get_next_message()
                    return

                self.set_state(self.STATE_PROCESSING)
                self.delivery_time = start_time = time.time()
                self.active_message = message

                self.measurement = data.Measurement()

                if message.method.redelivered:
                    self.counters[self.REDELIVERED] += 1
                    self.measurement.set_tag(self.REDELIVERED, True)

                try:
                    result = yield self.consumer.execute(message,
                                                         self.measurement)
                except Exception as error:
                    LOGGER.exception('Unhandled exception from consumer in '
                                     'process. This should not happen. %s',
                                     error)
                    result = data.MESSAGE_REQUEUE

                LOGGER.debug('Finished processing message: %r', result)
                self.on_processed(message, result, start_time)
            elif self.is_waiting_to_shutdown:
                LOGGER.info(
                    'Requeueing pending message due to pending shutdown')
                self.reject(message, True)
                self.shutdown_connections()
            elif self.is_shutting_down:
                LOGGER.info('Requeueing pending message due to shutdown')
                self.reject(message, True)
                self.on_ready_to_stop()
            else:
                LOGGER.warning('Exiting invoke_consumer without processing, '
                               'this should not happen. State: %s',
                               self.state_description)
            self.maybe_get_next_message()

    @property
    def is_processing(self):
        """Returns a bool specifying if the consumer is currently processing

        :rtype: bool

        """
        return self.state in [self.STATE_PROCESSING, self.STATE_STOP_REQUESTED]

    def maybe_get_next_message(self):
        """Pop the next message on the stack, adding a callback on the IOLoop
        to invoke the consumer with the message. This is done so we let the
        IOLoop perform any pending callbacks before trying to process the
        next message.

        """
        if self.pending:
            self.ioloop.add_callback(
                self.invoke_consumer, self.pending.popleft())

    def maybe_submit_measurement(self):
        """Check for configured instrumentation backends and if found, submit
        the message measurement info.

        """
        if self.statsd:
            self.submit_statsd_measurements()
        if self.influxdb:
            self.submit_influxdb_measurement()

    def on_connection_closed(self, name):
        if self.is_running:
            LOGGER.warning('Connection %s was closed, reconnecting', name)
            return self.connections[name].connect()

        ready = all([c.is_closed for c in self.connections.values()])
        if (self.is_shutting_down or self.is_waiting_to_shutdown) and ready:
            self.on_ready_to_stop()

    def on_connection_failure(self, *args, **kwargs):
        LOGGER.warning('Connection failure %r %r', args, kwargs)
        ready = all([c.is_closed for c in self.connections.values()])
        if (self.is_connecting or
                self.is_shutting_down or
                self.is_waiting_to_shutdown) and ready:
            self.on_ready_to_stop()

    def on_connection_ready(self, name):
        LOGGER.debug('Connection %s indicated it is ready', name)
        self.consumer.set_connection(self.connections[name])
        if all([c.is_connected for c in self.connections.values()]):
            for key in self.connections.keys():
                if self.connections[key].should_consume:
                    self.connections[key].consume(
                        self.queue_name, self.no_ack, self.qos_prefetch)
            if self.is_connecting:
                self.set_state(self.STATE_IDLE)

    def on_connection_blocked(self, name):
        LOGGER.warning('Connection %s blocked', name)
        if self.is_processing:
            self.consumer.on_blocked(name)

    def on_connection_unblocked(self, name):
        LOGGER.info('Connection %s unblocked', name)
        if self.is_processing:
            self.consumer.on_blocked(name)

    def on_confirmation(self, name, delivered, delivery_tag):
        if self.is_processing:
            self.consumer.on_confirmation(name, delivered, delivery_tag)
        else:
            LOGGER.warning('Publisher Confirmation received on connection %s '
                           'while not processing (%r %r)',
                           name, delivered, delivery_tag)

    def on_delivery(self, name, channel, method, properties, body):
        """Process a message from Rabbit

        :param str name: The connection name
        :param pika.channel.Channel channel: The message's delivery channel
        :param pika.frames.MethodFrame method: The method frame
        :param pika.spec.BasicProperties properties: The message properties
        :param str body: The message body

        """
        message = data.Message(name, channel, method, properties, body)
        if self.is_processing:
            return self.pending.append(message)
        self.invoke_consumer(message)

    def on_processed(self, message, result, start_time):
        """Invoked after a message is processed by the consumer and
        implements the logic for how to deal with a message based upon
        the result.

        :param rejected.data.Message message: The message that was processed
        :param int result: The result of the processing of the message
        :param float start_time: When the message was received

        """
        duration = max(start_time, time.time()) - start_time
        self.counters[self.TIME_SPENT] += duration
        self.measurement.add_duration(self.TIME_SPENT, duration)

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
            self.reject(message, False)
            self.counters[self.PROCESSING_EXCEPTION] += 1

        elif result == data.CONSUMER_EXCEPTION:
            LOGGER.debug('Re-queueing message due to ConsumerException')
            self.reject(message, True)
            self.on_processing_error()
            self.counters[self.CONSUMER_EXCEPTION] += 1

        elif result == data.RABBITMQ_EXCEPTION:
            LOGGER.debug('Processing interrupted due to RabbitMQException')
            self.on_processing_error()
            self.counters[self.RABBITMQ_EXCEPTION] += 1

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
        self.maybe_submit_measurement()
        self.reset_state()

    def on_processing_error(self):
        """Called when message processing failure happens due to a
        ConsumerException or an unhandled exception.

        """
        duration = time.time() - self.last_failure
        if duration > self.MAX_ERROR_WINDOW:
            LOGGER.info('Resetting failure window, %i seconds since last',
                        duration)
            self.reset_error_counter()
        self.counters[self.ERROR] += 1
        self.last_failure = time.time()
        if self.too_many_errors:
            LOGGER.critical('Error threshold exceeded (%i), shutting down',
                            self.counters[self.ERROR])
            self.shutdown_connections()

    def on_ready_to_stop(self):
        """Invoked when the consumer is ready to stop."""

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

        # Stop the IOLoop
        if self.ioloop:
            LOGGER.debug('Stopping IOLoop')
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

        if message.channel.is_closed:
            LOGGER.warning('Can not nack message, disconnected from RabbitMQ')
            self.counters[self.CLOSED_ON_COMPLETE] += 1
            return

        LOGGER.warning('Rejecting message %s %s requeue', message.delivery_tag,
                       'with' if requeue else 'without')
        message.channel.basic_nack(
            delivery_tag=message.delivery_tag, requeue=requeue)
        self.measurement.set_tag(self.NACKED, True)
        self.measurement.set_tag(self.REQUEUED, requeue)

    def report_stats(self):
        """Create the dict of stats data for the MCP stats queue"""
        if not self.previous:
            self.previous = dict()
            for key in self.counters:
                self.previous[key] = 0
        values = {
            'name': self.name,
            'consumer_name': self.consumer_name,
            'counts': dict(self.counters),
            'previous': dict(self.previous)
        }
        self.previous = dict(self.counters)
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
        LOGGER.debug('State reset to %s (%s in pending)',
                     self.state_description, len(self.pending))

    def run(self):
        """Start the consumer"""
        if self.profile_file:
            LOGGER.info('Profiling to %s', self.profile_file)
            profile.runctx('self._run()', globals(), locals(),
                           self.profile_file)
        else:
            self._run()
        LOGGER.debug('Exiting %s (%i, %i)', self.name, os.getpid(),
                     os.getppid())

    def _run(self):
        """Run method that can be profiled"""
        self.set_state(self.STATE_INITIALIZING)
        self.ioloop = ioloop.IOLoop.current()
        self.consumer_lock = locks.Lock()

        self.sentry_client = self.setup_sentry(
            self._kwargs['config'], self.consumer_name)

        try:
            self.setup()
        except (AttributeError, ImportError):
            return self.on_startup_error(
                'Failed to import the Python module for {}'.format(
                    self.consumer_name))

        if not self.is_stopped:
            try:
                self.ioloop.start()
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
        kwargs = {'extra': {
                      'consumer_name': self.consumer_name,
                      'env': dict(os.environ),
                      'message': message},
                  'time_spent': duration}
        LOGGER.debug('Sending exception to sentry: %r', kwargs)
        self.sentry_client.captureException(exc_info, **kwargs)

    def setup(self):
        """Initialize the consumer, setting up needed attributes and connecting
        to RabbitMQ.

        """
        LOGGER.info('Initializing for %s', self.name)

        if 'consumer' not in self.consumer_config:
            return self.on_startup_error(
                '"consumer" not specified in configuration')

        self.consumer = self.get_consumer(self.consumer_config)

        if not self.consumer:
            return self.on_startup_error(
                'Could not import "{}"'.format(
                    self.consumer_config.get(
                        'consumer', 'unconfigured consumer')))

        self.setup_instrumentation()
        self.reset_error_counter()
        self.setup_sighandlers()
        self.create_connections()

    def setup_influxdb(self, config):
        """Configure the InfluxDB module for measurement submission.

        :param dict config: The InfluxDB configuration stanza

        """
        base_tags = {
            'version': self.consumer_version
        }
        measurement = self.config.get('influxdb_measurement',
                                      os.environ.get('SERVICE'))
        if measurement != self.consumer_name:
            base_tags['consumer'] = self.consumer_name
        for key in {'ENVIRONMENT', 'SERVICE'}:
            if key in os.environ:
                base_tags[key.lower()] = os.environ[key]
        influxdb.install(
            '{}://{}:{}/write'.format(
                config.get('scheme',
                           os.environ.get('INFLUXDB_SCHEME', 'http')),
                config.get('host',
                           os.environ.get('INFLUXDB_HOST', 'localhost')),
                config.get('port', os.environ.get('INFLUXDB_PORT', '8086'))
            ),
            config.get('user', os.environ.get('INFLUXDB_USER')),
            config.get('password', os.environ.get('INFLUXDB_PASSWORD')),
            base_tags=base_tags)
        return config.get('database', 'rejected'), measurement

    def setup_instrumentation(self):
        """Configure instrumentation for submission per message measurements
        to statsd and/or InfluxDB.

        """
        if not self.config.get('stats') and not self.config.get('statsd'):
            return

        if 'stats' not in self.config:
            self.config['stats'] = {}

        # Backwards compatible statsd config support
        if self.config.get('statsd'):
            warnings.warn('Deprecated statsd configuration detected',
                          DeprecationWarning)
            self.config['stats'].setdefault('statsd',
                                            self.config.get('statsd'))

        if self.config['stats'].get('statsd'):
            if self.config['stats']['statsd'].get('enabled', True):
                self.statsd = statsd.Client(self.consumer_name,
                                            self.config['stats']['statsd'])
            LOGGER.debug('statsd measurements configured')

        # InfluxDB support
        if influxdb and self.config['stats'].get('influxdb'):
            if self.config['stats']['influxdb'].get('enabled', True):
                self.influxdb = self.setup_influxdb(
                    self.config['stats']['influxdb'])
            LOGGER.debug('InfluxDB measurements configured: %r', self.influxdb)

    def setup_sentry(self, cfg, consumer_name):
        # Setup the Sentry client if configured and installed
        sentry_dsn = cfg['Consumers'][consumer_name].get('sentry_dsn',
                                                         cfg.get('sentry_dsn'))
        if not raven or not sentry_dsn:
            return
        consumer = cfg['Consumers'][consumer_name]['consumer'].split('.')[0]
        kwargs = {
            'exclude_paths': [],
            'include_paths':
                ['pika', 'rejected', 'raven', 'tornado', consumer],
            'ignore_exceptions': ['rejected.consumer.ConsumerException',
                                  'rejected.consumer.MessageException',
                                  'rejected.consumer.ProcessingException'],
            'processors': ['raven.processors.SanitizePasswordsProcessor']
        }

        if os.environ.get('ENVIRONMENT'):
            kwargs['environment'] = os.environ['ENVIRONMENT']

        if self.consumer_version:
            kwargs['version'] = self.consumer_version

        for logger in {'pika', 'pika.channel', 'pika.connection',
                       'pika.callback', 'pika.heartbeat',
                       'rejected.process', 'rejected.state'}:
            breadcrumbs.ignore_logger(logger)

        return AsyncSentryClient(sentry_dsn, **kwargs)

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

    def submit_influxdb_measurement(self):
        """Submit a measurement for a message to InfluxDB"""
        measurement = influxdb.Measurement(*self.influxdb)
        measurement.set_timestamp(time.time())
        for key, value in self.measurement.counters.items():
            measurement.set_field(key, value)
        for key, value in self.measurement.tags.items():
            measurement.set_tag(key, value)
        for key, value in self.measurement.values.items():
            measurement.set_field(key, value)

        for key, values in self.measurement.durations.items():
            if len(values) == 1:
                measurement.set_field(key, values[0])
            elif len(values) > 1:
                measurement.set_field('{}-average'.format(key),
                                      sum(values) / len(values))
                measurement.set_field('{}-max'.format(key), max(values))
                measurement.set_field('{}-min'.format(key), min(values))
                measurement.set_field('{}-median'.format(key),
                                      utils.percentile(values, 50))
                measurement.set_field('{}-95th'.format(key),
                                      utils.percentile(values, 95))

        influxdb.add_measurement(measurement)
        LOGGER.debug('InfluxDB Measurement: %r', measurement.marshall())

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
                    self.statsd.incr('{}.{}'.format(key, value))
            elif isinstance(value, int):
                self.statsd.incr(key, value)
            else:
                LOGGER.warning('The %s value type of %s is unsupported',
                               key, type(value))

    @property
    def active_consumers(self):
        return len([c for c in self.connections.values()
                    if c.should_consume and c.is_active()])

    @property
    def config(self):
        return self._kwargs['config']

    @property
    def consumer_config(self):
        return self.config['Consumers'][self.consumer_name] or {}

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
        return int(self.consumer_config.get('max_errors',
                                            self.MAX_ERROR_COUNT))

    @property
    def no_ack(self):
        return not self.consumer_config.get('ack', True)

    @property
    def profile_file(self):
        """Return the full path to write the cProfile data

        :return: str

        """
        if 'profile' in self._kwargs and self._kwargs['profile']:
            profile_path = path.normpath(self._kwargs['profile'])
            if os.path.exists(profile_path) and os.path.isdir(profile_path):
                return path.join(
                    profile_path, '{}-{}.prof'.format(
                        os.getpid(), self._kwargs['consumer_name']))

    @property
    def qos_prefetch(self):
        """Return the base, configured QoS prefetch value.

        :rtype: int

        """
        return self.consumer_config.get(
            'qos_prefetch', self.QOS_PREFETCH_COUNT)

    @property
    def queue_name(self):
        return self.consumer_config.get('queue', self.name)

    @property
    def stats_queue(self):
        return self._kwargs['stats_queue']

    @property
    def too_many_errors(self):
        """Return a bool if too many errors have occurred.

        :rtype: bool

        """
        return self.counters[self.ERROR] >= self.max_error_count
