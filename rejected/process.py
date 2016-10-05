"""
Consumer process management. Imports consumer code, manages RabbitMQ
connection state and collects stats about the consuming process.

"""
import collections
import importlib
import logging
import math
import multiprocessing
import os
import pkg_resources
from os import path
try:
    import cProfile as profile
except ImportError:
    import profile
import signal
import sys
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
except ImportError:
    raven = None
from pika import spec

from rejected import __version__, data, state, statsd

LOGGER = logging.getLogger(__name__)


def import_consumer(value):
    """Pass in a string in the format of foo.Bar, foo.bar.Baz, foo.bar.baz.Qux
    and it will return a handle to the class

    :param str value: The consumer class in module.Consumer format
    :return: tuple(Class, str)

    """
    parts = value.split('.')
    import_name = '.'.join(parts[0:-1])
    import_handle = importlib.import_module(import_name)
    version = None
    if hasattr(import_handle, '__version__'):
        version = import_handle.__version__
    elif len(parts) > 2:
        package_handle = importlib.import_module(parts[0])
        if hasattr(package_handle, '__version__'):
            version = package_handle.__version__

    # Return the class handle
    return getattr(import_handle, parts[-1]), version


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
    DROPPED = 'dropped'
    ERROR = 'failed'
    FAILURES = 'failures_until_stop'
    NACKED = 'nacked'
    PROCESSED = 'processed'
    REQUEUED = 'requeued'
    REDELIVERED = 'redelivered'
    TIME_SPENT = 'processing_time'
    TIME_WAITED = 'idle_time'

    MESSAGE_AGE = 'message_age'

    CONSUMER_EXCEPTION = 'consumer_exception'
    MESSAGE_EXCEPTION = 'message_exception'
    PROCESSING_EXCEPTION = 'processing_exception'
    UNHANDLED_EXCEPTION = 'unhandled_exception'

    HB_INTERVAL = 300

    # Default message pre-allocation value
    QOS_PREFETCH_COUNT = 1
    QOS_PREFETCH_MULTIPLIER = 1.25
    QOS_MAX = 10000
    MAX_ERROR_COUNT = 5
    MAX_ERROR_WINDOW = 60
    MAX_SHUTDOWN_WAIT = 5

    def __init__(self,
                 group=None,
                 target=None,
                 name=None,
                 args=(),
                 kwargs=None):
        if kwargs is None:
            kwargs = {}
        super(Process, self).__init__(group, target, name, args, kwargs)
        self.ack = True
        self.active_message = None
        self.channel = None
        self.config = None
        self.connection = None
        self.connection_id = 0
        self.connection_name = None
        self.connections = None
        self.consumer = None
        self.consumer_lock = None
        self.consumer_name = None
        self.consumer_version = None
        self.counters = collections.Counter()
        self.delivery_time = None
        self.influxdb = None
        self.ioloop = None
        self.last_failure = 0
        self.last_stats_time = None
        self.logging_config = dict()
        self.measurement = None
        self.message_connection_id = None
        self.max_error_count = self.MAX_ERROR_COUNT
        self.max_frame_size = spec.FRAME_MAX_SIZE
        self.queue_name = None
        self.prepend_path = None
        self.previous = None
        self.sentry_client = None
        self.state = self.STATE_INITIALIZING
        self.state_start = time.time()
        self.stats_queue = None
        self.statsd = None

        # Override ACTIVE with PROCESSING
        self.STATES[0x04] = 'Processing'

    def ack_message(self, delivery_tag):
        """Acknowledge the message on the broker and log the ack

        :param str delivery_tag: Delivery tag to acknowledge

        """
        if not self.can_respond:
            LOGGER.warning('Can not ack message, disconnected from RabbitMQ')
            self.counters[self.CLOSED_ON_COMPLETE] += 1
            return
        LOGGER.debug('Acking %s', delivery_tag)
        self.channel.basic_ack(delivery_tag=delivery_tag)
        self.counters[self.ACKED] += 1
        self.measurement.set_tag(self.ACKED, True)

    def add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.

        """
        LOGGER.debug('Adding channel close callback')
        self.channel.add_on_close_callback(self.on_channel_closed)

    def calc_velocity(self, values):
        """Return the message consuming velocity for the process.

        :param dict values: The dict with velocity data
        :rtype: float

        """
        processed = (values['counts'].get(self.PROCESSED, 0) -
                     values['previous'].get(self.PROCESSED, 0))
        duration = time.time() - self.last_stats_time

        # If there were no messages, do not calculate, use the base
        if not processed or not duration:
            return 0

        # Calculate the velocity as the basis for the calculation
        velocity = float(processed) / float(duration)
        LOGGER.debug('Message processing velocity: %.2f/s', velocity)
        return velocity

    @property
    def can_respond(self):
        """Indicates if the process can still respond to RabbitMQ when the
        processing of a message has completed.

        :return: bool

        """
        if not self.channel:
            return False
        return self.message_connection_id == self.connection_id

    def cancel_consumer_with_rabbitmq(self):
        """Tell RabbitMQ the process no longer wants to consumer messages."""
        LOGGER.debug('Sending a Basic.Cancel to RabbitMQ')
        if self.channel and self.channel.is_open:
            self.channel.basic_cancel(consumer_tag=self.name)

    def close_connection(self):
        """This method closes the connection to RabbitMQ."""
        LOGGER.info('Closing connection')
        self.connection.close()

    def connect_to_rabbitmq(self, config, name):
        """Connect to RabbitMQ returning the connection handle.

        :param dict config: The Connections section of the configuration
        :param str name: The name of the connection

        """
        self.set_state(self.STATE_CONNECTING)
        self.connection_id += 1
        params = self.get_connection_parameters(config[name])
        LOGGER.debug('Connecting to %s:%i:%s as %s',
                     params.host, params.port, params.virtual_host,
                     params.credentials.username)
        return pika.TornadoConnection(params,
                                      self.on_connect_open,
                                      self.on_connect_failed,
                                      self.on_closed,
                                      False,
                                      self.ioloop)

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
            'connection': cfg['Connections'][connection],
            'connection_name': connection,
            'consumer_name': name,
            'process_name': '%s_%i_tag_%i' % (name, os.getpid(), number)
        }

    def get_connection_parameters(self, config):
        """Return connection parameters for a pika connection.

        :param dict config: Rejected connection configuration
        :rtype: pika.ConnectionParameters

        """
        heartbeat_interval = config.get('heartbeat_interval', self.HB_INTERVAL)
        password = config.get('password', config.get('pass', 'guest'))
        credentials = pika.PlainCredentials(config.get('user', 'guest'),
                                            password)
        return pika.ConnectionParameters(config.get('host', 'localhost'),
                                         config.get('port', 5672),
                                         config.get('vhost', '/'),
                                         credentials,
                                         frame_max=self.max_frame_size,
                                         socket_timeout=10,
                                         heartbeat_interval=heartbeat_interval)

    def get_consumer(self, cfg):
        """Import and create a new instance of the configured message consumer.

        :param dict cfg: The named consumer section of the configuration
        :rtype: instance
        :raises: ImportError

        """
        try:
            consumer_, version = import_consumer(cfg['consumer'])
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
            'drop_invalid_messages': cfg.get('drop_invalid_messages'),
            'message_type': cfg.get('message_type'),
            'error_exchange': cfg.get('error_exchange'),
            'error_max_retry': cfg.get('error_max_retry')
        }

        try:
            return consumer_(**kwargs)
        except Exception as error:
            LOGGER.exception('Error creating the consumer "%s": %s',
                             cfg['consumer'], error)

    def get_module_data(self):
        modules = {}
        for module_name in sys.modules.keys():
            module = sys.modules[module_name]
            if hasattr(module, '__version__'):
                modules[module_name] = module.__version__
            elif hasattr(module, 'version'):
                modules[module_name] = module.version
            else:
                try:
                    version = self.get_version(module_name)
                    if version:
                        modules[module_name] = version
                except Exception:
                    pass
        return modules

    @staticmethod
    def get_version(module_name):
        try:
            return pkg_resources.get_distribution(module_name).version
        except pkg_resources.DistributionNotFound:
            return None

    @gen.engine
    def invoke_consumer(self, message):
        """Wrap the actual processor processing bits

        :param rejected.data.Message message: The message to process

        """
        # Only allow for a single message to be processed at a time
        with (yield self.consumer_lock.acquire()):
            if self.is_idle:
                self.set_state(self.STATE_PROCESSING)
                self.delivery_time = start_time = time.time()
                self.active_message = message

                self.measurement = data.Measurement()

                if message.method.redelivered:
                    self.counters[self.REDELIVERED] += 1
                    self.measurement.set_tag(self.REDELIVERED, True)

                if message.properties.timestamp:
                    self.measurement.set_value(
                        self.MESSAGE_AGE,
                        max(message.properties.timestamp, start_time) -
                        message.properties.timestamp)

                self.start_message_processing()
                try:
                    result = yield self.consumer._execute(message,
                                                          self.measurement)
                except Exception as error:
                    LOGGER.exception('Unhandled exception from consumer in '
                                     'process. This should not happen. %s',
                                     error)
                    result = data.MESSAGE_REQUEUE

                LOGGER.debug('Finished processing message: %r', result)
                self.on_processed(message, result, start_time)

            elif self.is_waiting_to_shutdown or self.is_shutting_down:
                LOGGER.info('Requeueing pending message due to shutdown')
                self.reject(message.delivery_tag, True)
                self.on_ready_to_stop()
            else:
                LOGGER.warning('Exiting invoke_consumer without processing, '
                               'this should not happen. State: %s',
                               self.state_description)

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
        if self.influxdb:
            self.submit_influxdb_measurement()

    def on_channel_closed(self, _channel, reply_code, reply_text):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel _channel: The AMQP Channel
        :param int reply_code: The AMQP reply code
        :param str reply_text: The AMQP reply text

        """
        LOGGER.critical('Channel was closed: (%s) %s', reply_code, reply_text)
        del self.channel
        self.on_ready_to_stop()

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened. It
        will change the state to IDLE, add the callbacks and setup the channel
        to start consuming.

        :param pika.channel.Channel channel: The channel object

        """
        LOGGER.debug('Channel opened')
        self.channel = channel
        self.add_on_channel_close_callback()
        self.setup_channel()

    def on_closed(self, _unused, code, text):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Shutdown if not already doing so.

        :param pika.connection.Connection _unused: The closed connection
        :param int code: The AMQP reply code
        :param str text: The AMQP reply text

        """
        LOGGER.critical('Connection from RabbitMQ closed in state %s (%s, %s)',
                        self.state_description, code, text)
        self.channel = None
        if not self.is_shutting_down and not self.is_waiting_to_shutdown:
            self.on_ready_to_stop()

    def on_connect_failed(self, *args, **kwargs):
        LOGGER.critical('Could not connect to RabbitMQ: %r', (args, kwargs))
        self.on_ready_to_stop()

    def on_connect_open(self, connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.

        :type connection: pika.adapters.tornado_connection.TornadoConnection

        """
        LOGGER.debug('Connection opened')
        self.connection = connection
        self.open_channel()

    def on_message(self, channel=None, method=None, properties=None, body=None):
        """Process a message from Rabbit

        :param pika.channel.Channel channel: The channel the message was sent on
        :param pika.frames.MethodFrame method: The method frame
        :param pika.spec.BasicProperties properties: The message properties
        :param str body: The message body

        """
        self.invoke_consumer(data.Message(channel, method, properties, body))

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
        self.measurement.set_value(self.TIME_SPENT, duration)

        if result == data.MESSAGE_DROP:
            LOGGER.debug('Rejecting message due to drop return from consumer')
            self.reject(message.delivery_tag, False)
            self.counters[self.DROPPED] += 1

        elif result == data.MESSAGE_EXCEPTION:
            LOGGER.debug('Rejecting message due to MessageException')
            self.reject(message.delivery_tag, False)
            self.counters[self.MESSAGE_EXCEPTION] += 1
            self.measurement.set_tag('exception', 'MessageException')

        elif result == data.PROCESSING_EXCEPTION:
            LOGGER.debug('Rejecting message due to ProcessingException')
            self.reject(message.delivery_tag, False)
            self.counters[self.PROCESSING_EXCEPTION] += 1
            self.measurement.set_tag('exception', 'ProcessingException')

        elif result == data.CONSUMER_EXCEPTION:
            LOGGER.debug('Re-queueing message due to ConsumerException')
            self.reject(message.delivery_tag, True)
            self.on_processing_error()
            self.counters[self.CONSUMER_EXCEPTION] += 1
            self.measurement.set_tag('exception', 'ConsumerException')

        elif result == data.UNHANDLED_EXCEPTION:
            LOGGER.debug('Re-queueing message due to UnhandledException')
            self.reject(message.delivery_tag, True)
            self.on_processing_error()
            self.counters[self.UNHANDLED_EXCEPTION] += 1
            self.measurement.set_tag('exception', 'UnhandledException')

        elif result == data.MESSAGE_REQUEUE:
            LOGGER.debug('Re-queueing message due Consumer request')
            self.reject(message.delivery_tag, True)
            self.counters[self.REQUEUED] += 1

        elif result == data.MESSAGE_ACK and self.ack:
            self.ack_message(message.delivery_tag)

        self.counters[self.PROCESSED] += 1
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
            self.cancel_consumer_with_rabbitmq()
            self.close_connection()
            self.on_ready_to_stop()

    @staticmethod
    def on_qos_set(frame):
        """Invoked by pika when the QoS is set

        :param pika.frame.Frame frame: The QoS Frame

        """
        LOGGER.debug("QoS was set: %r", frame)

    def on_ready_to_stop(self):

        # Set the state to shutting down if it wasn't set as that during loop
        self.set_state(self.STATE_SHUTTING_DOWN)

        # Reset any signal handlers
        signal.signal(signal.SIGABRT, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGPROF, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)

        # If the connection is still around, close it
        if self.connection and self.connection.is_open:
            LOGGER.debug('Closing connection to RabbitMQ')
            self.connection.close()

        # Allow the consumer to gracefully stop and then stop the IOLoop
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

    def open_channel(self):
        """Open a channel on the existing open connection to RabbitMQ"""
        LOGGER.debug('Opening a channel on %r', self.connection)
        self.connection.channel(self.on_channel_open)

    @property
    def profile_file(self):
        """Return the full path to write the cProfile data

        :return: str

        """
        if not self._kwargs['profile']:
            return None
        if os.path.exists(self._kwargs['profile']) and \
                os.path.isdir(self._kwargs['profile']):
            return '%s/%s-%s.prof' % (path.normpath(self._kwargs['profile']),
                                      os.getpid(),
                                      self._kwargs['consumer_name'])
        return None

    @property
    def qos_prefetch(self):
        """Return the base, configured QoS prefetch value.

        :rtype: int

        """
        return self.config.get('qos_prefetch', self.QOS_PREFETCH_COUNT)

    def reject(self, delivery_tag, requeue=True):
        """Reject the message on the broker and log it. We should move this to
         use to nack when Pika supports it in a released version.

        :param str delivery_tag: Delivery tag to reject
        :param bool requeue: Specify if the message should be re-queued or not

        """
        if not self.ack:
            raise RuntimeError('Can not rejected messages when ack is False')
        if not self.can_respond:
            LOGGER.warning('Can not reject message, disconnected from RabbitMQ')
            self.counters[self.CLOSED_ON_COMPLETE] += 1
            self.measurement.set_tag(self.CLOSED_ON_COMPLETE, True)
            return

        LOGGER.warning('Rejecting message %s %s requeue', delivery_tag, 'with'
                       if requeue else 'without')
        self.channel.basic_nack(delivery_tag=delivery_tag, requeue=requeue)
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
            self.on_ready_to_stop()
        elif self.is_processing:
            self.set_state(self.STATE_IDLE)
        elif self.is_idle or self.is_connecting or self.is_shutting_down:
            pass
        else:
            LOGGER.critical('Unexepected state: %s', self.state_description)

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
        self.ioloop = ioloop.IOLoop.current()
        self.consumer_lock = locks.Lock()
        try:
            self.setup(self._kwargs['config'],
                       self._kwargs['consumer_name'],
                       self._kwargs['connection_name'],
                       self._kwargs['stats_queue'])
        except (AttributeError, ImportError) as error:
            name = self._kwargs['consumer_name']
            class_name = self._kwargs['config']['Consumers'][name]['consumer']
            LOGGER.exception('Could not start %s, stopping process: %r',
                             class_name, error)
            os.kill(os.getppid(), signal.SIGABRT)
            sys.exit(1)

        # Connect to RabbitMQ after the IOLoop has started
        self.ioloop.add_callback(self.connect_to_rabbitmq,
                                 self.connections,
                                 self.connection_name)

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
            return

        message = dict(self.active_message)
        try:
            duration = math.ceil(time.time() - self.delivery_time) * 1000
        except TypeError:
            duration = 0
        kwargs = {'logger': 'rejected.processs',
                  'modules': self.get_module_data(),
                  'extra': {
                      'consumer_name': self.consumer_name,
                      'connection': self.connection_name,
                      'env': dict(os.environ),
                      'message': message},
                  'time_spent': duration}
        LOGGER.debug('Sending exception to sentry: %r', kwargs)
        self.sentry_client.captureException(exc_info, **kwargs)

    def setup(self, cfg, consumer_name, connection_name, stats_queue):
        """Initialize the consumer, setting up needed attributes and connecting
        to RabbitMQ.

        :param dict cfg: Consumer config section
        :param str consumer_name: Consumer name for config
        :param str connection_name: The name of the connection
        :param multiprocessing.Queue stats_queue: Queue to MCP

        """
        LOGGER.info('Initializing for %s on %s connection', self.name,
                    connection_name)

        # Setup the Sentry client if configured and installed
        sentry_dsn = cfg['Consumers'][consumer_name].get('sentry_dsn',
                                                         cfg.get('sentry_dsn'))
        if raven and sentry_dsn:
            kwargs = {
                'exclude_paths': ['tornado'],
                'include_paths': ['pika',
                                  'rejected',
                                  cfg['Consumers'][consumer_name]['consumer']],
                'ignore_exceptions': ['rejected.consumer.ConsumerException',
                                      'rejected.consumer.MessageException',
                                      'rejected.consumer.ProcessingException'],
                'processors': ['raven.processors.SanitizePasswordsProcessor']
            }

            if os.environ.get('ENVIRONMENT'):
                kwargs['environment'] = os.environ['ENVIRONMENT']

            if self.consumer_version:
                kwargs['version'] = self.consumer_version

            self.sentry_client = raven.Client(sentry_dsn, **kwargs)

        self.connection_name = connection_name
        self.consumer_name = consumer_name
        self.config = cfg['Consumers'][consumer_name]

        self.connections = cfg['Connections']
        self.consumer = self.get_consumer(self.config)

        if not self.consumer:
            LOGGER.critical('Could not import and start processor')
            self.set_state(self.STATE_STOPPED)
            self.on_ready_to_stop()
            return

        self.stats_queue = stats_queue

        self.setup_instrumentation(cfg)

        # Consumer settings
        self.ack = self.config.get('ack', True)
        self.max_error_count = int(self.config.get('max_errors',
                                                   self.MAX_ERROR_COUNT))
        self.max_frame_size = self.config.get('max_frame_size',
                                              spec.FRAME_MAX_SIZE)
        self.queue_name = self.config['queue']

        self.reset_error_counter()
        self.setup_sighandlers()

    def setup_channel(self):
        """Setup the channel that will be used to communicate with RabbitMQ and
        set the QoS, send a Basic.Recover and set the channel object in the
        consumer object.

        """
        self.set_state(self.STATE_IDLE)

        # Set the channel in the consumer
        try:
            self.consumer._set_channel(self.channel)
        except AttributeError:
            LOGGER.info('Consumer does not support channel assignment')

        # Setup QoS, Send a Basic.Recover and then Basic.Consume
        self.channel.basic_qos(self.on_qos_set, 0, self.qos_prefetch, False)
        self.channel.basic_consume(consumer_callback=self.on_message,
                                   queue=self.queue_name,
                                   no_ack=not self.ack,
                                   consumer_tag=self.name)

    def setup_influxdb(self, config):
        """Configure the InfluxDB module for measurement submission.

        :param dict config: The InfluxDB configuration stanza

        """
        base_tags = {
            'connection': self.connection_name,
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

    def setup_instrumentation(self, config):
        """Configure instrumentation for submission per message measurements
        to statsd and/or InfluxDB.

        :param dict config: The application configuration stanza

        """
        if not config.get('stats'):
            if not config.get('statsd'):  # Backwards compatible statsd check
                return
            config['stats'] = {}

        # Backwards compatible statsd config support
        if config.get('statsd'):
            warnings.warn('Deprecated statsd configuration detected',
                          DeprecationWarning)
            config['stats'].setdefault('statsd', config.get('statsd'))

        if config['stats'].get('statsd'):
            self.statsd = statsd.Client(self.consumer_name,
                                        config['stats']['statsd'])
            LOGGER.debug('statsd measurements configured')

        # InfluxDB support
        if influxdb and config['stats'].get('influxdb'):
            self.influxdb = self.setup_influxdb(config['stats']['influxdb'])
            LOGGER.debug('InfluxDB measurements configured: %r', self.influxdb)

    def setup_sighandlers(self):
        """Setup the stats and stop signal handlers."""
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)

        signal.signal(signal.SIGPROF, self.on_sigprof)
        signal.signal(signal.SIGABRT, self.stop)

        signal.siginterrupt(signal.SIGPROF, False)
        signal.siginterrupt(signal.SIGABRT, False)
        LOGGER.debug('Signal handlers setup')

    def start_message_processing(self):
        """Keep track of the connection in case RabbitMQ disconnects while the
        message is processing.

        """
        self.message_connection_id = self.connection_id

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

        # Stop consuming
        self.cancel_consumer_with_rabbitmq()

        # Wait until the consumer has finished processing to shutdown
        if self.is_processing:
            LOGGER.info('Waiting for consumer to finish processing')
            self.set_state(self.STATE_STOP_REQUESTED)
            if signum == signal.SIGTERM:
                signal.siginterrupt(signal.SIGTERM, False)
            return

        self.on_ready_to_stop()

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
        influxdb.add_measurement(measurement)
        LOGGER.debug('InfluxDB Measurement: %r', measurement.marshall())

    def submit_statsd_measurements(self):
        """Submit a measurement for a message to statsd as individual items."""
        for key, value in self.measurement.counters.items():
            self.statsd.incr(key, value)
        for key, value in self.measurement.values.items():
            if isinstance(value, float):
                self.statsd.add_timing(key, value)
            else:
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
    def too_many_errors(self):
        """Return a bool if too many errors have occurred.

        :rtype: bool

        """
        return self.counters[self.ERROR] >= self.max_error_count
