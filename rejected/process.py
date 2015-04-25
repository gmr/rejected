"""
Consumer process management. Imports consumer code, manages RabbitMQ
connection state and collects stats about the consuming process.

"""
from pika import exceptions
from tornado import gen
import importlib
import logging
import math
import multiprocessing
import os
from os import path
import pika
try:
    import cProfile as profile
except ImportError:
    import profile
from pika import spec
from pika.adapters import tornado_connection
import signal
import sys
import time
from tornado import ioloop
import traceback

from rejected import __version__
from rejected import consumer
from rejected import data
from rejected import NullHandler
from rejected import state
from rejected import stats

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
    if hasattr(import_handle, '__version__'):
        version = import_handle.__version__
    else:
        version = None

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
    ERROR = 'failed'
    FAILURES = 'failures_until_stop'
    PROCESSED = 'processed'
    RECONNECTED = 'reconnected'
    REDELIVERED = 'redelivered_messages'
    REJECTED = 'rejected_messages'
    REQUEUED = 'requeued_messages'
    TIME_SPENT = 'processing_time'
    TIME_WAITED = 'idle_time'
    UNHANDLED_EXCEPTIONS = 'unhandled_exceptions'

    HB_INTERVAL = 300

    # Default message pre-allocation value
    QOS_PREFETCH_COUNT = 1
    QOS_PREFETCH_MULTIPLIER = 1.25
    QOS_MAX = 10000
    MAX_ERROR_COUNT = 5
    MAX_ERROR_WINDOW = 60
    MAX_SHUTDOWN_WAIT = 5
    RECONNECT_DELAY = 10

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
        self.channel = None
        self.config = None
        self.connection = None
        self.connection_id = 0
        self.connection_name = None
        self.connections = None
        self.consumer = None
        self.consumer_name = None
        self.dynamic_qos = True
        self.ioloop = None
        self.last_failure = 0
        self.last_stats_time = None
        self.logging_config = dict()
        self.message_connection_id = None
        self.max_error_count = self.MAX_ERROR_COUNT
        self.max_frame_size = spec.FRAME_MAX_SIZE
        self.qos_prefetch = None
        self.queue_name = None
        self.prepend_path = None
        self.state = self.STATE_INITIALIZING
        self.state_start = time.time()
        self.stats = None
        self.stats_queue = None

        # Override ACTIVE with PROCESSING
        self.STATES[0x04] = 'Processing'

    def ack_message(self, delivery_tag):
        """Acknowledge the message on the broker and log the ack

        :param str delivery_tag: Delivery tag to acknowledge

        """
        if not self.can_respond:
            LOGGER.warning('Can not ack message, disconnected from RabbitMQ')
            self.stats.incr(self.CLOSED_ON_COMPLETE)
            return
        LOGGER.debug('Acking %s', delivery_tag)
        self.channel.basic_ack(delivery_tag=delivery_tag)
        self.stats.incr(self.ACKED)

    def add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.

        """
        LOGGER.debug('Adding channel close callback')
        self.channel.add_on_close_callback(self.on_channel_closed)

    def add_on_connection_close_callback(self):
        """This method adds an on close callback that will be invoked by pika
        when RabbitMQ closes the connection to the publisher unexpectedly.

        """
        LOGGER.debug('Adding connection close callback')
        self.connection.add_on_close_callback(self.on_connection_closed)

    @property
    def base_qos_prefetch(self):
        """Return the base, configured QoS prefetch value.

        :rtype: int

        """
        return self.config.get('qos_prefetch', self.QOS_PREFETCH_COUNT)

    def calc_qos_prefetch(self, values):
        """Determine if the channel should use the dynamic QoS value, stay at
        the same QoS or use the default QoS.

        :rtype: bool or int

        """
        if not self.last_stats_time:
            return

        velocity = self.calc_velocity(values)
        qos_prefetch = int(math.ceil(velocity *
                                     float(self.QOS_PREFETCH_MULTIPLIER)))

        # Don't change anything
        if qos_prefetch == self.qos_prefetch:
            LOGGER.debug('No change in QoS prefetch calculation of %i',
                         self.qos_prefetch)
            return False

        # If calculated QoS exceeds max
        if qos_prefetch > self.QOS_MAX:
            LOGGER.debug('Hit QoS Max ceiling of %i', self.QOS_MAX)
            return self.set_qos_prefetch(self.QOS_MAX)

        # Set to base value if QoS calc is < than the base
        if self.base_qos_prefetch > qos_prefetch:
            LOGGER.debug('QoS calculation is lower than base: %i < %i',
                         qos_prefetch, self.base_qos_prefetch)
            return self.set_qos_prefetch()

        # Increase the QoS setting
        if qos_prefetch > self.qos_prefetch:
            LOGGER.debug('QoS calculation is higher than previous: %i > %i',
                         qos_prefetch, self.qos_prefetch)
            return self.set_qos_prefetch(qos_prefetch)

        # Lower the QoS value based upon the processed qty
        if qos_prefetch < self.qos_prefetch:
            LOGGER.debug('QoS calculation is lower than previous: %i < %i',
                         qos_prefetch, self.qos_prefetch)
            return self.set_qos_prefetch(qos_prefetch)

    def calc_velocity(self, values):
        """Return the message consuming velocity for the process.

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

    def connect_to_rabbitmq(self, cfg, name):
        """Connect to RabbitMQ returning the connection handle.

        :param dict cfg: The Connections section of the configuration
        :param str name: The name of the connection
        :rtype: pika.adapters.tornado_connection.TornadoConnection

        """
        LOGGER.debug('Connecting to %s:%i:%s as %s', cfg[name]['host'],
                     cfg[name]['port'], cfg[name]['vhost'], cfg[name]['user'])
        self.set_state(self.STATE_CONNECTING)
        self.connection_id += 1
        hb_interval = cfg[name].get('heartbeat_interval', self.HB_INTERVAL)
        parameters = self.get_connection_parameters(
            cfg[name]['host'], cfg[name]['port'], cfg[name]['vhost'],
            cfg[name]['user'], cfg[name]['pass'], hb_interval)
        return tornado_connection.TornadoConnection(parameters,
                                                    self.on_connection_open,
                                                    stop_ioloop_on_close=False)

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

    def get_connection_parameters(self, host, port, vhost, username, password,
                                  heartbeat_interval):
        """Return connection parameters for a pika connection.

        :param str host: The RabbitMQ host to connect to
        :param int port: The port to connect on
        :param str vhost: The virtual host
        :param str username: The username to use
        :param str password: The password to use
        :param int heartbeat_interval: AMQP Heartbeat interval
        :rtype: pika.ConnectionParameters

        """
        credentials = pika.PlainCredentials(username, password)
        return pika.ConnectionParameters(host, port, vhost, credentials,
                                         frame_max=self.max_frame_size,
                                         socket_timeout=10,
                                         heartbeat_interval=heartbeat_interval)

    @staticmethod
    def get_consumer(cfg):
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
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.extract_tb(exc_traceback)
            for line in lines:
                LOGGER.error(line)
            return

        if version:
            LOGGER.info('Creating consumer %s v%s', cfg['consumer'], version)
        else:
            LOGGER.info('Creating consumer %s', cfg['consumer'])

        kwargs = {}
        if 'config' in cfg:
            kwargs['configuration'] = cfg.get('config', dict())

        try:
            return consumer_(**kwargs)
        except TypeError:
            return consumer_(cfg.get('config', dict()))
        except Exception as error:
            LOGGER.error('Error creating the consumer "%s": %s',
                         cfg['consumer'], error)
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.extract_tb(exc_traceback)
            for line in lines:
                LOGGER.error(line)
            return

    def on_processed(self, start_time, method, result):
        LOGGER.debug('Post invoke consumer')
        self.stats.add_timing(self.TIME_SPENT, time.time() - start_time)

        if not result:
            LOGGER.debug('Bypassing ack due to False return consumer')
            return

        self.stats.incr(self.PROCESSED)
        if self.ack:
            self.ack_message(method.delivery_tag)
        if self.is_waiting_to_shutdown:
            self.on_ready_to_stop()
        else:
            self.reset_state()

    @gen.engine
    def invoke_consumer(self, method, message):
        """Wrap the actual processor processing bits

        :param Message message: Message to process
        :raises: consumer.ConsumerException

        """
        self.start_message_processing()
        start_time = time.time()

        # Try and process the message
        try:
            result = self.consumer._execute(message)
            yield result
            possible_exception = result.exception()
            if possible_exception:
                raise possible_exception
            self.on_processed(start_time, method, True)

        except KeyboardInterrupt:
            LOGGER.debug('CTRL-C')
            self.reject(message.delivery_tag, True)
            self.stop()
            self.on_processed(start_time, method, False)

        except exceptions.ChannelClosed as error:
            LOGGER.critical('RabbitMQ closed the channel: %r', error)
            self.reconnect()
            self.on_processed(start_time, method, False)

        except exceptions.ConnectionClosed as error:
            LOGGER.critical('RabbitMQ closed the connection: %r', error)
            self.reconnect()
            self.on_processed(start_time, method, False)

        except consumer.ConsumerException as error:
            LOGGER.debug('Consumer Exception')
            self.record_exception(error, True, sys.exc_info())
            self.reject(message.delivery_tag, True)
            self.processing_error()
            self.on_processed(start_time, method, False)

        except consumer.MessageException as error:
            LOGGER.debug('Message Exception')
            self.record_exception(error, True, sys.exc_info())
            self.reject(message.delivery_tag, False)
            self.on_processed(start_time, method, False)

        except Exception as error:
            LOGGER.debug('Exception')
            self.record_exception(error, True, sys.exc_info())
            self.reject(message.delivery_tag, True)
            self.processing_error()
            self.on_processed(start_time, method, False)

    @property
    def is_processing(self):
        """Returns a bool specifying if the consumer is currently processing

        :rtype: bool

        """
        return self.state in [self.STATE_PROCESSING, self.STATE_STOP_REQUESTED]

    def on_channel_closed(self, method_frame):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.frame.Method method_frame: The Channel.Close method frame

        """
        LOGGER.critical('Channel was closed: (%s) %s',
                        method_frame.method.reply_code,
                        method_frame.method.reply_text)
        del self.channel
        raise ReconnectConnection

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

    def on_connection_closed(self, _unused, code, text):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection _unused: The closed connection

        """
        LOGGER.critical('Connection from RabbitMQ closed in state %s (%s, %s)',
                        self.state_description, code, text)
        self.channel = None
        if not self.is_shutting_down and not self.is_waiting_to_shutdown:
            self.reconnect()

    def on_connection_open(self, _unused):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.

        :type _unused: pika.adapters.tornado_connection.TornadoConnection

        """
        LOGGER.debug('Connection opened')
        self.add_on_connection_close_callback()
        self.open_channel()

    def on_ready_to_stop(self):

        # Set the state to shutting down if it wasn't set as that during loop
        self.set_state(self.STATE_SHUTTING_DOWN)

        # Reset any signal handlers
        signal.signal(signal.SIGABRT, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGPROF, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)

        # If the connection is still around, close it
        if self.connection.is_open:
            LOGGER.debug('Closing connection to RabbitMQ')
            self.connection.close()

        # Allow the consumer to gracefully stop and then stop the IOLoop
        self.stop_consumer()

        # Stop the IOLoop
        LOGGER.debug('Stopping IOLoop')
        self.ioloop.stop()

        # Note that shutdown is complete and set the state accordingly
        self.set_state(self.STATE_STOPPED)
        LOGGER.info('Shutdown complete')
        os.kill(os.getppid(), signal.SIGALRM)

    def on_sigprof(self, _unused_signum, _unused_frame):
        """Called when SIGPROF is sent to the process, will dump the stats, in
        future versions, queue them for the master process to get data.

        :param int _unused_signum: The signal number
        :param frame _unused_frame: The python frame the signal was received at

        """
        values = self.stats.report()
        self.stats_queue.put(values, True)
        if self.is_processing or self.is_idle:
            self.calc_qos_prefetch(values)
        self.last_stats_time = time.time()
        signal.siginterrupt(signal.SIGPROF, False)

    def open_channel(self):
        """Open a channel on the existing open connection to RabbitMQ"""
        LOGGER.debug('Opening a channel on %r', self.connection)
        self.connection.channel(self.on_channel_open)

    def process(self, channel=None, method=None, properties=None, body=None):
        """Process a message from Rabbit

        :param pika.channel.Channel channel: The channel the message was sent on
        :param pika.frames.MethodFrame method: The method frame
        :param pika.spec.BasicProperties properties: The message properties
        :param str body: The message body

        """
        if not self.is_idle:
            LOGGER.critical('Received a message while in state: %s',
                            self.state_description)
            return self.reject(method.delivery_tag, True)
        self.set_state(self.STATE_PROCESSING)
        message = data.Message(channel, method, properties, body)
        if method.redelivered:
            self.stats.incr(self.REDELIVERED)
        self.invoke_consumer(method, message)

    def processing_error(self):
        """Called when message processing failure happens due to a
        ConsumerException or an unhandled exception.

        """
        duration = time.time() - self.last_failure
        if duration > self.MAX_ERROR_WINDOW:
            LOGGER.info('Resetting failure window, %i seconds since last',
                        duration)
            self.reset_error_counter()
        self.stats.incr(self.ERROR)
        self.last_failure = time.time()
        if self.too_many_errors:
            LOGGER.critical('Error threshold exceeded (%i), reconnecting',
                            self.stats[self.ERROR])
            self.cancel_consumer_with_rabbitmq()
            self.close_connection()
            self.reconnect()

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

    def reconnect(self):
        """Reconnect to RabbitMQ after sleeping for _RECONNECT_DELAY"""
        LOGGER.info('Reconnecting to RabbitMQ in %i seconds',
                    self.RECONNECT_DELAY)
        self.stats.incr(self.RECONNECTED)
        self.set_state(self.STATE_INITIALIZING)
        if self.connection:
            if self.connection.socket:
                fd = self.connection.socket.fileno()
                self.ioloop.remove_handler(fd)
            self.connection = None

        self.ioloop.add_timeout(time.time() + self.RECONNECT_DELAY,
                                self._reconnect)

    def _reconnect(self):
        """Create and set the RabbitMQ connection"""
        LOGGER.info('Connecting to RabbitMQ')
        self.reset_error_counter()
        self.connection = self.connect_to_rabbitmq(self.connections,
                                                   self.connection_name)
        self.setup_signal_handlers()

    def record_exception(self, error, handled=False, exc_info=None):
        """Record an exception

        :param exception error: The exception to record
        :param bool handled: Was the exception handled

        """
        self.stats.incr(self.ERROR)
        if handled:
            if not isinstance(error, consumer.MessageException):
                LOGGER.exception('Processor handled %s: %s',
                                 error.__class__.__name__, error)
        else:
            LOGGER.exception('Processor threw an uncaught exception %s: %s',
                             error.__class__.__name__, error)
            self.stats.incr(self.UNHANDLED_EXCEPTIONS)
        if not isinstance(error, consumer.MessageException) and exc_info:
            formatted_lines = traceback.format_exception(*exc_info)
            for offset, line in enumerate(formatted_lines):
                LOGGER.debug('(%s) %i: %s', error.__class__.__name__, offset,
                             line.strip())

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
            self.stats.incr(self.CLOSED_ON_COMPLETE)
            if self.is_processing:
                self.reset_state()
            return
        LOGGER.warning('Rejecting message %s %s requeue', delivery_tag, 'with'
                       if requeue else 'without')
        self.channel.basic_nack(delivery_tag=delivery_tag, requeue=requeue)
        self.stats.incr(self.REQUEUED if requeue else self.REJECTED)
        if self.is_processing:
            self.reset_state()

    def reset_error_counter(self):
        """Reset the error counter to 0"""
        LOGGER.debug('Resetting the error counter')
        self.stats[self.ERROR] = 0

    def reset_state(self):
        """Reset the runtime state after processing a message to either idle
        or shutting down based upon the current state.

        """
        if self.is_waiting_to_shutdown:
            self.set_state(self.STATE_SHUTTING_DOWN)
            self.on_ready_to_stop()
        elif self.is_processing:
            self.set_state(self.STATE_IDLE)
        elif self.is_idle or self.is_connecting:
            pass
        else:
            LOGGER.critical('Unexepected state: %s', self.state_description)

    def run(self):
        """Start the consumer"""
        logger = logging.getLogger()
        logger.addHandler(NullHandler())
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
        self.ioloop = ioloop.IOLoop.instance()
        try:
            self.setup(self._kwargs['config'],
                       self._kwargs['consumer_name'],
                       self._kwargs['connection_name'],
                       self._kwargs['stats_queue'],
                       self._kwargs['logging_config'])
        except ImportError as error:
            name = self._kwargs['consumer_name']
            class_name = self._kwargs['config']['Consumers'][name]['consumer']
            LOGGER.critical('Could not import %s, stopping process: %r',
                            class_name, error)
            return

        if not self.is_stopped:
            try:
                self.ioloop.start()
            except KeyboardInterrupt:
                LOGGER.warning('CTRL-C while waiting for clean shutdown')

    def set_qos_prefetch(self, value=None):
        """Set the QOS Prefetch count for the channel.

        :param int value: The value to set the prefetch to

        """
        qos_prefetch = int(value or self.base_qos_prefetch)
        if qos_prefetch != self.qos_prefetch:
            self.qos_prefetch = qos_prefetch
            LOGGER.debug('Setting the QOS Prefetch to %i', qos_prefetch)
            self.channel.basic_qos(self.on_qos_set, 0, qos_prefetch, False)

    def on_qos_set(self, frame):
        """Invoked by pika when the QoS is set"""
        LOGGER.debug("QoS was set: %r", frame)

    def setup(self, cfg, consumer_name, connection_name, stats_queue,
              logging_config):
        """Initialize the consumer, setting up needed attributes and connecting
        to RabbitMQ.

        :param dict cfg: Consumer config section
        :param str consumer_name: Consumer name for config
        :param str connection_name: The name of the connection
        :param multiprocessing.Queue stats_queue: Queue to MCP
        :param dict logging_config: Logging config from YAML file

        """
        LOGGER.info('Initializing for %s on %s connection', self.name,
                    connection_name)
        self.connection_name = connection_name
        self.consumer_name = consumer_name
        self.config = cfg['Consumers'][consumer_name]
        self.connections = cfg['Connections']
        self.consumer = self.get_consumer(self.config)
        self.stats_queue = stats_queue

        if not self.consumer:
            LOGGER.critical('Could not import and start processor')
            self.set_state(self.STATE_STOPPED)
            exit(1)

        # Setup the stats counter instance
        self.stats = stats.Stats(self.name, consumer_name, cfg['statsd'] or {})

        # Set statsd in the consumer
        if self.stats.statsd:
            try:
                self.consumer._set_statsd(self.stats.statsd)
            except AttributeError:
                LOGGER.info('Consumer does not support statsd assignment')

        # Consumer settings
        self.ack = self.config.get('ack', True)
        self.dynamic_qos = self.config.get('dynamic_qos', False)
        self.max_error_count = int(self.config.get('max_errors',
                                                   self.MAX_ERROR_COUNT))
        self.max_frame_size = self.config.get('max_frame_size',
                                              spec.FRAME_MAX_SIZE)
        self.queue_name = self.config['queue']

        self.reset_error_counter()
        self.setup_signal_handlers()
        self.connection = self.connect_to_rabbitmq(self.connections,
                                                   self.connection_name)

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
        self.set_qos_prefetch()
        self.channel.basic_recover(requeue=True)
        self.channel.basic_consume(consumer_callback=self.process,
                                   queue=self.queue_name,
                                   no_ack=not self.ack,
                                   consumer_tag=self.name)

    def setup_signal_handlers(self):
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

    def stop(self, signum=None, _frame_unused=None):
        """Stop the consumer from consuming by calling BasicCancel and setting
        our state.

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

    @property
    def too_many_errors(self):
        """Return a bool if too many errors have occurred.

        :rtype: bool

        """
        return self.stats[self.ERROR] >= self.max_error_count


class ReconnectConnection(Exception):
    pass
