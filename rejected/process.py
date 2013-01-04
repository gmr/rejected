"""Consumer process management. Imports consumer code, manages RabbitMQ
connection state and collects stats about the consuming process.

"""
from pika import exceptions
import logging
import math
import multiprocessing
import os
import pika
from pika.adapters import tornado_connection
import signal
import sys
import time
from tornado import ioloop
import traceback

from rejected import __version__
from rejected import consumer
from rejected import data
from rejected import state

LOGGER = logging.getLogger(__name__)


def import_namespaced_class(namespaced_class):
    """Pass in a string in the format of foo.Bar, foo.bar.Baz, foo.bar.baz.Qux
    and it will return a handle to the class

    :param str namespaced_class: The namespaced class
    :return: tuple(Class, str)

    """
    LOGGER.debug('Importing %s', namespaced_class)
    # Split up our string containing the import and class
    parts = namespaced_class.split('.')

    # Build our strings for the import name and the class name
    import_name = '.'.join(parts[0:-1])
    class_name = parts[-1]

    import_handle = __import__(import_name, fromlist=class_name)
    if hasattr(import_handle, '__version__'):
        version = import_handle.__version__
    else:
        version = None

    # Return the class handle
    return getattr(import_handle, class_name), version


class Process(multiprocessing.Process, state.State):
    """Core process class that

    """
    _AMQP_APP_ID = 'rejected/%s' % __version__
    _QOS_PREFETCH_COUNT = 1

    # Additional State constants
    STATE_PROCESSING = 0x04

    # Counter constants
    ACKED = 'acked'
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

    _HBINTERVAL = 30

    # Locations to search for newrelic ini files
    INI_DIRS = ['.', '/etc/', '/etc/newrelic']
    INI_FILE = 'newrelic.ini'

    # Default message pre-allocation value
    _QOS_PREFETCH_COUNT = 1
    _QOS_PREFETCH_MULTIPLIER = 1.25
    _QOS_MAX = 10000
    _MAX_ERROR_COUNT = 5
    _MAX_ERROR_WINDOW = 60
    _MAX_SHUTDOWN_WAIT = 5
    _RECONNECT_DELAY = 10

    def __init__(self, group=None, target=None, name=None, args=(),
                 kwargs=None):
        if kwargs is None:
            kwargs = {}
        super(Process, self).__init__(group, target, name, args, kwargs)
        self._ack = True
        self._application = None
        self._channel = None
        self._config = None
        self._connection_name = None
        self._connections = None
        self._consumer = None
        self._counts = self.new_counter_dict()
        self._dynamic_qos = True
        self._hbinterval = self._HBINTERVAL
        self._last_counts = None
        self._last_failure = 0
        self._last_stats_time = None
        self._max_framesize = pika.spec.FRAME_MAX_SIZE
        self._qos_prefetch = None
        self._state = self.STATE_INITIALIZING
        self._state_start = time.time()
        self._stats_queue = None

        # Override ACTIVE with PROCESSING
        self._STATES[0x04] = 'Processing'

    def ack_message(self, delivery_tag):
        """Acknowledge the message on the broker and log the ack

        :param str delivery_tag: Delivery tag to acknowledge

        """
        LOGGER.debug('Acking %s', delivery_tag)
        self._channel.basic_ack(delivery_tag=delivery_tag)
        self.increment_count(self.ACKED)

    def add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.

        """
        LOGGER.info('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def add_on_connection_close_callback(self):
        """This method adds an on close callback that will be invoked by pika
        when RabbitMQ closes the connection to the publisher unexpectedly.

        """
        LOGGER.info('Adding connection close callback')
        self._connection.add_on_close_callback(self.on_connection_closed)

    @property
    def base_qos_prefetch(self):
        """Return the base, configured QoS prefetch value.

        :rtype: int

        """
        return self._config.get('qos_prefetch', self._QOS_PREFETCH_COUNT)

    def calculate_qos_prefetch(self):
        """Determine if the channel should use the dynamic QoS value, stay at
        the same QoS or use the default QoS.

        :rtype: bool or int

        """
        qos_prefetch = self.dynamic_qos_pretch

        # Don't change anything
        if qos_prefetch == self._qos_prefetch:
            LOGGER.debug('No change in QoS prefetch calculation of %i',
                         self._qos_prefetch)
            return False

        # Don't change anything
        if self.count_processed_last_interval < qos_prefetch:
            LOGGER.error('Processed fewer messages last interval than the '
                         'qos_prefetch value')
            return False

        # If calculated QoS exceeds max
        if qos_prefetch > self._QOS_MAX:
            return self.set_qos_prefetch(self._QOS_MAX)

        # Set to base value if QoS calc is < than the base
        if self.base_qos_prefetch > qos_prefetch:
            LOGGER.debug('QoS calculation is lower than base: %i < %i',
                         qos_prefetch, self.base_qos_prefetch)
            return self.set_qos_prefetch()

        # Increase the QoS setting
        if qos_prefetch > self._qos_prefetch:
            LOGGER.debug('QoS calculation is higher than previous: %i > %i',
                         qos_prefetch, self._qos_prefetch)
            return self.set_qos_prefetch(qos_prefetch)

        # Lower the QoS value based upon the processed qty
        if qos_prefetch < self._qos_prefetch:
            LOGGER.debug('QoS calculation is lower than previous: %i < %i',
                         qos_prefetch, self._qos_prefetch)
            return self.set_qos_prefetch(qos_prefetch)

    def cancel_consumer_with_rabbitmq(self):
        """Tell RabbitMQ the process no longer wants to consumer messages."""
        LOGGER.info('Sending a Basic.Cancel to RabbitMQ')
        if self._channel and self._channel.is_open:
            self._channel.basic_cancel(consumer_tag=self.name)

    def close_connection(self):
        """This method closes the connection to RabbitMQ."""
        LOGGER.info('Closing connection')
        self._connection.close()

    def connect_to_rabbitmq(self, config, name):
        """Connect to RabbitMQ returning the connection handle.

        :param dict config: The Connections section of the configuration
        :param str name: The name of the connection
        :rtype: pika.adapters.tornado_conneciton.TornadoConnection

        """
        LOGGER.debug('Connecting to %s:%i:%s as %s',
                     config[name]['host'], config[name]['port'],
                     config[name]['vhost'], config[name]['user'])
        self.set_state(self.STATE_CONNECTING)
        parameters = self.get_connection_parameters(config[name]['host'],
                                                    config[name]['port'],
                                                    config[name]['vhost'],
                                                    config[name]['user'],
                                                    config[name]['pass'])
        return tornado_connection.TornadoConnection(parameters,
                                                    self.on_connection_open,
                                                    True)

    def count(self, stat):
        """Return the current count quantity for a specific stat.

        :param str stat: Name of stat to get value for
        :rtype: int or float

        """
        return self._counts.get(stat, 0)

    @property
    def count_processed_last_interval(self):
        """Return the number of messages counted in the last interval. If
        there is no last interval counts, return 0.

        :rtype: int

        """
        if not self._last_counts:
            return 0
        return self._counts[self.PROCESSED] - self._last_counts[self.PROCESSED]

    @property
    def dynamic_qos_pretch(self):
        """Calculate the prefetch count based upon the message velocity * the
        _QOS_PREFETCH_MULTIPLIER.

        :rtype: int

        """
        # Round up the velocity * the multiplier
        value = int(math.ceil(self.message_velocity *
                              float(self._QOS_PREFETCH_MULTIPLIER)))
        LOGGER.debug('Calculated prefetch value: %i', value)
        return value

    def get_config(self, config, number, name, connection):
        """Initialize a new consumer thread, setting defaults and config values

        :param dict config: Consumer config section from YAML File
        :param int number: The identification number for the consumer
        :param str name: The name of the consumer
        :param str connection: The name of the connection):
        :rtype: dict

        """
        return {'connection': config['Connections'][connection],
                'connection_name': connection,
                'consumer_name': name,
                'process_name': '%s_%i_tag_%i' % (name, os.getpid(), number)}

    def get_connection_parameters(self, host, port, vhost, username, password):
        """Return connection parameters for a pika connection.

        :param str host: The RabbitMQ host to connect to
        :param int port: The port to connect on
        :param str vhost: The virtual host
        :param str username: The username to use
        :param str password: The password to use
        :rtype: pika.ConnectionParameters

        """
        credentials = pika.PlainCredentials(username, password)
        return pika.ConnectionParameters(host, port, vhost, credentials,
                                         frame_max=self._max_framesize,
                                         heartbeat_interval=self._hbinterval)

    def get_consumer(self, config):
        """Import and create a new instance of the configured message consumer.

        :param dict config: The named consumer section of the configuration
        :rtype: instance
        :raises: ImportError

        """
        # Try and import the module
        try:
            consumer_, version = import_namespaced_class(config['consumer'])
        except Exception as error:
            LOGGER.error('Error importing the consumer "%s": %s',
                         config['consumer'], error)
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.extract_tb(exc_traceback)
            for line in lines:
                LOGGER.error(line)
            return

        if version:
            LOGGER.info('Creating consumer %s v%s', config['consumer'], version)
        else:
            LOGGER.info('Creating consumer %s', config['consumer'])

        # If we have a config, pass it in to the constructor
        if 'config' in config:
            try:
                return consumer_(config['config'])
            except Exception as error:
                LOGGER.error('Error creating the consumer "%s": %s',
                             config['consumer'], error)
                exc_type, exc_value, exc_traceback = sys.exc_info()
                lines = traceback.extract_tb(exc_traceback)
                for line in lines:
                    LOGGER.error(line)
                return

        # No config to pass
        try:
            return consumer_()
        except Exception as error:
            LOGGER.error('Error creating the consumer "%s": %s',
                         config['consumer'], error)
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.extract_tb(exc_traceback)
            for line in lines:
                LOGGER.error(line)
            return

    def increment_count(self, counter, value=1):
        """Increment the specified counter, checking to see if the counter is
        the error counter. if it is, check to see if there have been too many
        errors and if it needs to reconnect.

        :param str counter: The counter name passed in from the constant
        :param int|float value: The amount to increment by

        """
        self._counts[counter] += value

    @property
    def is_idle(self):
        """Is the system idle

        :rtype: bool

        """
        return self._state == self.STATE_IDLE

    @property
    def is_processing(self):
        """Returns a bool specifying if the consumer is currently processing

        :rtype: bool

        """
        return self._state in [self.STATE_PROCESSING, self.STATE_STOP_REQUESTED]

    @property
    def message_velocity(self):
        """Return the message consuming velocity for the process.

        :rtype: float

        """
        processed = self.count_processed_last_interval
        duration = time.time() - self._last_stats_time
        LOGGER.debug('Processed %i messages in %i seconds', processed, duration)

        # If there were no messages, do not calculate, use the base
        if not processed or not duration:
            return 0

        # Calculate the velocity as the basis for the calculation
        velocity = float(processed) / float(duration)
        LOGGER.debug('Message processing velocity: %.2f', velocity)
        return velocity

    def new_counter_dict(self):
        """Return a dict object for our internal stats keeping.

        :rtype: dict

        """
        return {self.ACKED: 0,
                self.ERROR: 0,
                self.FAILURES: 0,
                self.UNHANDLED_EXCEPTIONS: 0,
                self.PROCESSED: 0,
                self.RECONNECTED: 0,
                self.REDELIVERED: 0,
                self.REJECTED: 0,
                self.REQUEUED: 0,
                self.TIME_SPENT: 0,
                self.TIME_WAITED: 0}

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
        del self._channel
        raise ReconnectConnection

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened. It
        will change the state to IDLE, add the callbacks and setup the channel
        to start consuming.

        :param pika.channel.Channel channel: The channel object

        """
        LOGGER.info('Channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.set_state(self.STATE_IDLE)
        self.setup_channel()

    def on_connection_closed(self, unused):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection unused: The closed connection

        """
        LOGGER.critical('Connection from RabbitMQ closed: %r', unused)
        self._channel = None
        if not self.is_shutting_down:
            self.reconnect()

    def on_connection_open(self, unused):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.

        :type unused: pika.adapters.tornado_connection.TornadoConnection

        """
        LOGGER.info('Connection opened')
        self.add_on_connection_close_callback()
        self.open_channel()

    def on_ready_to_stop(self):

        # Set the state to shutting down if it wasn't set as that during loop
        self.set_state(self.STATE_SHUTTING_DOWN)

        # If the connection is still around, close it
        if self._connection.is_open:
            LOGGER.debug('Closing connection to RabbitMQ')
            self._connection.close()

        # Allow the consumer to gracefully stop and then stop the IOLoop
        self.stop_consumer()

        # Note that shutdown is complete and set the state accordingly
        LOGGER.info('Shutdown complete')
        self.set_state(self.STATE_STOPPED)

    def on_sigprof(self, unused_signum, unused_frame):
        """Called when SIGPROF is sent to the process, will dump the stats, in
        future versions, queue them for the master process to get data.

        :param int unused_signum: The signal number
        :param frame unused_frame: The python frame the signal was received at

        """
        LOGGER.info('Currently %s: %r', self.state_description, self._counts)

    def open_channel(self):
        """Open a channel on the existing open connection to RabbitMQ"""
        LOGGER.info('Opening a channel on %r', self._connection)
        self._connection.channel(self.on_channel_open)

    def process(self, channel=None, method=None, header=None, body=None):
        """Process a message from Rabbit

        :param pika.channel.Channel channel: The channel the message was sent on
        :param pika.frames.MethodFrame method: The method frame
        :param pika.frames.HeaderFrame header: The header frame
        :param str body: The message body

        """
        if not self.is_idle:
            LOGGER.critical('Received a message while in state: %s',
                            self.state_description)
            return self.reject(method.delivery_tag, True)
        self.set_state(self.STATE_PROCESSING)
        LOGGER.debug('Received message #%s', method.delivery_tag)
        message = data.Message(channel, method, header, body)
        if method.redelivered:
            self.increment_count(self.REDELIVERED)
        self._state_start = time.time()
        if not self._process(message):
            LOGGER.debug('Bypassing ack due to False return from _process')
            return
        self.increment_count(self.PROCESSED)
        if self._ack:
            self.ack_message(method.delivery_tag)
        self.reset_state()

    def processing_failure(self):
        """Called when message processing failure happens due to a
        ConsumerException or an unhandled exception.

        """
        duration = time.time() - self._last_failure
        if duration > self._MAX_ERROR_WINDOW:
            LOGGER.info('Resetting failure window, %i seconds since last',
                        duration)
            self.reset_failure_counter()
        self.increment_count(self.FAILURES, -1)
        self._last_failure = time.time()
        if self._counts[self.FAILURES] == 0:
            LOGGER.critical('Error threshold exceeded (%i), reconnecting',
                            self._counts[self.ERROR])
            self.cancel_consumer_with_rabbitmq()
            self.close_connection()
            self.reconnect()

    def reconnect(self):
        """Reconnect to RabbitMQ after sleeping for _RECONNECT_DELAY"""
        LOGGER.info('Reconnecting to RabbitMQ in %i seconds',
                    self._RECONNECT_DELAY)
        self.increment_count(self.RECONNECTED)
        self.set_state(self.STATE_INITIALIZING)
        if self._connection:
            if self._connection.socket:
                fd = self._connection.socket.fileno()
                self._connection.ioloop.remove_handler(fd)
            self._connection.ioloop.stop()
            del self._connection
        else:
            _ioloop = ioloop.IOLoop.instance()
            _ioloop.stop()
        time.sleep(self._RECONNECT_DELAY)
        self._connection = self.connect_to_rabbitmq(self._connections,
                                                    self._connection_name)

    def record_exception(self, error, handled=False):
        """Record an exception

        :param exception error: The exception to record
        :param bool handled: Was the exception handled

        """
        formatted_lines = traceback.format_exc().splitlines()
        self.increment_count(self.ERROR)
        if handled:
            LOGGER.warning('Processor handled %s: %s',
                           error.__class__.__name__, error)
        else:
            LOGGER.critical('Processor threw an uncaught exception %s: %s',
                            error.__class__.__name__, error)
            self.increment_count(self.UNHANDLED_EXCEPTIONS)
        for offset, line in enumerate(formatted_lines):
            LOGGER.debug('(%s) %i: %s', error.__class__.__name__,
                         offset, line.strip())

    def reject(self, delivery_tag, requeue=True):
        """Reject the message on the broker and log it. We should move this to
         use to nack when Pika supports it in a released version.

        :param str delivery_tag: Delivery tag to reject
        :param bool requeue: Specify if the message should be re-queued or not

        """
        if not self._ack:
            raise RuntimeError('Can not rejected messages when ack is False')
        LOGGER.warning('Rejecting message %s %s requeue', delivery_tag,
                       'with' if requeue else 'without')
        self._channel.basic_nack(delivery_tag=delivery_tag, requeue=requeue)
        self.increment_count(self.REQUEUED if requeue else self.REJECTED)
        if self.is_processing:
            self.reset_state()

    def reset_failure_counter(self):
        """Reset the failure counter to the max error count"""
        LOGGER.debug('Resetting the failure counter to %i',
                     self._max_error_count)
        self._counts[self.FAILURES] = self._max_error_count

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
        try:
            self.setup(self._kwargs['config'],
                       self._kwargs['connection_name'],
                       self._kwargs['consumer_name'],
                       self._kwargs['stats_queue'])
        except ImportError as error:
            name = self._kwargs['consumer_name']
            consumer = self._kwargs['config']['Consumers'][name]['consumer']
            LOGGER.critical('Could not import %s, stopping process: %r',
                            consumer, error)
            return

        while not self.is_waiting_to_shutdown:
            try:
                self._connection.ioloop.start()
            except KeyboardInterrupt:
                self.stop()
                self._connection.ioloop.start()
            while not self._connection and self.is_connecting:
                time.sleep(0.1)
        LOGGER.debug('Exiting %s', self.name)

    def set_qos_prefetch(self, value=None):
        """Set the QOS Prefetch count for the channel.

        :param int value: The value to set the prefetch to

        """
        qos_prefetch = int(value or self.base_qos_prefetch)
        if qos_prefetch != self._qos_prefetch:
            self._qos_prefetch = qos_prefetch
            LOGGER.info('Setting the QOS Prefetch to %i', qos_prefetch)
            self._channel.basic_qos(prefetch_count=qos_prefetch)

    def set_state(self, new_state):
        """Assign the specified state to this consumer object.

        :param int new_state: The new state of the object
        :raises: ValueError

        """
        # Keep track of how much time we're spending waiting and processing
        if new_state == self.STATE_PROCESSING and self.is_idle:
            self.increment_count(self.TIME_WAITED, self.time_in_state)

        elif new_state == self.STATE_IDLE and self.is_processing:
            self.increment_count(self.TIME_SPENT, self.time_in_state)

        # Use the parent object to set the state
        super(Process, self)._set_state(new_state)

    def setup(self, config, connection_name, consumer_name, stats_queue):
        """Initialize the consumer, setting up needed attributes and connecting
        to RabbitMQ.

        :param dict config: Consumer config section
        :param str connection_name: The name of the connection
        :param str consumer_name: Consumer name for config
        :param multiprocessing.Queue stats_queue: The queue to append stats in
        :raises: ImportError

        """
        LOGGER.info('Initializing for %s on %s', self.name, connection_name)

        # The queue for populating stats data
        self._stats_queue = stats_queue

        # Hold the consumer config
        self._connection_name = connection_name
        self._consumer_name = consumer_name
        self._config = config['Consumers'][consumer_name]
        self._connections = config['Connections']

        # Setup the consumer
        self._consumer = self.get_consumer(self._config)
        if not self._consumer:
            raise ImportError('Could not import and start processor')

        # Set the routing information
        self._queue_name = self._config['queue']

        # Set the dynamic QoS toggle
        self._dynamic_qos = self._config.get('dynamic_qos', True)

        # Set the various control nobs
        self._ack = self._config.get('ack', True)

        # How many errors until the process stops
        self._max_error_count = int(self._config.get('max_errors',
                                                     self._MAX_ERROR_COUNT))
        self.reset_failure_counter()

        # Get the heartbeat interval
        self._hbinterval = self._config.get('heartbeat_interval',
                                            self._HBINTERVAL)

        # Get the framesize
        self._max_framesize = self._config.get('max_frame_size',
                                               pika.spec.FRAME_MAX_SIZE)

        # Setup the signal handler for stats
        self.setup_signal_handlers()

        # Create the RabbitMQ Connection
        self._connection = self.connect_to_rabbitmq(self._connections,
                                                    self._connection_name)

    def setup_channel(self):
        """Setup the channel that will be used to communicate with RabbitMQ and
        set the QoS, send a Basic.Recover and set the channel object in the
        consumer object.

        """
        # Set the channel in the consumer
        try:
            self._consumer.set_channel(self._channel)
        except AttributeError:
            LOGGER.debug('Consumer does not support channel assignment')

        # Setup QoS, Send a Basic.Recover and then Basic.Consume
        self.set_qos_prefetch()
        self._channel.basic_recover(requeue=True)
        self._channel.basic_consume(consumer_callback=self.process,
                                    queue=self._queue_name,
                                    no_ack=not self._ack,
                                    consumer_tag=self.name)

    def setup_signal_handlers(self):
        """Setup the stats and stop signal handlers. Use SIGABRT instead of
        SIGTERM due to the multiprocessing's behavior with SIGTERM.

        """
        signal.signal(signal.SIGABRT, self.stop)
        signal.signal(signal.SIGPROF, self.on_sigprof)
        signal.siginterrupt(signal.SIGABRT, False)
        signal.siginterrupt(signal.SIGPROF, False)

    def stop(self, signum=None, frame_unused=None):
        """Stop the consumer from consuming by calling BasicCancel and setting
        our state.

        """
        LOGGER.debug('Stop called in state: %s', self.state_description)
        if self.is_stopped:
            LOGGER.debug('Stop requested but consumer is already stopped')
            return
        elif self.is_shutting_down:
            LOGGER.debug('Stop requested but consumer is already shutting down')
            return
        elif self.is_waiting_to_shutdown:
            LOGGER.debug('Stop requested but already waiting to shut down')
            return

        LOGGER.info('Shutting down')

        # Stop consuming
        self.cancel_consumer_with_rabbitmq()

        # Wait until the consumer has finished processing to shutdown
        if self.is_processing:
            self.set_state(self.STATE_STOP_REQUESTED)
            if signum == signal.SIGABRT:
                signal.siginterrupt(signal.SIGABRT, False)
            return

        self.on_ready_to_stop()

    def stop_consumer(self):
        """Stop the consumer object and allow it to do a clean shutdown if it
        has the ability to do so.

        """
        try:
            LOGGER.info('Shutting down the consumer')
            self._consumer.shutdown()
        except AttributeError:
            LOGGER.debug('Consumer does not have a shutdown method')

    @property
    def time_in_state(self):
        """Return the time that has been spent in the current state.

        :rtype: float

        """
        return time.time() - self._state_start

    @property
    def too_many_errors(self):
        """Return a bool if too many errors have occurred.

        :rtype: bool

        """
        return self.count(self.ERROR) >= self._max_error_count

    def _process(self, message):
        """Wrap the actual processor processing bits

        :param Message message: Message to process
        :raises: consumer.ConsumerException

        """
        # Try and process the message
        try:
            self._consumer.process(message)

        except KeyboardInterrupt:
            self.reject(message.delivery_tag, True)
            self.stop()
            return False

        except exceptions.ChannelClosed as error:
            LOGGER.critical('RabbitMQ closed the channel: %r', error)
            self.reconnect()
            return False

        except exceptions.ConnectionClosed as error:
            LOGGER.critical('RabbitMQ closed the connection: %r', error)
            self.reconnect()
            return False

        except consumer.ConsumerException as error:
            self.record_exception(error, True)
            self.reject(message.delivery_tag, True)
            self.processing_failure()
            return False

        except consumer.MessageException as error:
            self.record_exception(error, True)
            self.reject(message.delivery_tag, False)
            return False

        except Exception as error:
            self.record_exception(error, False)
            self.reject(message.delivery_tag, True)
            self.processing_failure()
            return False

        return True


class ReconnectConnection(Exception):
    pass
