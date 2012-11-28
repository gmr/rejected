"""Consumer process management. Imports consumer code, manages RabbitMQ
connection state and collects stats about the consuming process.

"""
import copy
from tornado import ioloop
import logging
import math
import multiprocessing
import os
import pika
from pika import exceptions
from pika.adapters import tornado_connection
import signal
import sys
import threading
import time
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
    ERROR = 'failed'
    PROCESSED = 'processed'
    REDELIVERED = 'redelivered_messages'
    REJECTED = 'rejected_messages'
    TIME_SPENT = 'processing_time'
    TIME_WAITED = 'waiting_time'

    _HBINTERVAL = 10

    # Default message pre-allocation value
    _QOS_PREFETCH_COUNT = 1
    _QOS_PREFETCH_MULTIPLIER = 1.25
    _QOS_MAX = 10000
    _MAX_ERROR_COUNT = 5
    _MAX_SHUTDOWN_WAIT = 5

    def __init__(self, group=None, target=None, name=None, args=(), kwargs={}):
        super(Process, self).__init__(group, target, name, args, kwargs)
        self._ack = True
        self._channel = None
        self._config= None
        self._consumer = None
        self._counts = self._new_counter_dict()
        self._dynamic_qos = True
        self._hbinterval = self._HBINTERVAL
        self._last_counts = None
        self._last_stats_time = None
        self._max_framesize = pika.spec.FRAME_MAX_SIZE
        self._qos_prefetch = None
        self._state = self.STATE_INITIALIZING
        self._state_start = time.time()
        self._stats_queue = None

        # Override ACTIVE with PROCESSING
        self._STATES[0x04] = 'Processing'

    def _ack_message(self, delivery_tag):
        """Acknowledge the message on the broker and log the ack

        :param str delivery_tag: Delivery tag to acknowledge

        """
        LOGGER.debug('Acking %s', delivery_tag)
        self._channel.basic_ack(delivery_tag=delivery_tag)
        self._reset_state()

    @property
    def _base_qos_prefetch(self):
        """Return the base, configured QoS prefetch value.

        :rtype: int

        """
        return self._config.get('qos_prefetch', self._QOS_PREFETCH_COUNT)

    def _calculate_qos_prefetch(self):
        """Determine if the channel should use the dynamic QoS value, stay at
        the same QoS or use the default QoS.

        """
        qos_prefetch = self._dynamic_qos_pretch

        # Don't change anything
        if qos_prefetch == self._qos_prefetch:
            LOGGER.debug('No change in QoS prefetch calculation of %i',
                         self._qos_prefetch)
            return

        # Don't change anything
        if self._count_processed_last_interval < qos_prefetch:
            LOGGER.error('Processed fewer messages last interval than the '
                         'qos_prefetch value')
            return

        # If calculated QoS exceeds max
        if qos_prefetch > self._QOS_MAX:
            return self._set_qos_prefetch(self._QOS_MAX)

        # Set to base value if QoS calc is < than the base
        if self._base_qos_prefetch > qos_prefetch:
            LOGGER.debug('QoS calculation is lower than base: %i < %i',
                         qos_prefetch, self._base_qos_prefetch)
            return self._set_qos_prefetch()

        # Increase the QoS setting
        if qos_prefetch > self._qos_prefetch:
            LOGGER.debug('QoS calculation is higher than previous: %i > %i',
                         qos_prefetch, self._qos_prefetch)
            return self._set_qos_prefetch(qos_prefetch)

        # Lower the QoS value based upon the processed qty
        if qos_prefetch < self._qos_prefetch:
            LOGGER.debug('QoS calculation is lower than previous: %i < %i',
                         qos_prefetch, self._qos_prefetch)
            return self._set_qos_prefetch(qos_prefetch)

    @property
    def _count_processed_last_interval(self):
        """Return the number of messages counted in the last interval. If
        there is no last interval counts, return 0.

        :rtype: int

        """
        if not self._last_counts:
            return 0
        return self._counts[self.PROCESSED] - self._last_counts[self.PROCESSED]

    def _cancel_consumer_with_rabbitmq(self):
        """Tell RabbitMQ the process no longer wants to consumer messages."""
        LOGGER.debug('Sending a Basic.Cancel to RabbitMQ')
        if self._channel and self._channel.is_open:
            self._channel.basic_cancel(consumer_tag=self.name)

    def _count(self, stat):
        """Return the current count quantity for a specific stat.

        :param str stat: Name of stat to get value for
        :rtype: int or float

        """
        return self._counts.get(stat, 0)

    @property
    def _dynamic_qos_pretch(self):
        """Calculate the prefetch count based upon the message velocity * the
        _QOS_PREFETCH_MULTIPLIER.

        :rtype: int

        """
        # Round up the velocity * the multiplier
        value = int(math.ceil(self._message_velocity *
                              float(self._QOS_PREFETCH_MULTIPLIER)))
        LOGGER.debug('Calculated prefetch value: %i', value)
        return value

    @property
    def _message_velocity(self):
        """Return the message consuming velocity for the process.

        :rtype: float

        """
        processed = self._count_processed_last_interval
        duration = time.time() - self._last_stats_time
        LOGGER.debug('Processed %i messages in %i seconds', processed, duration)

        # If there were no messages, do not calculate, use the base
        if not processed or not duration:
            return 0

        # Calculate the velocity as the basis for the calculation
        velocity = float(processed) / float(duration)
        LOGGER.debug('Message processing velocity: %.2f', velocity)
        return velocity

    def _get_config(self, config, number, name, connection):
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
                'name': '%s_%i_tag_%i' % (name, os.getpid(), number)}

    def _get_connection(self, config, name):
        """Connect to RabbitMQ returning the connection handle.

        :param dict config: The Connections section of the configuration
        :param str name: The name of the connection
        :rtype: pika.adapters.torando_connection.TorandoConnection

        """

        # Process is initially idle
        self._set_state(self.STATE_CONNECTING)

        LOGGER.debug('Connecting to %s:%i:%s as %s',
                     config[name]['host'], config[name]['port'],
                     config[name]['vhost'], config[name]['user'])
        parameters = self._get_connection_parameters(config[name]['host'],
                                                     config[name]['port'],
                                                     config[name]['vhost'],
                                                     config[name]['user'],
                                                     config[name]['pass'])
        # Get the configuration for convenience
        try:
            tornado_connection.TornadoConnection(parameters,
                                                 self.on_connected,
                                                 stop_ioloop_on_close=True)
        except pika.exceptions.AMQPConnectionError as error:
            LOGGER.critical('Could not connect to %s:%s:%s %r',
                            config[name]['host'], config[name]['port'],
                            config[name]['vhost'], error)
            self._set_state(self.STATE_STOPPED)

    def _get_connection_parameters(self, host, port, vhost, username, password):
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

    def _get_consumer(self, config):
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

    def _increment_error_count(self):
        """Increment the error count checking to see if the process should stop
        due to the number of errors.

        """
        self._counts[self.ERROR] += 1
        if self.too_many_errors:
            LOGGER.debug('Error threshold exceeded, stopping')
            self.stop()

    def _new_counter_dict(self):
        """Return a dict object for our internal stats keeping.

        :rtype: dict

        """
        return {self.ERROR: 0,
                self.PROCESSED: 0,
                self.REDELIVERED: 0,
                self.REJECTED: 0,
                self.TIME_SPENT: 0,
                self.TIME_WAITED: 0}

    def _on_error(self, method, exception):
        """Called when a runtime error encountered.

        :param pika.frames.MethodFrame method: The method frame with an error
        :param Exception exception: The error that occurred

        """
        LOGGER.error('Runtime exception raised: %s', exception)

        # If we do not have no_ack set, then reject the message
        if self._ack:
            self._reject(method.delivery_tag)

        self._increment_error_count()

    def _on_message_exception(self, method, exception):
        """Called when a consumer.MessageException is raised, will reject the
        message, not requeueing it.

        :param pika.frames.MethodFrame method: The method frame with an error
        :param Exception exception: The error that occurred
        :raises: RuntimeError

        """
        LOGGER.error('Message was rejected: %s', exception)

        # Raise an exception if no_ack = True since the msg can't be rejected
        if not self._ack:
            raise RuntimeError('Can not rejected messages when ack is False')

        # Reject the message and do not requeue it
        self._reject(method.delivery_tag, False)
        self._counts[self.REJECTED] += 1

    def _process(self, message):
        """Wrap the actual processor processing bits

        :param Message message: Message to process
        :raises: consumer.ConsumerException

        """
        # Try and process the message
        try:
            self._consumer.process(message)
        except consumer.ConsumerException as exception:
            raise exception
        except consumer.MessageException as exception:
            raise exception
        except Exception as error:
            formatted_lines = traceback.format_exc().splitlines()
            LOGGER.critical('Processor threw an uncaught exception %s: %s',
                            type(error), error)
            for line in formatted_lines:
                LOGGER.error('%s', line.strip())
            raise consumer.ConsumerException

    def _reject(self, delivery_tag, requeue=True):
        """Reject the message on the broker and log it. We should move this to
         use to nack when Pika supports it in a released version.

        :param str delivery_tag: Delivery tag to reject
        :param bool requeue: Specify if the message should be requeued or not

        """
        self._channel.basic_reject(delivery_tag=delivery_tag, requeue=requeue)
        self._reset_state()

    def _reset_state(self):
        """Reset the runtime state after processing a message to either idle
        or shutting down based upon the current state.

        """
        if self.is_processing:
            self._set_state(self.STATE_IDLE)
        elif self.is_waiting_to_shutdown:
            self._set_state(self.STATE_SHUTTING_DOWN)
        else:
            LOGGER.critical('Unexepected state: %s', self.state_description)

    def _set_qos_prefetch(self, value=None):
        """Set the QOS Prefetch count for the channel.

        :param int value: The value to set the prefetch to

        """
        self._qos_prefetch = int(value or self._base_qos_prefetch)
        LOGGER.info('Setting the QOS Prefetch to %i', self._qos_prefetch)
        self._channel.basic_qos(prefetch_count=self._qos_prefetch)

    def _set_state(self, new_state):
        """Assign the specified state to this consumer object.

        :param int new_state: The new state of the object
        :raises: ValueError

        """
        # Keep track of how much time we're spending waiting and processing
        if (new_state == self.STATE_PROCESSING and
            self._state == self.STATE_IDLE):
            self._counts[self.TIME_WAITED] += self._time_in_state
        elif (new_state == self.STATE_IDLE and
              self._state == self.STATE_PROCESSING):
            self._counts[self.TIME_SPENT] += self._time_in_state

        # Use the parent object to set the state
        super(Process, self)._set_state(new_state)

    def _setup(self, config, connection_name, consumer_name, stats_queue):
        """Initialize the consumer, setting up needed attributes and connecting
        to RabbitMQ.

        :param dict config: Consumer config section
        :param str connection_name: The name of the connection
        :param str consumer_name: Consumer name for config
        :param multiprocessing.Queue stats_queue: The queue to append stats in
        :raises: ImportError

        """
        LOGGER.info('Initializing for %s on %s', self.name, connection_name)
        #self._stats_queue = stats_queue

        # Hold the consumer config
        self._consumer_name = consumer_name
        self._config = config['Consumers'][consumer_name]

        # Setup the consumer
        self._consumer = self._get_consumer(self._config)
        if not self._consumer:
            raise ImportError('Could not import and start processor')

        # Set the routing information
        self._queue_name = self._config['queue']

        # Set the dynamic QoS toggle
        self._dynamic_qos = self._config.get('dynamic_qos', True)

        # Set the various control nobs
        self._ack = self._config.get('ack', True)

        # How many errors until the process stops
        self._max_error_count = self._config.get('max_errors',
                                                 self._MAX_ERROR_COUNT)

        # Get the heartbeat interval
        self._hbinterval = self._config.get('heartbeat_interval',
                                            self._HBINTERVAL)

        # Get the framesize
        self._max_framesize = self._config.get('max_frame_size',
                                               pika.spec.FRAME_MAX_SIZE)

        # Setup the signal handler for stats
        self._setup_signal_handlers()

        # Create the RabbitMQ Connection
        self._connection = self._get_connection(config['Connections'],
                                                connection_name)

    def _setup_signal_handlers(self):
        """Setup the stats and stop signal handlers. Use SIGABRT instead of
        SIGTERM due to the multiprocessing's behavior with SIGTERM.

        """
        signal.signal(signal.SIGPROF, self.get_stats)
        signal.signal(signal.SIGABRT, self.stop)

    def _stop_consumer(self):
        """Stop the consumer object and allow it to do a clean shutdown if it
        has the ability to do so.

        """
        try:
            LOGGER.info('Shutting down the consumer')
            self._consumer.shutdown()
        except AttributeError:
            LOGGER.debug('Consumer does not have a shutdown method')

    @property
    def _time_in_state(self):
        """Return the time that has been spent in the current state.

        :rtype: float

        """
        return time.time() - self._state_start

    def _wait_to_shutdown(self):
        """Loop while the existing message is processing so the process can
        shutdown cleanly after the message has been processed and if
        no_ack is False, acked or rejected.

        """
        # It's possible to go from waiting to shutdown to idle
        while self.is_waiting_to_shutdown and not self.is_idle:
            LOGGER.debug('Waiting to shutdown, current state: %s',
                         self.state_description)
            time.sleep(0.2)

    def get_stats(self, signum_unused, stack_frame_unused):
        """Get the queue depth from a passive queue declare and call the
        callback specified in the invocation with a dictionary of our current
        running state.

        :param int signum_unused: The signal received
        :param frame stack_frame_unused: The current stack frame

        """
        def on_results(frame):
            """Handle the callback from Pika with the queue depth information"""
            LOGGER.debug('Received queue depth info from RabbitMQ')

            # Count the idle time thus far if the consumer is just waiting
            if self.is_idle:
                self._counts[self.TIME_WAITED] += self._time_in_state
                self._state_start = time.time()

            # Call the callback with the data
            stats = {'consumer_name': self._consumer_name,
                     'name': self.name,
                     'counts': self._counts,
                     'queue': {'name': self._queue_name,
                               'message_count': frame.method.message_count,
                               'consumer_count': frame.method.consumer_count},
                     'state': {'value': self._state,
                               'description': self.state_description}}

            # Add the stats to the queue
            if self._stats_queue:
                self._stats_queue.put(stats, False)

            # Calculate dynamic QOS prefetch
            if self._dynamic_qos and self._last_counts:
                self._calculate_qos_prefetch()

            self._last_counts = copy.deepcopy(self._counts)
            self._last_stats_time = time.time()

        # Perform the passive queue declare
        LOGGER.debug('SIGINFO received, Performing a queue depth check')
        self._channel.queue_declare(callback=on_results,
                                    queue=self._queue_name,
                                    passive=True)

    @property
    def is_processing(self):
        """Returns a bool specifying if the consumer is currently processing

        :rtype: bool

        """
        return self._state == self.STATE_PROCESSING

    def on_channel_closed(self, reason_code='Unknown', reason_text='Unknown'):
        """Callback invoked by Pika when the channel has been closed.

        :param int reason_code: AMQP close status code
        :param str reason_text: AMQP close message

        """
        method = LOGGER.debug if reason_code == 0 else LOGGER.warning
        method('Channel closed (%s): %s', reason_code, reason_text)
        if self.is_running:
            self.stop()

    def on_channel_open(self, channel):
        """The channel is open so now lets set our QOS prefetch and register
        the consumer.

        :param pika.channel.Channel channel: The open channel

        """
        LOGGER.info('Channel #%i to RabbitMQ Opened', channel.channel_number)
        self._channel = channel
        self._channel.add_on_close_callback(self.on_channel_closed)

        # Set the channel in the consumer
        try:
            self._consumer.set_channel(channel)
        except AttributeError:
            LOGGER.warning('Consumer does not support channel assignment')

            # Set our QOS Prefetch Count
        self._set_qos_prefetch()

        # Set our runtime state
        self._set_state(self.STATE_IDLE)

        # Ask for stuck messages
        self._channel.basic_recover(requeue=True)

        # Start the message consumer
        self._channel.basic_consume(consumer_callback = self.process,
                                    queue=self._queue_name,
                                    no_ack=not self._ack,
                                    consumer_tag=self.name)

    def on_closed(self, reason_code='Unknown', reason_text='Unknown'):
        """Callback invoked by Pika when our connection has been closed.

        :param int reason_code: AMQP close status code
        :param str reason_text: AMQP close message

        """
        method = LOGGER.debug if reason_code == 0 else LOGGER.warning
        method('Connection closed (%s): %s', reason_code, reason_text)
        if self.is_running:
            self.stop()

    def on_connected(self, connection):
        """We have connected to RabbitMQ so setup our connection attribute and
        then open a channel.

        :param pika.connection.Connection connection: RabbitMQ Connection

        """
        LOGGER.debug('Connected to RabbitMQ')
        self._connection = connection
        self._connection.add_on_close_callback(self.on_closed)
        self._channel = self._connection.channel(self.on_channel_open)

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
            return self._reject(method.delivery_tag)

        # Set our state to processing
        self._set_state(self.STATE_PROCESSING)
        LOGGER.debug('Received message #%s', method.delivery_tag)

        # Build the message wrapper object for all the parts
        message = data.Message(channel, method, header, body)
        if method.redelivered:
            self._counts[self.REDELIVERED] += 1

        # Set our start time
        self._state_start = time.time()

        # Process the message, evaluating the success
        try:
            self._process(message)
        except consumer.ConsumerException as error:
            return self._on_error(method, error)
        except consumer.MessageException as error:
            return self._on_message_exception(method, error)

        LOGGER.debug('Message processed')

        # Message was processed
        self._counts[self.PROCESSED] += 1

        # If no_ack was not set when we setup consuming, do so here
        if self._ack:
            self._ack_message(method.delivery_tag)
        else:
            self._reset_state()

        # Hack to see if a overly busy consumer can be interrupted enough to
        # properly stop when signals are raised
        time.sleep(0.01)

    def run(self):
        """Start the consumer"""
        try:
            self._setup(self._kwargs['config'],
                        self._kwargs['connection_name'],
                        self._kwargs['consumer_name'],
                        self._kwargs['stats_queue'])
        except ImportError as error:
            name = self._kwargs['consumer_name']
            consumer = self._kwargs['config']['Consumers'][name]['consumer']
            LOGGER.critical('Could not import %s, stopping process: %r',
                            consumer, error)
            return

        # Run the IOLoop in a thread so it does not get interrupted on a signal
        ioloop_ = threading.Thread(target=ioloop.IOLoop.instance().start)
        ioloop_.start()

        while not self.is_stopped:
            try:
                ioloop_.join(timeout=0.1)
            except KeyboardInterrupt:
                pass

        LOGGER.debug('Exiting %s', self.name)

    def stop(self, signum_unused=None, frame_unused=None):
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

        # Wait until the consumer has finished processing to shutdown
        if self.is_processing and self._channel and self._channel.is_open:
            self._cancel_consumer_with_rabbitmq()
            self._set_state(self.STATE_STOP_REQUESTED)
            self._wait_to_shutdown()

        # Set the state to shutting down if it wasn't set as that during loop
        LOGGER.info('Shutting down')
        self._set_state(self.STATE_SHUTTING_DOWN)

        # If the connection is still around, close it
        if self._connection.is_open:
            LOGGER.info('Closing connection to RabbitMQ')
            try:
                self._connection.close()
            except KeyError:
                pass

        # Allow the consumer to gracefully stop and then stop the IOLoop
        self._stop_consumer()

        # Note that shutdown is complete and set the state accordingly
        LOGGER.info('Shutdown complete')
        self._set_state(self.STATE_STOPPED)

        # Force the IOLoop to stop
        ioloop.IOLoop.instance().stop()

    @property
    def too_many_errors(self):
        """Return a bool if too many errors have occurred.

        :rtype: bool

        """
        return self._count(self.ERROR) >= self._max_error_count

