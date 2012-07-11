"""Consumer process management. Imports consumer code, manages RabbitMQ
connection state and collects stats about the consuming process.

"""
from tornado import ioloop
import logging
import os
import multiprocessing
import pika
from pika import exceptions
from pika.adapters import tornado_connection
import signal
import threading
import time
import traceback

from rejected import __version__
from rejected import consumer
from rejected import data
from rejected import state

logger = logging.getLogger(__name__)


def import_namespaced_class(namespaced_class):
    """Pass in a string in the format of foo.Bar, foo.bar.Baz, foo.bar.baz.Qux
    and it will return a handle to the class

    :param str namespaced_class: The namespaced class
    :return: tuple(Class, str)

    """
    logger.debug('Importing %s', namespaced_class)
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
    TIME_SPENT = 'processing_time'
    TIME_WAITED = 'waiting_time'

    # Default message pre-allocation value
    _QOS_PREFETCH_COUNT = 1
    _MAX_ERROR_COUNT = 5
    _MAX_SHUTDOWN_WAIT = 5

    def __init__(self, group=None, target=None, name=None, args=(), kwargs={}):
        super(Process, self).__init__(group, target, name, args, kwargs)
        self._channel = None
        self._config= None
        self._consumer = None
        self._counts = self._new_counter_dict()
        self._prefetch_count = self._QOS_PREFETCH_COUNT
        self._state = self.STATE_INITIALIZING
        self._state_start = time.time()
        self._stats_queue = None

        # Override ACTIVE with PROCESSING
        self._STATES[0x04] = 'Processing'

    def _ack_message(self, delivery_tag):
        """Acknowledge the message on the broker and log the ack

        :param str delivery_tag: Delivery tag to acknowledge

        """
        logger.debug('Acking %s', delivery_tag)
        self._channel.basic_ack(delivery_tag=delivery_tag)
        self._reset_state()

    def _cancel_consumer_with_rabbitmq(self):
        """Tell RabbitMQ the process no longer wants to consumer messages."""
        logger.debug('Sending a Basic.Cancel to RabbitMQ')
        self._channel.basic_cancel(consumer_tag=self.name)

    def _count(self, stat):
        """Return the current count quantity for a specific stat.

        :param str stat: Name of stat to get value for
        :rtype: int or float

        """
        return self._counts.get(stat, 0)

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

        logger.debug('Connecting to %s:%i:%s as %s',
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
                                                 self.on_connected)
        except pika.exceptions.AMQPConnectionError as error:
            logger.critical('Could not connect to %s:%s:%s %r',
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
        return pika.ConnectionParameters(host, port, vhost, credentials)

    def _get_consumer(self, config):
        """Import and create a new instance of the configured message consumer.

        :param dict config: The named consumer section of the configuration
        :rtype: instance
        :raises: ImportError

        """
        # Try and import the module
        consumer_, version = import_namespaced_class(config['consumer'])
        if version:
            logger.info('Creating consumer %s v%s', config['consumer'], version)
        else:
            logger.info('Creating consumer %s', config['consumer'])

        # If we have a config, pass it in to the constructor
        if 'config' in config:
            try:
                return consumer_(config['config'])
            except Exception as error:
                logger.critical('Could not create %s: %r',
                                config['consumer'], error)
                return None

        # No config to pass
        try:
            return consumer_()
        except Exception as error:
            logger.critical('Could not create %s: %r',
                            config['consumer'], error)
            return None

    def _new_counter_dict(self):
        """Return a dict object for our internal stats keeping.

        :rtype: dict

        """
        return {self.ERROR: 0,
                self.PROCESSED: 0,
                self.REDELIVERED: 0,
                self.TIME_SPENT: 0,
                self.TIME_WAITED: 0}

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
        except Exception as error:
            formatted_lines = traceback.format_exc().splitlines()
            logger.critical('Processor threw an uncaught exception %s: %s',
                            type(error), error)
            for line in formatted_lines:
                logger.debug('%s', line.strip())
            raise consumer.ConsumerException

    def _reject(self, delivery_tag):
        """Reject the message on the broker and log it. We should move this to
         use to nack when Pika supports it in a released version.

        :param str delivery_tag: Delivery tag to reject

        """
        self._channel.basic_reject(delivery_tag=delivery_tag)
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
            logger.critical('Unexepected state: %s', self.state_description)


    def _set_qos_prefetch(self, value=None):
        """Set the QOS Prefetch count for the channel.

        :param int value: The value to set the prefetch to

        """
        value = value or self._prefetch_count
        logger.info('Setting the QOS Prefetch to %i', value)
        self._channel.basic_qos(prefetch_count=value, callback=None)

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
        logger.info('Initializing for %s on %s', self.name, connection_name)
        self._stats_queue = stats_queue

        # Hold the consumer config
        self._consumer_name = consumer_name
        self._config = config['Consumers'][consumer_name]

        # Setup the consumer
        self._consumer = self._get_consumer(self._config)
        if not self._consumer:
            raise ImportError('Could not import and start processor')

        # Set the routing information
        self._queue_name = self._config['queue']

        # Set the various control nobs
        self._ack = self._config.get('ack', True)
        self._max_error_count = self._config.get('max_errors',
                                                 self._MAX_ERROR_COUNT)
        self._prefetch_count = self._config.get('prefetch_count',
                                                self._QOS_PREFETCH_COUNT)

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
            logger.info('Shutting down the consumer')
            self._consumer.shutdown()
        except AttributeError:
            logger.debug('Consumer does not have a shutdown method')

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
            logger.debug('Waiting to shutdown, current state: %s',
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
            logger.debug('Received queue depth info from RabbitMQ')

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
            self._stats_queue.put(stats, False)

        # Perform the passive queue declare
        logger.debug('SIGINFO received, Performing a queue depth check')
        self._channel.queue_declare(callback=on_results,
                                    queue=self._queue_name,
                                    passive=True)

    def on_channel_closed(self, reason_code='Unknown', reason_text='Unknown'):
        """Callback invoked by Pika when the channel has been closed.

        :param int reason_code: AMQP close status code
        :param str reason_text: AMQP close message

        """
        method = logger.debug if reason_code == 0 else logger.warning
        method('Channel closed (%s): %s', reason_code, reason_text)
        if self.is_running:
            self.stop()

    def on_channel_open(self, channel):
        """The channel is open so now lets set our QOS prefetch and register
        the consumer.

        :param pika.channel.Channel channel: The open channel

        """
        logger.info('Channel #%i to RabbitMQ Opened', channel.channel_number)
        self._channel = channel
        self._channel.add_on_close_callback(self.on_channel_closed)

        # Set the channel in the consumer
        try:
            self._consumer.set_channel(channel)
        except AttributeError:
            logger.warning('Consumer does not support channel assignment')

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
        method = logger.debug if reason_code == 0 else logger.warning
        method('Connection closed (%s): %s', reason_code, reason_text)
        if self.is_running:
            self.stop()

    def on_connected(self, connection):
        """We have connected to RabbitMQ so setup our connection attribute and
        then open a channel.

        :param pika.connection.Connection connection: RabbitMQ Connection

        """
        logger.debug('Connected to RabbitMQ')
        self._connection = connection
        self._connection.add_on_close_callback(self.on_closed)
        self._channel = self._connection.channel(self.on_channel_open)

    def on_error(self, method, exception):
        """Called when a runtime error encountered.

        :param pika.frames.MethodFrame method: The method frame with an error
        :param Exception exception: The error that occurred

        """
        logger.error('Runtime exception raised: %s', exception)
        self._counts[self.ERROR] += 1

        # If we do not have no_ack set, then reject the message
        if self._ack:
            self._reject(method.delivery_tag)

        # Check our error count
        if self.too_many_errors:
            logger.debug('Error threshold exceeded, stopping')
            self.stop()

    def process(self, channel=None, method=None, header=None, body=None):
        """Process a message from Rabbit

        :param pika.channel.Channel channel: The channel the message was sent on
        :param pika.frames.MethodFrame method: The method frame
        :param pika.frames.HeaderFrame header: The header frame
        :param str body: The message body

        """
        if not self.is_idle:
            logger.critical('Received a message while in state: %s',
                            self.state_description)
            return self._reject(method.delivery_tag)

        # Set our state to processing
        self._set_state(self.STATE_PROCESSING)
        logger.debug('Received message #%s', method.delivery_tag)

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
            return self.on_error(method, error)
        logger.debug('Message processed')

        # Message was processed
        self._counts[self.PROCESSED] += 1

        # If no_ack was not set when we setup consuming, do so here
        if self._ack:
            self._ack_message(method.delivery_tag)
        else:
            self._reset_state()

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
            logger.critical('Could not import %s, stopping process: %r',
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

        logger.debug('Exiting %s', self.name)

    @property
    def is_processing(self):
        """Returns a bool specifying if the consumer is currently processing

        :rtype: bool

        """
        return self._state == self.STATE_PROCESSING

    def stop(self, signum_unused=None, frame_unused=None):
        """Stop the consumer from consuming by calling BasicCancel and setting
        our state.

        """
        logger.debug('Stop called in state: %s', self.state_description)
        if self.is_stopped:
            logger.debug('Stop requested but consumer is already stopped')
            return
        elif self.is_shutting_down:
            logger.debug('Stop requested but consumer is already shutting down')
            return
        elif self.is_waiting_to_shutdown:
            logger.debug('Stop requested but already waiting to shut down')
            return

        # Wait until the consumer has finished processing to shutdown
        if self.is_processing:
            self._cancel_consumer_with_rabbitmq()
            self._set_state(self.STATE_STOP_REQUESTED)
            self._wait_to_shutdown()

        # Set the state to shutting down if it wasn't set as that during loop
        logger.info('Shutting down')
        self._set_state(self.STATE_SHUTTING_DOWN)

        # A stop was requested, close the channel and stop the IOLoop
        if self._channel:
            logger.debug('Closing channel on RabbitMQ connection')
            self._channel.close()

        # If the connection is still around, close it
        if self._connection.is_open:
            logger.info('Closing connection to RabbitMQ')
            try:
                self._connection.close()
            except KeyError:
                pass

        # Allow the consumer to gracefully stop and then stop the IOLoop
        self._stop_consumer()
        ioloop.IOLoop.instance().stop()

        # Note that shutdown is complete and set the state accordingly
        logger.info('Shutdown complete')
        self._set_state(self.STATE_STOPPED)

    @property
    def too_many_errors(self):
        """Return a bool if too many errors have occurred.

        :rtype: bool

        """
        return self._count(self.ERROR) >= self._max_error_count

