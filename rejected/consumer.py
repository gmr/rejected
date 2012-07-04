"""Core consumer class that handles the communication with RabbitMQ and the
delegation of messages to the Processor class for processing. Controls the life
cycle of a message received from RabbitMQ.

"""
import copy
import logging
import os
import pika
from pika import exceptions
from pika.adapters import tornado_connection
import time
import traceback

from rejected import __version__

logger = logging.getLogger(__name__)

def import_namespaced_class(namespaced_class):
    """Pass in a string in the format of foo.Bar, foo.bar.Baz, foo.bar.baz.Qux
    and it will return a handle to the class

    :param str namespaced_class: The namespaced class
    :rtype: class

    """
    # Split up our string containing the import and class
    parts = namespaced_class.split('.')

    # Build our strings for the import name and the class name
    import_name = '.'.join(parts[0:-1])
    class_name = parts[-1]

    # get the handle to the class for the given import
    class_handle = getattr(__import__(import_name, fromlist=class_name),
                           class_name)

    # Return the class handle
    return class_handle


class RejectedConsumer(object):
    """Core consumer class for processing messages and dealing with AMQP and
    message processing.

    """
    _AMQP_APP_ID = 'rejected/%s' % __version__
    _QOS_PREFETCH_COUNT = 1

    # State constants
    STATE_INITIALIZING = 0x01
    STATE_CONNECTING = 0x02
    STATE_IDLE = 0x03
    STATE_PROCESSING = 0x04
    STATE_STOP_REQUESTED = 0x05
    STATE_SHUTTING_DOWN = 0x06
    STATE_STOPPED = 0x07

    # For reverse lookup
    _STATES = {0x01: 'Initializing',
               0x02: 'Connecting',
               0x03: 'Idle',
               0x04: 'Processing',
               0x05: 'Stop Requested',
               0x06: 'Shutting down',
               0x07: 'Stopped'}

    # Counter constants
    ERROR = 'failed'
    PROCESSED = 'processed'
    REDELIVERED = 'redelivered_messages'
    TIME_SPENT = 'processing_time'
    TIME_WAITED = 'waiting_time'

    # Default message pre-allocation value
    _QOS_PREFETCH_COUNT = 1
    _MAX_ERROR_COUNT = 5

    def __init__(self, config, consumer_number, consumer_name, connection_name):
        """Initialize a new consumer thread, setting defaults and config values

        :param dict config: Consumer config section from YAML File
        :param int consumer_number: The identification number for the consumer
        :param str consumer_name: The name of the consumer
        :param str connection_name: The name of the connection
        :raises: ImportError

        """
        logger.info('Initializing for %s on %s', consumer_name, connection_name)
        self._state = self.STATE_INITIALIZING
        self._state_start = time.time()

        # Set the consumer name for logging purposes
        self.name = self._get_name(consumer_name, consumer_number)

        # Hold the consumer config
        self._consumer = config['Consumers'][consumer_name]

        # Setup the processor
        self._processor = self._get_processor(self._consumer)
        if not self._processor:
            raise ImportError('Could not import and start processor')

        # Set the routing information
        self._queue_name = self._consumer['queue']

        # Set the various control nobs
        self._ack = self._consumer.get('ack', True)
        self._max_error_count = self._consumer.get('max_errors',
                                                   self._MAX_ERROR_COUNT)
        self._prefetch_count = self._consumer.get('prefetch_count',
                                                  self._QOS_PREFETCH_COUNT)

        # Get the RabbitMQ Connection started
        self._channel = None
        self._connection = self._get_connection(config['Connections'],
                                                connection_name)

        # Setup a counter dictionary
        self._counts = self._initialize_counts()

    def _ack_message(self, delivery_tag):
        """Acknowledge the message on the broker and log the ack

        :param int delivery_tag: Delivery tag to acknowledge

        """
        logger.debug('Acking %s', delivery_tag)
        self._channel.basic_ack(delivery_tag=delivery_tag)

    def _can_change_state(self):
        """Check the current state, calling stop if required and returning False
        if the state should not change.

        :rtype: bool

        """
        # If we have a requested stop, call it
        if self._state == RejectedConsumer.STATE_STOP_REQUESTED:
            logger.debug('Stop requested prior to changing state')
            self.stop()
            return False

        # Make sure we're not in a blocking state
        if self.is_stopped:
            logger.debug('No state change while stopping or stopped')
            return False

        # Let our calling party know it's ok
        return True

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
            self._set_state(RejectedConsumer.STATE_STOPPED)

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

    def _get_name(self, consumer_name, consumer_number):
        """Initialize a new consumer thread, setting defaults and config values

        :param str consumer_name: The name of the consumer
        :param int consumer_number: The identification number for the consumer
        :rtype: str

        """
        return '%s_%i_tag_%i' % (consumer_name, os.getpid(), consumer_number)

    def _get_processor(self, config):
        """Initialize the message processor

        :param dict config: The named consumer section of the configuration
        :rtype: instance

        """
        # Try and import the module
        namespaced_class = '%s.%s' % (config['import'], config['processor'])
        logger.info('Creating message processor: %s', namespaced_class)
        processor_class = import_namespaced_class(namespaced_class)

        # If we have a config, pass it in to the constructor
        if 'config' in config:
            try:
                return processor_class(config['config'])
            except Exception as error:
                logger.critical('Could not create %s: %r',
                                namespaced_class, error)
                return None

        # No config to pass
        try:
            return processor_class()
        except Exception as error:
            logger.critical('Could not create %s: %r', namespaced_class, error)
            return None


    def _initialize_counts(self):
        """Return a dict object for our internal stats keeping.

        :rtype: dict

        """
        return {RejectedConsumer.ERROR: 0,
                RejectedConsumer.PROCESSED: 0,
                RejectedConsumer.REDELIVERED: 0,
                RejectedConsumer.TIME_SPENT: 0,
                RejectedConsumer.TIME_WAITED: 0}

    def _process(self, message):
        """Wrap the actual processor processing bits

        :param Message message: Message to process
        :rtype: bool

        """
        # Try and process the message
        try:
            return self._processor.process(message)
        except Exception as error:
            formatted_lines = traceback.format_exc().splitlines()
            logger.critical('Processor threw an uncaught exception %s: %s',
                            type(error), error)
            for line in formatted_lines:
                logger.debug('%s', line.strip())

        # We erred out so return False
        return False

    def _reject(self, delivery_tag):
        """Reject the message on the broker and log it. We should move this to
         use to nack when Pika supports it in a released version.

        :param int delivery_tag: Delivery tag to reject
        :type delivery_tag: int

        """
        self._channel.basic_reject(delivery_tag=delivery_tag)

    def _set_qos_prefetch(self, value=None):
        """Set the QOS Prefetch count for the channel.

        :param int value: The value to set the prefetch to

        """
        value = value or self._prefetch_count
        logger.info('Setting the QOS Prefetch to %i', value)
        self._channel.basic_qos(prefetch_count=value, callback=None)

    def _set_state(self, state):
        """Assign the specified state to this consumer object.

        :param int state: The new state of the object
        :raises: ValueError

        """
        # Keep track of how much time we're spending waiting and processing
        if state == self.STATE_PROCESSING and self._state == self.STATE_IDLE:
            self._counts[self.TIME_WAITED] += self._time_in_state
        elif state == self.STATE_IDLE and self._state == self.STATE_PROCESSING:
            self._counts[self.TIME_SPENT] += self._time_in_state

        # Make sure it's a valid state
        if state not in RejectedConsumer._STATES:
            raise ValueError('%s is not a valid state for this object' %
                             self._STATES[state])
        # Set the state
        self._state = state
        self._state_start = time.time()

    @property
    def _time_in_state(self):
        """Return the time that has been spent in the current state.

        :rtype: float

        """
        return time.time() - self._state_start

    @property
    def is_idle(self):
        """Returns a bool specifying if the consumer is currently idle.

        :rtype: bool

        """
        return self._state == self.STATE_IDLE

    @property
    def is_running(self):
        """Returns a bool determining if the consumer is in a running state or
        not

        :rtype: bool

        """
        return self._state in [RejectedConsumer.STATE_IDLE,
                               RejectedConsumer.STATE_PROCESSING]

    @property
    def is_shutting_down(self):
        """Designates if the consumer is shutting down.

        :rtype: bool

        """
        return self._state == self.STATE_SHUTTING_DOWN

    @property
    def is_stopped(self):
        """Returns a bool determining if the consumer is stopped or stopping

        :rtype: bool

        """
        return self._state in [RejectedConsumer.STATE_SHUTTING_DOWN,
                               RejectedConsumer.STATE_STOPPED]

    def on_basic_cancel(self, channel):
        """Callback indicating our basic.cancel has completed. Now close the
        connection.

        :param pika.channel.Channel channel: The open channel

        """
        logger.debug('Basic.Cancel on %r complete', channel)
        self._set_state(self.STATE_STOPPED)

    def on_channel_close(self, reason_code, reason_text):
        """Callback invoked by Pika when the channel has been closed by
        RabbitMQ

        :param int reason_code: AMQP close status code
        :param str reason_text: AMQP close message

        """
        logger.critical('Channel closed by remote (%s): %s',
                        reason_code, reason_text)
        self._set_state(self.STATE_STOPPED)

    def on_channel_open(self, channel):
        """The channel is open so now lets set our QOS prefetch and register
        the consumer.

        :param pika.channel.Channel channel: The open channel

        """
        logger.info('Channel #%i to RabbitMQ Opened', channel.channel_number)
        self._channel = channel
        self._channel.add_on_close_callback(self.on_channel_close)

        # Set our QOS Prefetch Count
        self._set_qos_prefetch()

        # Set our runtime state
        self._set_state(RejectedConsumer.STATE_IDLE)

        # Ask for stuck messages
        self._channel.basic_recover(requeue=True)

        # Start the message consumer
        self._channel.basic_consume(consumer_callback = self.process,
                                    queue=self._queue_name,
                                    no_ack=not self._ack,
                                    consumer_tag=self.name)

    def on_closed(self, reason_code, reason_text):
        """Callback invoked by Pika when our connection has been closed.

        :param int reason_code: AMQP close status code
        :param str reason_text: AMQP close message

        """
        # We've closed our pika connection so stop
        # Log that we're done
        logger.info('RabbitMQ connection closed: (%s) %s',
                    reason_code, reason_text)

        # Shutdown the message processor
        try:
            logger.debug('Shutting down processor')
            self._processor.shutdown()
        except AttributeError:
            logger.debug('Processor does not have a shutdown method')

        # Set the runtime state
        self._set_state(RejectedConsumer.STATE_STOPPED)

    def on_connected(self, connection):
        """We have connected to RabbitMQ so setup our connection attribute and
        then open a channel.

        :param pika.connection.Connection connection: RabbitMQ Connection

        """
        logger.debug('Connected to RabbitMQ')
        self._connection = connection
        self._connection.add_on_close_callback(self.on_closed)
        self._connection.channel(self.on_channel_open)

    def on_error(self, method, exception):
        """Called when a runtime error encountered.

        :param pika.frames.MethodFrame method: The method frame with an error
        :param Exception exception: The error that occurred

        """
        logger.error('Runtime exception raised: %s', exception)
        self._count[self.ERROR] += 1

        # If we do not have no_ack set, then reject the message
        if self._ack:
            self._reject(method.delivery_tag)

        # Check our error count
        if self.too_many_errors:
            logger.debug('Error threshold exceeded, stopping')
            self.stop()

        # Set our state to consuming
        self._set_state(self.STATE_IDLE)

    def process(self, channel=None, method=None, header=None, body=None):
        """Process a message from Rabbit

        :param pika.channel.Channel channel: The channel the message was sent on
        :param pika.frames.MethodFrame method: The method frame
        :param pika.frames.HeaderFrame header: The header frame
        :param str body: The message body
        :rtype: bool

        """
        if self.is_shutting_down:
            logger.critical('Received a message while shutting down')
            self._reject(method.delivery_tag)
            return False

        # Set our state to processing
        self._set_state(self.STATE_PROCESSING)
        logger.debug('Received message #%s', method.delivery_tag)

        # Build the message wrapper object for all the parts
        message = Message(channel, method, header, body)
        if method.redelivered:
            self._counts[self.REDELIVERED] += 1

        # Set our start time
        self._state_start = time.time()

        # Process the message, evaluating the success
        try:
            self._process(message)
        except RuntimeError as error:
            return self.on_error(method, error)

        # Message was processed
        self._counts[self.PROCESSED] += 1

        # If no_ack was not set when we setup consuming, do so here
        if self._ack:
            self._ack_message(method.delivery_tag)

        # Set the state as idle
        self._set_state(self.STATE_IDLE)

    @property
    def state(self):
        """Return the current state value

        :rtype: int

        """
        return self._state

    @property
    def state_desc(self):
        """Return the string description of our running state.

        :rtype: str

        """
        return RejectedConsumer._STATES[self._state]

    def stats(self, callback):
        """Get the queue depth from a passive queue declare and call the
        callback specified in the invocation with a dictionary of our current
        running state.

        :param method callback: Method to call when we have our data

        """
        def on_passive_queue_declare(frame):
            """Handle the callback from Pika with the queue depth information"""
            logger.debug('Calling %s with queue_state_data', callback)

            # Count the idle time thus far if the consumer is just waiting
            if self.is_idle:
                self._counts[self.TIME_WAITED] += self._time_in_state
                self._state_start = time.time()

            # Call the callback with the data
            callback({'name': self.name,
                      'counts': self._counts,
                      'queue': {'name': self._queue_name,
                                'message_count': frame.method.message_count,
                                'consumer_count': frame.method.consumer_count},
                      'state': {'value': self.state,
                                'description': self.state_desc}})

        # Perform the passive queue declare
        logger.debug('Performing a queue depth check')
        self._channel.queue_declare(callback=on_passive_queue_declare,
                                    queue=self._queue_name,
                                    passive=True)

    def stop(self):
        """Stop the consumer from consuming by calling BasicCancel and setting
        our state.

        """
        logger.info('Stopping the consumer')

        if self.is_stopped:
            logger.error('Stop requested but consumer is already stopped')
            return

        if self.is_shutting_down:
            logger.error('Stop requested but consumer is already shutting down')
            return

        # A stop was requested, send basic cancel
        self._channel.basic_cancel(consumer_tag=self.name,
                                   callback=self.on_basic_cancel)

        # Set the state to shutting down
        self._set_state(RejectedConsumer.STATE_SHUTTING_DOWN)

    @property
    def too_many_errors(self):
        """Return a bool if too many errors have occurred.

        :rtype: bool

        """
        return self._count(self.ERROR) >= self._max_error_count


class DataObject(object):
    """A class that will return a plain text representation of all of the
    attributes assigned to the object.

    """
    def __repr__(self):
        """Return a string representation of the object and all of its
        attributes.

        :rtype: str

        """
        items = list()
        for key, value in self.__dict__.iteritems():
            if getattr(self.__class__, key, None) != value:
                items.append('%s=%s' % (key, value))
        return "<%s(%s)>" % (self.__class__.__name__, items)


class Message(DataObject):
    """Class for containing all the attributes about a message object creating a
    flatter, move convenient way to access the data while supporting the legacy
    methods that were previously in place in rejected < 2.0

    """

    def __init__(self, channel, method, header, body):
        """Initialize a message setting the attributes from the given channel,
        method, header and body.

        :param pika.channel.Channel channel: The channel the msg was received on
        :param pika.frames.Method method: Pika Method Frame object
        :param pika.frames.Header header: Pika Header Frame object
        :param str body: Pika message body

        """
        DataObject.__init__(self)
        self.channel = channel
        self.method = method
        self.properties = Properties(header)
        self.body = copy.copy(body)

        # Map method properties
        self.consumer_tag = method.consumer_tag
        self.delivery_tag = method.delivery_tag
        self.exchange = method.exchange
        self.redelivered = method.redelivered
        self.routing_key = method.routing_key


class Properties(DataObject):
    """A class that represents all of the field attributes of AMQP's
    Basic.Properties

    """

    def __init__(self, header):
        """Create a base object to contain all of the properties we need

        :param pika.spec.BasicProperties header: A header object from Pika

        """
        DataObject.__init__(self)
        self.content_type = header.content_type
        self.content_encoding = header.content_encoding
        self.headers = copy.deepcopy(header.headers) or dict()
        self.delivery_mode = header.delivery_mode
        self.priority = header.priority
        self.correlation_id = header.correlation_id
        self.reply_to = header.reply_to
        self.expiration = header.expiration
        self.message_id = header.message_id
        self.timestamp = header.timestamp
        self.type = header.type
        self.user_id = header.user_id
        self.app_id = header.app_id
        self.cluster_id = header.cluster_id
