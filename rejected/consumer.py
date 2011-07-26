"""
Core consumer class that handles the communication with RabbitMQ and the
delegation of messages to the Processor class for processing. Controls the life
cycle of a message received from RabbitMQ.

"""
__author__ = 'Gavin M. Roy'
__email__ = 'gmr@myyearbook.com'
__since__ = '2011-07-22'

import logging
import os
import pika
from pika.adapters.tornado_connection import TornadoConnection
import time
import traceback
import zlib

from . import compat
from . import utils
from . import __version__

_AMQP_APP_ID = 'rejected/%s' % __version__
_QOS_PREFETCH_COUNT = 1


class Consumer(object):
    """
    Core consumer class for processing messages and dealing with AMQP and
    message processing.

    """
    # State constants
    INITIALIZING = 0x01
    CONSUMING = 0x02
    SHUTTING_DOWN = 0x03
    STOPPED = 0x04
    PROCESSING = 0x05
    STOP_REQUESTED = 0x06

    # For reverse lookup
    _STATES = {0x01: 'Initializing',
               0x02: 'Consuming',
               0x03: 'Shutting down',
               0x04: 'Stopped',
               0x05: 'Processing',
               0x06: 'Stop Requested'}

    # Counter constants
    ERROR = 'failed'
    PROCESSED = 'processed'
    REDELIVERED = 'redelivered_messages'
    TIME_SPENT = 'processing_time'

    # Default message pre-allocation value
    _QOS_PREFETCH_COUNT = 1

    def __init__(self, config, consumer_number, consumer_name, connection_name):
        """Initialize a new consumer thread, setting defaults and config values

        :param config: Consumer config section from YAML File
        :type config: dict
        :param consumer_number: The identification number for the consumer
        :type consumer_number: int
        :param consumer_name: The name of the consumer
        :type consumer_name: str
        :param connection_name: The name of the connection
        :type connection_name
        :raises: ImportError

        """
        self._config = {'connection': config['Connections'][connection_name],
                        'connection_name': connection_name,
                        'consumer_name': consumer_name,
                        'name': '%s_%i_tag_%i' % (consumer_name,
                                                  os.getpid(),
                                                  consumer_number)}

        # Create our logger
        self._logger = logging.getLogger('rejected.consumer')
        self._logger.debug('%s: Initializing for %s and %s',
                           self.name, consumer_name, connection_name)

        # Application State
        self._state = None
        self._set_state(Consumer.INITIALIZING)

        # Connection objects
        self._connection = None
        self._channel = None

        # Carry our config as a subset
        # Setup the attributes
        self._add_config_attributes(config)

        # Setup the processor
        self._processor = self._init_processor()
        if not self._processor:
            raise ImportError('Could not import and start processor')

        # Create our pika connection parameters attribute
        connection = self._config['connection']
        credentials = pika.PlainCredentials(connection['user'],
                                            connection['pass'])
        self._config['pika'] = pika.ConnectionParameters(connection['host'],
                                                         connection['port'],
                                                         connection['vhost'],
                                                         credentials)

        # Start the connection process to RabbitMQ
        self._connect()

        # Setup a counter dictionary
        self._counts = {Consumer.PROCESSED: 0,
                        Consumer.ERROR: 0,
                        Consumer.REDELIVERED: 0,
                        Consumer.TIME_SPENT: 0}

    ## Public Methods

    def get_processing_information(self, callback):
        """Get the queue depth from a passive queue declare and call the
        callback specified in the invocation with a dictionary of our current
        running state.

        :param callback: Method to call when we have our data
        :type callback: method or function

        """
        def on_passive_queue_declare(frame):
            """Handle the callback from Pika with the queue depth information"""
            self._logger.debug('Calling %s with queue_state_data', callback)
            # Call the callback with the data
            callback({'name': self._config['name'],
                      'connection': self._config['connection_name'],
                      'consumer_name': self._config['consumer_name'],
                      'counts': self._counts,
                      'queue': {'name': self._config['queue_name'],
                                'message_count': frame.method.message_count,
                                'consumer_count': frame.method.consumer_count},
                      'state': {'value': self.state,
                                'description': self.state_desc}})

        # Perform the passive queue declare
        self._logger.debug('%s: Performing a queue depth check', self.name)
        self._channel.queue_declare(callback=on_passive_queue_declare,
                                    queue=self._config['queue_name'],
                                    passive=True)

    def on_basic_cancel(self, channel):
        """Callback indicating our basic.cancel has completed. Now close the
        connection.

        :param channel: The open channel
        :type channel: pika.channel.Channel

        """
        self._logger.debug('%s: Basic.Cancel on %r complete',
                           self.name, channel)
        self._set_state(self.STOPPED)

    def on_channel_close(self, reason_code, reason_text):
        """Callback invoked by Pika when the channel has been closed by
        RabbitMQ

        :param reason_code: AMQP close status code
        :type reason_code: int
        :param reason_text: AMQP close message
        :type reason_text: str

        """
        self._logger.critical('%s: The channel has closed: (%s) %s',
                              self.name, reason_code, reason_text)
        self._set_state(self.STOPPED)

    def on_channel_open(self, channel):
        """The channel is open so now lets set our QOS prefetch and register
        the consumer.

        :param channel: The open channel
        :type channel: pika.channel.Channel

        """
        self._logger.debug('%s: Channel to RabbitMQ Opened', self.name)
        self._channel = channel
        self._channel.add_on_close_callback(self.on_channel_close)

        # Set our QOS Prefetch Count
        self._set_qos_prefetch()

        # Set our runtime state
        self._set_state(Consumer.CONSUMING)

        # Ask for stuck messages
        self._channel.basic_recover(requeue=True)

        # Start the message consumer
        self._channel.basic_consume(consumer_callback = self.process,
                                    queue=self._config['queue_name'],
                                    no_ack=self._config['no_ack'],
                                    consumer_tag=self.name)

    def on_closed(self, reason_code, reason_text):
        """Callback invoked by Pika when our connection has been closed.

        :param reason_code: AMQP close status code
        :type reason_code: int
        :param reason_text: AMQP close message
        :type reason_text: str

        """
        # We've closed our pika connection so stop
        # Log that we're done
        self._logger.info('%s: RabbitMQ connection closed: (%s) %s',
                          self.name, reason_code, reason_text)

        # Shutdown the message processor
        self._logger.debug('%s: Shutting down processor', self.name)
        try:
            self._processor.shutdown()
        except AttributeError:
            self._logger.debug('%s: Processor does not have a shutdown method',
                               self.name)

        # Set the runtime state
        self._set_state(Consumer.STOPPED)

    def on_connected(self, connection):
        """We have connected to RabbitMQ so setup our connection attribute and
        then open a channel.

        :parameter connection: RabbitMQ Connection
        :type connection: pika.connection.Connection

        """
        self._logger.debug('%s: Connected to RabbitMQ', self.name)
        self._connection = connection
        self._connection.add_on_close_callback(self.on_closed)
        self._connection.channel(self.on_channel_open)

    def process(self, channel, method, header, body):
        """Process a message from Rabbit

        @TODO Build new class structure support in as well for native Pika
              message types

        :param channel: The channel the message was sent on
        :type channel: pika.channel.Channel
        :param method: The method frame
        :type method: pika.frames.MethodFrame
        :param header: The header frame
        :type header: pika.frames.HeaderFrame
        :param body: The message body
        :type body: str
        :returns: bool

        """
        # Set our state to processing
        self._processing()

        # Don't accept the message if we're shutting down
        if self._state == Consumer.SHUTTING_DOWN:
            self._logger.critical('%s: Received a message while shutting down',
                                  self.name)
            return False

        # Build the message wrapper object for all the parts
        message = Message(channel, method, header, body,
                          self._config['compressed_messages'])
        if method.redelivered:
            redelivered = ' - is a redelivered message'
            self._increment_stat(Consumer.REDELIVERED)
        else:
            redelivered = ''
        self._logger.debug('%s: Received message #%s%s',
                           self.name, method.delivery_tag, redelivered)

        # Set our start time
        start_time = time.time()

        # Process the message, evaluating the success
        if self._process(message):

            # Message was processed
            self._increment_stat(Consumer.PROCESSED, start_time)

            # If no_ack was not set when we setup consuming, do so here
            if not self._config['no_ack']:
                self._ack(method.delivery_tag)

            # Exit while setting our state to consuming
            return self._consuming()

        # Processing failed
        self._increment_stat(Consumer.ERROR, start_time)

        # If we do not have no_ack set, then reject the message
        if not self._config['no_ack']:
            self._reject(method.delivery_tag)

        # No-Ack is on, do we want to republish?
        elif self._config['republish_on_error']:
            self._republish(self._config['republish_exchange'],
                            self._config['republish_key'],
                            message)

        # Check our error count
        self._error_count_check()

        # Set our state to consuming
        self._consuming()

    def stop(self):
        """Stop the consumer from consuming by calling BasicCancel and setting
        our state.

        """
        self._logger.info('%s: Stopping the consumer', self.name)

        # If we're processing set our state to let our processor know to call
        # us when we're done
        if self._state == Consumer.PROCESSING:
            self._set_state(Consumer.STOP_REQUESTED)
            return

        # If we're already shutting down, note it for debugging purposes
        if self._state == Consumer.SHUTTING_DOWN:
            self._logger.debug('%s: Already shutting down', self.name)
            return

        # If we're already stopped, note it for debugging purposes
        if self._state == Consumer.STOPPED:
            self._logger.debug('%s: Already stopped', self.name)
            return

        self._set_state(Consumer.SHUTTING_DOWN)
        self._channel.basic_cancel(consumer_tag=self.name,
                                   callback=self.on_basic_cancel)

    ## Internal methods

    def _ack(self, delivery_tag):
        """Acknowledge the message on the broker and log the ack

        :param delivery_tag: Delivery tag to acknowledge
        :type delivery_tag: int

        """
        self._logger.debug('%s: Acking %s', self.name, delivery_tag)
        self._channel.basic_ack(delivery_tag=delivery_tag)

    def _add_config_attributes(self, config):
        """Append values to the _config dictionary of attributes

        :param config: The configuration as specified in the YAML file
        :type config: dict

        """
        # Get the full config section for consumers or bindings (legacy)
        consumers = compat.get_consumer_config(config)
        self._config['consumer'] = consumers[self._config['consumer_name']]

        # Set the queue name to config
        self._config['queue_name'] = self._config['consumer']['queue']

        # Initialize object wide variables
        self._config['no_ack'] = \
            compat.get_compatible_config(self._config['consumer'],
                                        'noack',
                                        'auto_ack')

        # Are the messages compressed in and out with zlib?
        self._config['compressed_messages'] = self._config.get('compressed',
                                                               False)


        # Republish on error?
        self._config['republish_on_error'] = \
            compat.get_compatible_config(self._config['consumer'],
                                        'republish_on_error',
                                        'requeue_on_error')

        # The requeue key can be specified or default to consumer name
        self._config['republish_key'] = \
            self._config['consumer'].get('republish_key',
                                         self._config['consumer_name'])


        # Get the republish exchange or legacy exchange value
        self._config['republish_exchange'] = \
            compat.get_compatible_config(self._config['consumer'],
                                        'republish_exchange',
                                        'exchange')

        # The maximum number of errors to tolerate
        self._config['max_error_count'] = \
            self._config['consumer'].get('max_errors', 5)

    def _can_change_state(self):
        """Check the current state, calling stop if required and returning False
        if the state should not change.

        :returns: bool

        """
        # If we have a requested stop, call it
        if self._state == Consumer.STOP_REQUESTED:
            self._logger.debug('%s: Stop requested prior to changing state',
                               self.name)
            self.stop()
            return False

        # Make sure we're not in a blocking state
        if self.is_stopped:
            self._logger.debug('%s: No state change while stopping or stopped',
                               self.name)
            return False

        # Let our calling party know it's ok
        return True

    def _connect(self):
        """Connect to RabbitMQ

        :raises: ConnectionException

        """
        # Get the configuration for convenience
        try:
            TornadoConnection(self._config['pika'], self.on_connected)
        except pika.exceptions.AMQPConnectionError as error:
            self._logger.critical('%s: Could not connect: %s', self.name, error)
            self._set_state(Consumer.STOPPED)

    def _consuming(self):
        """Set the state to Consumer.CONSUMING, checking if we need to shutdown
        before we move forward

        """
        # Set our state to consuming if we can
        if self._can_change_state():
            self._set_state(Consumer.CONSUMING)

    def _count(self, stat):
        """Return the current count quantity for a specific stat.

        :param stat: Name of stat to get value for
        :type stat: str
        :returns: int or float

        """
        return self._counts.get(stat, -1)

    def _error_count_check(self):
        """Check the quantity of errors in the thread & shutdown if required"""
        if self._count(Consumer.ERROR) >= self._config['max_error_count']:
            self._logger.error('%s: Processor returned %i errors',
                               self.name, self._count(Consumer.ERROR))
            # Stop the consumer
            self.stop()

    def _increment_stat(self, stat, start_time=None):
        """Increment the stats counter for the given stat and add the duration
        of time spent processing.

        :param stat: The name of the stat to increment
        :type stat: str
        :param start_time: The time we started processing
        :type start_time: float

        """
        self._counts[stat] += 1
        if start_time:
            self._counts[Consumer.TIME_SPENT] += (time.time() - start_time)

    def _init_processor(self):
        """Initialize the message processor"""

        # Import our processor class
        import_name = self._config['consumer']['import']
        class_name = self._config['consumer']['processor']

        # Try and import the module
        processor_class = utils.import_namespaced_class("%s.%s" % (import_name,
                                                                   class_name))
        self._logger.info('%s: Creating message processor: %s.%s',
                          self.name, import_name, class_name)

        # If we have a config, pass it in to the constructor
        if 'config' in self._config['consumer']:
            try:
                return processor_class(self._config['consumer']['config'])
            except Exception as error:
                self._logger.critical('Could not load %s.%s: %s',
                                      import_name, class_name, error)
                return None

        # No config to pass
        try:
            return processor_class()
        except Exception as error:
            self._logger.critical('Could not load %s.%s: %s',
                                  import_name, class_name, error)
            return None

    def _process(self, message):
        """Wrap the actual processor processing bits

        :param message: Message to process
        :type message: Message
        :returns: bool

        """
        # Try and process the message
        try:
            return self._processor.process(message)
        except Exception as error:
            formatted_lines = traceback.format_exc().splitlines()
            self._logger.critical('%s: Processor threw an uncaught exception',
                                  self.name)
            self._logger.critical('%s: %s:%s', self.name, type(error), error)
            for line in formatted_lines:
                self._logger.critical('%s: %s', self.name, line.strip())

        # We erred out so return False
        return False

    def _reject(self, delivery_tag):
        """Reject the message on the broker and log it. We should move this to
         use to nack when Pika supports it in a released version.

        :param delivery_tag: Delivery tag to reject
        :type delivery_tag: int

        """
        self._logger.debug('%s: Rejecting %s', self.name, delivery_tag)

        # Switch to nack when we use a version of pika that has it
        self._channel.basic_reject(delivery_tag=delivery_tag)

    def _processing(self):
        """Set the state to Consumer.PROCESSING, checking first if there is a
        requested shutdown

        """
        # Set our state to processing if we can
        if self._can_change_state():
            self._set_state(Consumer.PROCESSING)

    def _republish(self, exchange, routing_key, message):
        """Republish a message (on error)

        :param exchange: The exchange to publish to
        :type exchange: str
        :param routing_key: The routing key to use
        :type routing_key: str
        :param message: The message to republish
        :type message: Message

        """
        # Override what sent the message
        properties = message.properties
        properties.app_id =  _AMQP_APP_ID
        properties.user_id = self._config['connection']['user']

        # Publish the message
        self._channel.basic_publish(exchange=exchange,
                                    routing_key=routing_key,
                                    body=message.get_body(),
                                    properties=properties)

    def _set_qos_prefetch(self):
        """Set the QOS Prefetch count for the channel"""
        value = self._config['consumer'].get('qos',
                                             Consumer._QOS_PREFETCH_COUNT)
        self._logger.info('%s: Setting the QOS Prefetch to %i',
                          self.name, value)
        self._channel.basic_qos(prefetch_count=value, callback=None)

    def _set_state(self, state):
        """Assign the specified state to this consumer object.

        :param state: The current state of the object
        :type state: int

        """
        # Make sure it's a valid state
        if state not in Consumer._STATES:
            raise ValueError('%s is not a valid state for this object' % \
                             Consumer._STATES[state])
        # Set the state
        self._state = state

    ## Properties

    @property
    def is_running(self):
        """Returns a bool determining if the consumer is in a running state or
        not

        :returns: bool

        """
        return self._state in [Consumer.CONSUMING, Consumer.PROCESSING]

    @property
    def is_stopped(self):
        """Returns a bool determining if the consumer is stopped or stopping

        :returns: bool

        """
        return self._state in [Consumer.SHUTTING_DOWN, Consumer.STOPPED]

    @property
    def name(self):
        """Return the name of the consumer

        :returns: str

        """
        return self._config['name']

    @property
    def state(self):
        """Return the current state value

        :returns: int

        """
        return self._state

    @property
    def state_desc(self):
        """Return the string description of our running state.

        :returns: str

        """
        return Consumer._STATES[self._state]


class DataObject(object):
    """A class that will return a plain text representation of all of the
    attributes assigned to the object.

    """
    def __init__(self):
        self._logger = logging.getLogger('rejected.%s' %
                                         self.__class__.__name__)

    def __repr__(self):
        """Return a string representation of the object and all of its
        attributes.

        :returns: str

        """
        items = list()
        for key, value in self.__dict__.iteritems():
            if getattr(self.__class__, key, None) != value:
                items.append('%s=%s' % (key, value))
        return "<%s(%s)>" % items


class Message(DataObject):
    """Class for containing all the attributes about a message object creating a
    flatter, move convenient way to access the data while supporting the legacy
    methods that were previously in place in rejected < 2.0

    """

    def __init__(self, channel, method, header, body, compressed):
        """Initialize a message setting the attributes from the given channel,
        method, header and body.

        :param channel: Pika Channel original message was received on
        :type channel: pika.channel.Channel
        :param method: Pika Method Frame object with attributes
        :type method: pika.frames.Method
        :param header: Pika Header Frame object with attributes
        :type header: pika.frames.Header
        :param body: Pika message body
        :type body: pika.frames.Body
        :param compressed: Is the body compressed with zlib?
        :type compressed: bool

        """
        DataObject.__init__(self)

        # Map the channel so we have access to it in our clients
        self._pika_objects = {'channel': channel,
                              'method': method,
                              'header': header,
                              'compressed': compressed,
                              'body': body}

        # Map method properties
        self.consumer_tag = method.consumer_tag
        self.delivery_tag = method.delivery_tag
        self.exchange = method.exchange
        self.redelivered = method.redelivered
        self.routing_key = method.routing_key

        # BasicProperties fields
        self.properties = Properties(header)

    def get_body(self, decompress=False):
        """Return the body, decompressing it if we asked for decompressed and
        if we are configured to support compressed objects.

        :returns: str

        """
        if decompress and self._pika_objects['compressed']:
            try:
                return zlib.decompress(self._pika_objects['body'])
            except zlib.error:
                self._logger.warn('Invalid zlib compressed message body')

        return self._pika_objects['body']

    @property
    def body(self):
        """Return the message body, decompressing it if it was compressed.

        :returns: str

        """
        return self.get_body(True)

    @property
    def channel(self):
        """Returns the channel attribute while raising a DeprecationWarning
        about using the Processor base class instead.

        """
        self._logger.warning("You should be using the rejected Processor base\
 class instead of the message.channel or message.delivery_info['channel']\
 attributes")
        return self._pika_objects['channel']

    @property
    def delivery_info(self):
        """Returns the delivery_info dictionary while logging a warning about
        accessing the delivery_info property

        """
        self._logger.warning("Use the attributes of the Message object instead\
 of the delivery_info attribute")
        return {'channel': self.channel,
                'delivery_tag': self.delivery_tag,
                'redelivered': self.redelivered,
                'routing_key': self.routing_key,
                'exchange': self.exchange,
                'consumer_tag': self.consumer_tag}


class Properties(DataObject):
    """A class that represents all of the field attributes of AMQP's
    Basic.Properties

    """

    def __init__(self, header):
        """Create a base object to contain all of the properties we need

        :param header: A header object from Pika
        :type header: pika.spec.BasicProperties

        """
        DataObject.__init__(self)
        self.content_type = header.content_type
        self.content_encoding = header.content_encoding
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
