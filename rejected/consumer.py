"""
consumer.py

"""
__author__ = 'Gavin M. Roy'
__email__ = 'gmr@myyearbook.com'
__since__ = '2011-07-22'

import logging
import pika
from pika.adapters.tornado_connection import TornadoConnection
import traceback
import zlib

from . import common
from . import utils
from . import __version__


class Consumer(object):
    """
    Core consumer class for processing messages and dealing with AMQP

    """
    INITIALIZING = 0x01
    CONSUMING = 0x02
    SHUTTING_DOWN = 0x03
    STOPPED = 0x04
    _QOS_PREFETCH_COUNT = 1

    def __init__(self, config, consumer_number, consumer_name, connection_name):
        """Initialize a new consumer thread, setting defaults and config values

        :param config: Consumer config section from YAML File
        :type config: dict

        """
        self.name = '%s-%s-%i' % \
                    (consumer_number, consumer_name, connection_name)

        # Create our logger
        self._logger = logging.getLogger('rejected.consumer')
        self._logger.debug('%s: Initializing for %s and %s',
                           self.name, consumer_name, connection_name)

        # Application State
        self.state = Consumer.INITIALIZING

        # Our connection name:
        self._connection_name = connection_name
        self._consumer_name = consumer_name

        # Connection objects
        self._connection = None
        self._channel = None

        # Carry our config as a subset
        self._config = {'connection': config['Connections'][connection_name]}

        # Setup the attributes
        self._setup_attributes(config)

        # Setup the processor
        self._processor = self._init_processor()

        # Setup our counters
        self._setup_counters()

        # Create our pika connection parameters attribute
        credentials = pika.PlainCredentials(config['connection']['user'],
                                            config['connection']['pass'])
        self._pika_parameters = \
            pika.ConnectionParameters(config['connection']['host'],
                                      config['connection']['port'],
                                      config['connection']['vhost'],
                                      credentials)

        # Start the connection process to RabbitMQ
        self._connect()

    def _setup_attributes(self, config):

        # Get the full config section for consumers or bindings (legacy)
        consumers = common.get_consumer_config(config)
        self._config['consumer'] = consumers[self._consumer_name]

        # Set the queue name to config
        self._queue_name = self._config['consumer']['queue']

        # Initialize object wide variables
        temp = self._config['consumer'].get('noack') or \
               self._config['consumer'].get('auto_ack', False)
        self._do_ack = not temp

        # Are the messages compressed in and out with zlib?
        self._compressed_messages = self._config.get('compressed', False)

        # Republish on error? Not found, look for the legacy value
        temp = self._config['consumer'].get('republish_on_error', None) or \
               self._config['consumer'].get('requeue_on_error', False)

        # Set our republish on error value
        self._republish_on_error = temp

        # The requeue key can be specified or use the consumer name
        # for legacy support
        self._republish_key = self._config['consumer'].get('republish_key',
                                                           self._consumer_name)

        # Get the republish exchange or legacy exchange value
        temp = self._config['consumer'].get('republish_exchange', None) or \
               self._config['consumer'].get('exchange', False)
        self._republish_exchange = temp

        # The maximum number of errors to tolerate
        self._max_error_count = self._config['consumer'].get('max_errors', 5)

    def _setup_counters(self):
        """Initialize the base counter values"""
        self._errors = 0
        self._interval_count = 0
        self._interval_start = None
        self._messages_processed = 0

    def _connect(self):
        """Connect to RabbitMQ

        :raises: ConnectionException

        """
        # Get the configuration for convenience
        try:
            TornadoConnection(self._pika_parameters, self.on_connected)
        except pika.exceptions.AMQPConnectionError as error:
            self._logger.critical('%s: Could not connect: %s', self.name, error)

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

    def on_channel_open(self, channel):
        """The channel is open so now lets set our QOS prefetch and register
        the consumer.

        :param channel: The open channel
        :type channel: pika.channel.Channel

        """
        self._logger.debug('%s: Channel to RabbitMQ Opened', self.name)
        self._channel = channel

        # Set our QOS Prefetch Count
        self._set_qos_prefetch()

        # Start the message consumer
        self._channel.basic_consume(consumer_callback = self.process,
                                    queue=self._queue_name,
                                    no_ack=not self._do_ack,
                                    consumer_tag=self.name)

        # Set our runtime state
        self.state = Consumer.CONSUMING

    def on_closed(self, connection):
        """Callback invoked by Pika when our connection has been closed.

        :parameter connection: RabbitMQ Connection
        :type connection: pika.connection.Connection

        """
        # We've closed our pika connection so stop
        # Log that we're done
        self._logger.info( '%s: RabbitMQ connection closed', self.name)

        # Shutdown the message processor
        self._logger.debug('%s: Shutting down processor', self.name)
        try:
            self._processor.shutdown()
        except AttributeError:
            self._logger.debug('%s: Processor does not have a shutdown method',
                               self.name)

        # Set the runtime state
        self.state = Consumer.STOPPED

    def on_basic_cancel(self, channel):
        """Callback indicating our basic.cancel has completed. Now close the
        connection.

        :param channel: The open channel
        :type channel: pika.channel.Channel

        """
        self._logger.debug('%s: Basic.Cancel complete', self.name)
        self._connection.close()


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
        # Don't accept the message if we're shutting down
        if self.state == Consumer.SHUTTING_DOWN:
            return False

        # Build a legacy message wrapper
        message = LegacyMessage(method, header, body)
        self._logger.debug('%s: received message %s',
                           self.name, method.delivery_tag)

        # If we're compressed in message body, decompress it
        if self._compressed_messages:
            message = self._decompress(message)

        # Process the message, evaluating the success
        if self._process(message):

            # Message was processed
            self._messages_processed += 1

            # If we're not auto-acking at the broker level, do so here
            if self._do_ack:
                self._ack(message)

            # Done
            return

        # Processing failed
        self._errors += 1

        # If we're not auto-acking at the broker level, do so here
        if self._do_ack:
            self._nack(message)

        # No-Ack is on so do we want to republish?
        elif self._republish_on_error:
            self._republish(self._republish_exchange,
                            self._republish_key,
                            message)

        # Check our error count
        self._error_count_check()

    def stop(self):
        """Stop the consumer from consuming by calling BasicCancel and setting
        our state.

        """
        self._logger.info('%s: Stopping the consumer', self.name)

        # If we're already shutting down, note it for debugging purposes
        if self.state == Consumer.SHUTTING_DOWN:
            self._logger.debug('%s: Already shutting down', self.name)
            return

        self.state = Consumer.SHUTTING_DOWN
        self._channel.basic_cancel(self.name, callback=self.on_basic_cancel)


    def _ack(self, message):
        """Acknowledge the message on the broker

        :param message: Message to process
        :type message: LegacyMessage

        """
        self._logger.debug('%s: Acking %s', self.name, message.delivery_tag)
        self._channel.basic_ack(message.delivery_tag)

    def _nack(self, message):
        """Negatively acknowledge the message on the broker

        :param message: Message to process
        :type message: LegacyMessage

        """
        self._logger.debug('%s: Nacking %s', self.name, message.delivery_tag)
        self._channel.basic_nack(message.delivery_tag)

    def _compress(self, message):
        """Compress a zlib compressed body part

        :param message: The AMQP message with the body part to compress
        :type message: LegacyMessage
        :returns: LegacyMessage

        """
        try:
            message.body = zlib.compress(message.body)
        except zlib.error as error:
            self._logger.warn('Could not compress message.body: %s', error)
        return message

    def _decompress(self, message):
        """Decompress a zlib compressed body part

        :param message: The AMQP message with the body part to decompress
        :type message: LegacyMessage
        :returns: LegacyMessage

        """
        try:
            message.body = zlib.decompress(message.body)
        except zlib.error:
            self._logger.warn('Invalid zlib compressed message.body')
        return message

    def _error_count_check(self):
        """Check the quantity of errors in the thread and shutdown if required

        """
        # If we've had too many according to the config, shutdown
        if self._errors >= self._max_error_count:
            self._logger.error('%s: Processor returned %i errors',
                               self.name, self._errors)
            # Stop the consumer
            self.stop()

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
            return processor_class(self._config['consumer']['config'])

        # No config to pass
        return processor_class()

    def _process(self, message):
        """Wrap the actual processor processing bits

        :param message: Message to process
        :type message: LegacyMessage
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

    def _republish(self, exchange, routing_key, _previous_message):
        """Republish a message (on error)

        :param exchange: The exchange to publish to
        :type exchange: str
        :param routing_key: The routing key to use
        :type routing_key: str
        :param _previous_message: The message to republish
        :type _previous_message: LegacyMessage

        """
        # Compress the message if we need to
        if self._compressed_messages:
            _previous_message = self._compress(_previous_message)

        properties = _previous_message.header.properties
        properties.app_id =  'rejected/%s' % __version__
        properties.user_id = self._config['connection']['user']

        self._channel.basic_publish(exchange=exchange,
                                    routing_key=routing_key,
                                    body=_previous_message.body,
                                    properties=properties)

    def _set_qos_prefetch(self):
        """Set the QOS Prefetch count for the channel"""
        value = self._config['consumer'].get('qos',
                                             Consumer._QOS_PREFETCH_COUNT)
        self._logger.info('%s: Setting the QOS Prefetch to %i',
                          self.name, value)
        self._channel.basic_qos(value, 0, 0)

    @property
    def information(self):
        """Return information from the Thread about its runtime state

        :returns: dict

        """
        return {'name': self.name,
                'connection': self._connection_name,
                'consumer_name': self._consumer_name,
                'queue': self._queue_name,
                'processed': self._messages_processed,
                'queue_depth': self.queue_depth}

    @property
    def queue_depth(self):
        """Get the queue depth from a passive queue declare.

        :returns: int
        """
        # Log that we're done
        self._logger.info( '%s: Performing a queue depth check', self.name)
        def on_passive_queue_declare(channel, frame):
            self._logger.debug('Got queue depth: %r %r', channel, frame)
            return 0

        self._channel.queue_declare(self._queue_name,
                                    passive=True,
                                    callback=on_passive_queue_declare)


class LegacyMessage(object):

    def __init__(self, method, header, body):

        self.delivery_tag = method.delivery_tag
        self.delivery_info = {'routing_key': method.routing_key}
        self.properties = header
        self.body = body
