"""
consumer.py

"""
__author__ = 'Gavin M. Roy'
__email__ = 'gmr@myyearbook.com'
__since__ = '2011-07-22'

import logging
import pika
import threading
import time
import traceback
import zlib

from . import common
from . import utils
from . import __version__

_QOS_PREFETCH_COUNT = 1


class Consumer(threading.Thread):
    """
    Core consumer class for processing messages and dealing with AMQP

    """

    def __init__(self, config, consumer_name, connection_name):
        """Initialize a new consumer thread, setting defaults and config values

        :param config: Consumer config section from YAML File
        :type config: dict

        """

        # Init the Thread Object itself
        threading.Thread.__init__(self)

        # Create our logger
        self._logger = logging.getLogger('rejected.consumer')
        self._logger.debug('%s: Initializing for %s and %s',
                           self.name, consumer_name, connection_name)

        # Our connection name:
        self._connection_name = connection_name
        self._consumer_name = consumer_name

        # Connection objects
        self._connection = None
        self._channel = None

        # Shutdown state
        self._shutdown = False

        # Carry our config as a subset
        self._config = {'connection': config['Connections'][connection_name]}

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

        # Do we want to throttle?
        self._throttle_threshold = self._config['consumer'].get('throttle', 0)
        self._throttle = bool(self._throttle_threshold)

        # Setup the processor
        self._processor = self._init_processor()

        # Connect to the AMQP Broker
        try:
            self._connection = self._connect()
        except IOError:
            return

        # Create the Channel
        self._channel = self._connection.channel()

        # Setup our counters
        self._errors = 0
        self._interval_count = 0
        self._interval_start = None
        self._messages_processed = 0
        self._throttle_count = 0
        self._throttle_duration = 0
        self._total_wait = 0.0

    def _ack(self, message):
        """Acknowledge the message on the broker

        :param message: Message to process
        :type message: amqplib.basic_message.Message

        """
        self._logger.debug('%s: Acking %s', self.name, message.delivery_tag)
        self._channel.basic_ack(message.delivery_tag)

    def _cancel(self):
        """Send a basic cancel"""
        self._logger.info('%s: Cancelling the consumer', self.name)
        self._channel.basic_cancel(self.name)
        self._logger.debug('%s: Basic.Cancel complete', self.name)

    def _close(self):
        """Close the connection to RabbitMQ"""
        if self._connection:
            try:
                self._logger.debug('%s: Closing the AMQP connection', self.name)
                self._connection.close()
                self._logger.info('%s: AMQP connection closed', self.name)
            except IOError as error:
                # Socket is likely already closed
                self._logger.debug('%s: Error closing the AMQP connection: %r',
                                   self.name, error)
            except TypeError as error:
                # Bug
                self._logger.debug('%s: Error closing the AMQP connection: %r',
                                   self.name, error)

        # Shutdown the message processor
        self._logger.debug('%s: Shutting down processor', self.name)
        try:
            self._processor.shutdown()
        except AttributeError:
            self._logger.debug('%s: Processor does not have a shutdown method',
                               self.name)

    def _connect(self):
        """Connect to RabbitMQ

        :raises: ConnectionException

        """
        # Get the configuration for convenience
        config = self._config['connection']
        self._logger.debug('%s connecting to %s:%i:%s to consume %s',
                            self.name, config['host'], config['port'],
                            config['vhost'], self._queue_name)

        # Try and create our new AMQP connection
        credentials = pika.PlainCredentials(config['user'], config['pass'])
        parameters = pika.ConnectionParameters(config['host'],
                                               config['port'],
                                               config['vhost'],
                                               credentials)

        try:
            connection = pika.BlockingConnection(parameters)
        except pika.exceptions.AMQPConnectionError as error:
            self._logger.critical('%s: Could not connect: %s', self.name, error)
            return False

        # Return the connection
        return connection

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
            # Cancel the consumer
            self._cancel()
            # Shutdown the thread
            self.shutdown()

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
        value = self._config['consumer'].get('qos', _QOS_PREFETCH_COUNT)
        self._logger.info('%s: Setting the QOS Prefetch to %i',
                          self.name, value)
        self._channel.basic_qos(value, 0, 0)

    def _throttle_check(self):
        """Check our quantities to see if we should throttle the consumer,
        sleeping if we need to do so.

        """
        self._logger.debug('%s: Checking throttle tolerances', self.name)

        # Get the duration from when we starting this interval to now
        self._throttle_duration += time.time() - self._interval_start
        self._interval_count += 1

        # We only throttle to the second, so reset and return after 1 sec
        if self._throttle_duration >= 1:
            self._interval_count = 0
            self._interval_start = None
            self._throttle_duration = 0
            return

        # Still under threshold? Return
        if self._interval_count < self._throttle_threshold:
            return

        # Time to throttle
        self._throttle_count += 1

        # Figure out how much time to sleep
        sleep_time = 1 - self._throttle_duration

        # Sleep and setup for the next interval
        self._logger.debug('%s: Throttling to %i msg/sec, sleeping %.2f sec',
                          self.name, self._throttle_threshold, sleep_time)
        time.sleep(sleep_time)

        # Reset our counters
        self._interval_count = 0
        self._interval_start = None
        self._throttle_duration = 0

    def process(self, channel, method, header, body):
        """Process a message from Rabbit

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
        message = LegacyMessage(method, header, body)
        self._logger.debug('%s: received message %s',
                           self.name, method.delivery_tag)

        # If we're throttling
        if self._throttle and self._interval_start is None:
            self._interval_start = time.time()

        # If we're compressed in message body, decompress it
        if self._compressed_messages:
            message = self._decompress(message)

        # Process the message, evaluating the success
        if self._process(message):

            # Message was processed
            self._messages_processed += 1

            # Are we trying to shutdown? send a basic cancel first
            if self._shutdown:
                self._cancel()

            # If we're not auto-acking at the broker level, do so here
            if self._do_ack:
                self._ack(message)

        # Processing failed
        else:

            # Do we need to republish?
            if self._republish_on_error:
                self._republish(self._republish_exchange,
                                self._republish_key,
                                message)

            # Keep track of how many errors we've had
            self._errors += 1

            # Check our error count
            self._error_count_check()

        # If we're throttling, check if we need to throttle and do so if needed
        if self._throttle:
            self._throttle_check()

    def run(self):
        """Called when the thread has started so we can process messages"""
        self._logger.info('%s: now running', self.name)
        # Set the qos prefetch based on a default of _QOS_PREFETCH_COUNT
        #self._set_qos_prefetch()

        # Tell the broker we're ready to receive messages
        self._channel.basic_consume(consumer_callback = self.process,
                                    queue=self._queue_name,
                                    no_ack=not self._do_ack,
                                    consumer_tag=self.name)

        # Wait for messages
        self._logger.debug('%s: Basic.Consume issued for %s',
                           self.name, self._queue_name)

        # Loop while we're running
        self._channel.start_consuming()

        # We've exited the loop so close the connection
        self._close()

        # Delete our processor
        del self._processor

        # Log that we're done
        self._logger.info( '%s: Stopped processing messages', self.name)

    def shutdown(self):
        """Let our wait loops and processors know we want to shutdown"""
        self._logger.debug('%s: Shutdown request received.', self.name)

        # If we're already shutting down, note it for debugging purposes
        if self._shutdown:
            self._logger.debug('%s: Already shutting down', self.name)
            return

        self._shutdown = True
        self._cancel()

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
                'throttle_count': self._throttle_count,
                'queue_depth': self.queue_depth}

    @property
    def queue_depth(self):
        """Get the queue depth from a passive queue declare.

        :returns: int
        """
        response = self._channel.queue_declare(self._queue_name, True)
        self._logger.debug('queue_depth response: %r', response)
        return response[1]


class LegacyMessage(object):

    def __init__(self, method, header, body):

        self.delivery_tag = method.delivery_tag
        self.delivery_info = {'routing_key': method.routing_key}
        self.header = header
        self.body = body
