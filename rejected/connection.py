"""
Connection
==========
Class that is used to manage connection and communication state.

"""
import logging
import os

import pika
from pika import spec

from rejected import state

LOGGER = logging.getLogger(__name__)


class Connection(state.State):
    """Contains the connection to RabbitMQ used by
    :class:`~rejected.process.Process` and
    :class:`~rejected.consumer.Consumer`. This is an internal object but, but
    is accessed in the consumer for publishing.

    """
    HB_INTERVAL = 300
    STATE_CLOSED = 0x08

    def __init__(self, name, config, consumer_name, should_consume,
                 publisher_confirmations, io_loop, callbacks):
        super(Connection, self).__init__()
        self.blocked = False
        self.callbacks = callbacks
        self.channel = None
        self.confirmation_futures = {}
        self.config = config
        self.delivery_tag = 0
        self.should_consume = should_consume
        self.consumer_tag = '{}-{}'.format(consumer_name, os.getpid())
        self.io_loop = io_loop
        self.last_confirmation = 0
        self.name = name
        self.publisher_confirmations = publisher_confirmations
        self.handle = self.connect()

        # Override STOPPED with CLOSED
        self.STATES[0x08] = 'CLOSED'

    @property
    def is_closed(self):
        """Returns ``True`` if the connection is closed.

        :rtype: bool

        """
        return self.is_stopped

    def add_confirmation_future(self, future):
        """Invoked by :class:`~rejected.consumer.Consumer` when publisher
        confirmations are enabled, containing a stack of futures to finish,
        by delivery tag, when RabbitMQ confirms the delivery.

        :param tornado.concurrent.Future future: The future to resolve

        """
        self.delivery_tag += 1
        self.confirmation_futures[self.delivery_tag] = future

    def clear_confirmation_futures(self):
        """Invoked by :class:`~rejected.consumer.Consumer` when process has
        finished and publisher confirmations are enabled.

        """
        self.confirmation_futures = {}

    def connect(self):
        """Create the low-level AMQP connection to RabbitMQ.

        :rtype: pika.TornadoConnection

        """
        self.set_state(self.STATE_CONNECTING)
        return pika.TornadoConnection(
            self._connection_parameters,
            on_open_callback=self.on_open,
            on_open_error_callback=self.on_open_error,
            on_close_callback=self.on_closed,
            stop_ioloop_on_close=False,
            custom_ioloop=self.io_loop)

    def shutdown(self):
        """Start the connection shutdown process, cancelling any active
        consuming and closing the channel if the connection is not active.

        """
        if self.is_shutting_down:
            LOGGER.debug('Connection %s is already shutting down', self.name)
            return

        self.set_state(self.STATE_SHUTTING_DOWN)
        LOGGER.debug('Connection %s is shutting down', self.name)
        if not self.is_active:
            return self.channel.close()
        LOGGER.debug('Connection %s is sending a Basic.Cancel to RabbitMQ',
                     self.name)
        self.channel.basic_cancel(self.on_consumer_cancelled,
                                  self.consumer_tag)

    def on_open(self, _handle):
        """Invoked when the connection is opened

        :type _handle: pika.adapters.tornado_connection.TornadoConnection

        """
        LOGGER.debug('Connection %s is open', self.name)
        self.handle.channel(self.on_channel_open)
        self.handle.add_on_connection_blocked_callback(self.on_blocked)
        self.handle.add_on_connection_unblocked_callback(self.on_unblocked)

    def on_open_error(self, *args, **kwargs):
        LOGGER.error('Connection %s failure %r %r', self.name, args, kwargs)
        self.set_state(self.STATE_CLOSED)
        self.callbacks.on_open_error(self.name)

    def on_closed(self, _connection, status_code, status_text):
        self.set_state(self.STATE_CLOSED)
        LOGGER.debug('Connection %s closed (%s) %s',
                     self.name, status_code, status_text)
        self.callbacks.on_closed(self.name)

    def on_blocked(self, frame):
        LOGGER.warning('Connection %s is blocked: %r', frame)
        self.blocked = True
        self.callbacks.on_blocked(self.name)

    def on_unblocked(self, frame):
        LOGGER.warning('Connection %s is unblocked: %r', frame)
        self.blocked = False
        self.callbacks.on_unblocked(self.name)

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened. It
        will change the state to IDLE, add the callbacks and setup the channel
        to start consuming.

        :param pika.channel.Channel channel: The channel object

        """
        LOGGER.debug('Connection %s channel is now open', self.name)
        self.set_state(self.STATE_IDLE)
        self.channel = channel
        self.channel.add_on_close_callback(self.on_channel_closed)
        self.channel.add_on_cancel_callback(self.on_consumer_cancelled)
        self.channel.add_on_return_callback(self.on_return)
        if self.publisher_confirmations:
            self.delivery_tag = 0
            self.channel.confirm_delivery(self.on_confirmation)
        self.callbacks.on_ready(self.name)

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
        LOGGER.debug('Connection %s channel was closed: (%s) %s',
                     self.name, reply_code, reply_text)
        del self.channel
        if self.is_running:
            self.set_state(self.STATE_CONNECTING)
            self.handle.channel(self.on_channel_open)
        elif self.is_shutting_down:
            LOGGER.debug('Connection %s closing', self.name)
            self.handle.close()

    def consume(self, queue_name, no_ack, prefetch_count):
        """Consume messages from RabbitMQ, changing the state, QoS and issuing
        the RPC to RabbitMQ to start delivering messages.

        :param str queue_name: The name of the queue to consume from
        :param False no_ack: Enable no-ack mode
        :param int prefetch_count: The number of messages to prefetch

        """
        self.set_state(self.STATE_ACTIVE)
        self.channel.basic_qos(self.on_qos_set, 0, prefetch_count, False)
        self.channel.basic_consume(consumer_callback=self.on_delivery,
                                   queue=queue_name,
                                   no_ack=no_ack,
                                   consumer_tag=self.consumer_tag)

    def on_qos_set(self, frame):
        """Invoked by pika when the QoS is set

        :param pika.frame.Frame frame: The QoS Frame

        """
        LOGGER.debug("Connection %s QoS was set: %r", self.name, frame)

    def on_consumer_cancelled(self, _frame):
        """Invoked by pika when a ``Basic.Cancel`` or ``Basic.CancelOk``
        is received.

        :param _frame: The Basic.Cancel or Basic.CancelOk frame
        :type _frame: pika.frame.Frame

        """
        LOGGER.debug('Connection %s consumer has been cancelled', self.name)
        if self.is_shutting_down:
            self.channel.close()
        else:
            self.set_state(self.STATE_IDLE)

    def on_confirmation(self, frame):
        """Invoked by pika when RabbitMQ responds to a Basic.Publish RPC
        command, passing in either a Basic.Ack or Basic.Nack frame with
        the delivery tag of the message that was published. The delivery tag
        is an integer counter indicating the message number that was sent
        on the channel via Basic.Publish.

        :param pika.frame.Method frame: Basic.Ack or Basic.Nack frame

        """
        delivered = frame.method.NAME.split('.')[1].lower() == 'ack'
        LOGGER.debug('Connection %s received delivery confirmation '
                     '(Delivered: %s)', self.name, delivered)
        if frame.method.multiple:
            for index in range(self.last_confirmation + 1,
                               frame.method.delivery_tag):
                self.confirm_delivery(index, delivered)
        self.confirm_delivery(frame.method.delivery_tag, delivered)
        self.last_confirmation = frame.method.delivery_tag

    def confirm_delivery(self, delivery_tag, delivered):
        """Invoked by RabbitMQ when it is confirming delivery via a Basic.Ack

        :param int delivery_tag: The message # being confirmed
        :param bool delivered: Was the message delivered

        """
        LOGGER.debug('Confirming delivery of %i: %s', delivery_tag, delivered)
        try:
            self.confirmation_futures[delivery_tag].set_result(delivered)
        except KeyError:
            LOGGER.warning('Attempted to confirm delivery without future: %r',
                           delivery_tag)

    def on_delivery(self, channel, method, properties, body):
        """Invoked by pika when RabbitMQ delivers a message from a queue.

        :param channel: The channel the message was delivered on
        :type channel: pika.channel.Channel
        :param method: The AMQP method frame
        :type method: pika.frame.Frame
        :param properties: The AMQP message properties
        :type properties: pika.spec.Basic.Properties
        :param bytes body: The message body

        """
        self.callbacks.on_delivery(
            self.name, channel, method, properties, body)

    def on_return(self, channel, method, properties, body):
        """Invoked by RabbitMQ when it returns a message that was published.

        :param channel: The channel the message was delivered on
        :type channel: pika.channel.Channel
        :param method: The AMQP method frame
        :type method: pika.frame.Frame
        :param properties: The AMQP message properties
        :type properties: pika.spec.Basic.Properties
        :param bytes body: The message body

        """
        self.callbacks.on_return(self.name, channel, method, properties, body)

    @property
    def _connection_parameters(self):
        """Return connection parameters for a pika connection.

        :rtype: pika.ConnectionParameters

        """
        return pika.ConnectionParameters(
            self.config.get('host', 'localhost'),
            self.config.get('port', 5672),
            self.config.get('vhost', '/'),
            pika.PlainCredentials(
                self.config.get('user', 'guest'),
                self.config.get('password', self.config.get('pass', 'guest'))),
            ssl=self.config.get('ssl', False),
            frame_max=self.config.get('frame_max', spec.FRAME_MAX_SIZE),
            socket_timeout=self.config.get('socket_timeout', 10),
            heartbeat_interval=self.config.get(
                'heartbeat_interval', self.HB_INTERVAL))
