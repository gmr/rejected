"""
Connection
==========
Class that is used to manage connection and communication state.

"""
import collections
import logging
import os

import pika
from pika import spec

from rejected import errors, log, state, utils

LOGGER = logging.getLogger(__name__)

Published = collections.namedtuple(
    'Published', ['delivery_tag', 'message_id',
                  'exchange', 'routing_key', 'future'])
"""Used to keep track of published messages for delivery confirmations"""


Callbacks = collections.namedtuple(
    'callbacks', ['on_ready', 'on_open_error',
                  'on_closed', 'on_blocked', 'on_unblocked',
                  'on_confirmation', 'on_delivery'])


class Connection(state.State):
    """Contains the connection to RabbitMQ used by
    :class:`~rejected.process.Process` and
    :class:`~rejected.consumer.Consumer`. This is an internal object but, but
    is accessed in the consumer for publishing.

    """
    HB_INTERVAL = 300
    STATE_CLOSED = 0x08
    STATE_CONNECTED = 0x09

    def __init__(self, name, config, consumer_name, should_consume,
                 publisher_confirmations, io_loop, callbacks):
        super(Connection, self).__init__()
        self.blocked = False
        self.callbacks = callbacks
        self.channel = None
        self.config = config
        self.correlation_id = None
        self.delivery_tag = 0
        self.should_consume = should_consume
        self.consumer_tag = '{}-{}-{}'.format(name, consumer_name, os.getpid())
        self.io_loop = io_loop
        self.last_confirmation = 0
        self.logger = log.CorrelationIDAdapter(LOGGER, {'parent': self})
        self.name = name
        self.published_messages = []
        self.publisher_confirmations = publisher_confirmations
        self.handle = None
        self.connect()

        # Set specific state values
        self.STATES[0x08] = 'CLOSED'
        self.STATES[0x09] = 'CONNECTED'

    @property
    def is_closed(self):
        """Returns ``True`` if the connection is closed.

        :rtype: bool

        """
        return self.is_stopped

    @property
    def is_connected(self):
        """Returns ``True`` if the connection is open

        :rtype: bool

        """
        return self.state in [self.STATE_ACTIVE, self.STATE_CONNECTED]

    def add_confirmation_future(self, exchange, routing_key, properties,
                                future):
        """Invoked by :class:`~rejected.consumer.Consumer` when publisher
        confirmations are enabled, containing a stack of futures to finish,
        by delivery tag, when RabbitMQ confirms the delivery.

        :param str exchange: The exchange the message was published to
        :param str routing_key: The routing key that was used
        :param properties: AMQP message properties of published message
        :type properties: pika.spec.Basic.Properties
        :param tornado.concurrent.Future future: The future to resolve

        """
        self.delivery_tag += 1
        self.published_messages.append(
            Published(self.delivery_tag, properties.message_id, exchange,
                      routing_key, future))

    def clear_confirmation_futures(self):
        """Invoked by :class:`~rejected.consumer.Consumer` when process has
        finished and publisher confirmations are enabled.

        """
        self.published_messages = []

    def connect(self):
        """Create the low-level AMQP connection to RabbitMQ.

        :rtype: pika.TornadoConnection

        """
        self.set_state(self.STATE_CONNECTING)
        self.handle = pika.TornadoConnection(
            self._connection_parameters,
            on_open_callback=self.on_open,
            on_open_error_callback=self.on_open_error,
            stop_ioloop_on_close=False,
            custom_ioloop=self.io_loop)

    def reset(self):
        self.channel = None
        self.handle = None
        self.correlation_id = None
        self.published_messages = []
        self.delivery_tag = 0
        self.last_confirmation = 0
        self.set_state(self.STATE_CLOSED)

    def shutdown(self):
        """Start the connection shutdown process, cancelling any active
        consuming and closing the channel if the connection is not active.

        """
        if self.is_shutting_down:
            self.logger.debug('Already shutting down')
            return

        self.set_state(self.STATE_SHUTTING_DOWN)
        self.logger.debug('Shutting down connection')
        if not self.is_active:
            return self.channel.close()
        self.logger.debug('Sending a Basic.Cancel to RabbitMQ')
        self.channel.basic_cancel(self.on_consumer_cancelled,
                                  self.consumer_tag)

    def on_open(self, _handle):
        """Invoked when the connection is opened

        :type _handle: pika.adapters.tornado_connection.TornadoConnection

        """
        self.logger.debug('Connection opened')
        self.handle.add_on_connection_blocked_callback(self.on_blocked)
        self.handle.add_on_connection_unblocked_callback(self.on_unblocked)
        self.handle.add_on_close_callback(self.on_closed)
        self.handle.channel(self.on_channel_open)

    def on_open_error(self, *args, **kwargs):
        self.logger.error('Connection failure %r %r', args, kwargs)
        self.set_state(self.STATE_CLOSED)
        self.callbacks.on_open_error(self.name)

    def on_closed(self, _connection, reply_code, reply_text):
        self.set_state(self.STATE_CLOSED)
        self.logger.debug('Connection closed (%s) %s', reply_code, reply_text)
        self.reset()
        self.callbacks.on_closed(self.name)

    def on_blocked(self, frame):
        self.logger.warning('Connection blocked: %r', frame)
        self.blocked = True
        self.callbacks.on_blocked(self.name)

    def on_unblocked(self, frame):
        self.logger.warning('Connection unblocked: %r', frame)
        self.blocked = False
        self.callbacks.on_unblocked(self.name)

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened. It
        will change the state to CONNECTED, add the callbacks and setup the
        channel to start consuming.

        :param pika.channel.Channel channel: The channel object

        """
        self.logger.debug('Channel opened')
        self.set_state(self.STATE_CONNECTED)
        self.channel = channel
        self.channel.add_on_close_callback(self.on_channel_closed)
        self.channel.add_on_cancel_callback(self.on_consumer_cancelled)
        if self.publisher_confirmations:
            self.delivery_tag = 0
            self.channel.confirm_delivery(self.on_confirmation)
            self.channel.add_on_return_callback(self.on_return)
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
        self.logger.warning('Channel was closed: (%s) %s - %s',
                            reply_code, reply_text, self.state_description)
        if not (400 <= reply_code <= 499):
            self.set_state(self.STATE_CLOSED)
            return

        if self.is_shutting_down:
            self.logger.debug('Closing connection')
            self.handle.close()
            return

        self.set_state(self.STATE_CONNECTING)
        self.handle.channel(self.on_channel_open)

        pending = self.pending_confirmations()
        if not pending:
            raise errors.RabbitMQException(self.name, reply_code, reply_text)
        pending[0][1].future.set_exception(
                errors.RabbitMQException(self.name, reply_code, reply_text))

    def consume(self, queue_name, no_ack, prefetch_count):
        """Consume messages from RabbitMQ, changing the state, QoS and issuing
        the RPC to RabbitMQ to start delivering messages.

        :param str queue_name: The name of the queue to consume from
        :param False no_ack: Enable no-ack mode
        :param int prefetch_count: The number of messages to prefetch

        """
        if self.state == self.STATE_ACTIVE:
            self.logger.debug('%s already consuming', self.name)
            return
        self.set_state(self.STATE_ACTIVE)
        self.channel.basic_qos(self.on_qos_set, 0, prefetch_count, False)
        self.channel.basic_consume(
            consumer_callback=self.on_delivery, queue=queue_name,
            no_ack=no_ack, consumer_tag=self.consumer_tag)

    def on_qos_set(self, frame):
        """Invoked by pika when the QoS is set

        :param pika.frame.Frame frame: The QoS Frame

        """
        self.logger.debug('QoS was set: %r', frame)

    def on_consumer_cancelled(self, _frame):
        """Invoked by pika when a ``Basic.Cancel`` or ``Basic.CancelOk``
        is received.

        :param _frame: The Basic.Cancel or Basic.CancelOk frame
        :type _frame: pika.frame.Frame

        """
        self.logger.debug('Consumer has been cancelled')
        if self.is_shutting_down:
            self.channel.close()
        else:
            self.set_state(self.STATE_CONNECTED)

    def on_confirmation(self, frame):
        """Invoked by pika when RabbitMQ responds to a Basic.Publish RPC
        command, passing in either a Basic.Ack or Basic.Nack frame with
        the delivery tag of the message that was published. The delivery tag
        is an integer counter indicating the message number that was sent
        on the channel via Basic.Publish.

        :param pika.frame.Method frame: Basic.Ack or Basic.Nack frame

        """
        delivered = frame.method.NAME.split('.')[1].lower() == 'ack'
        self.logger.debug('Received publisher confirmation (Delivered: %s)',
                          delivered)
        if frame.method.multiple:
            for index in range(self.last_confirmation + 1,
                               frame.method.delivery_tag):
                self.confirm_delivery(index, delivered)
        self.confirm_delivery(frame.method.delivery_tag, delivered)
        self.last_confirmation = frame.method.delivery_tag

    def confirm_delivery(self, delivery_tag, delivered):
        """Invoked by RabbitMQ when it is confirming delivery via a Basic.Ack

        :param
        :param int delivery_tag: The message # being confirmed
        :param bool delivered: Was the message delivered

        """
        for offset, msg in self.pending_confirmations():
            if delivery_tag == msg.delivery_tag:
                self.published_messages[offset].future.set_result(delivered)
                return
        for msg in self.published_messages:
            if msg.delivery_tag == delivery_tag and msg.future.done():
                return
        self.logger.warning('Attempted to confirm publish without future: %r',
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
        pending = self.pending_confirmations()
        if not pending:  # Exit early if there are no pending messages
            self.logger.warning('RabbitMQ returned message %s and no pending '
                                'messages are unconfirmed',
                                utils.message_info(method.exchange,
                                                   method.routing_key,
                                                   properties))
            return

        self.logger.warning('RabbitMQ returned message %s: (%s) %s',
                            utils.message_info(method.exchange,
                                               method.routing_key, properties),
                            method.reply_code, method.reply_text)

        # Try and match the exact message or first message published that
        # matches the exchange and routing key
        for offset, msg in pending:
            if (msg.message_id == properties.message_id or
                    (msg.exchange == method.exchange and
                     msg.routing_key == method.routing_key)):
                self.published_messages[offset].future.set_result(False)
                return

        # Handle the case where we only can go on message ordering
        self.published_messages[0].future.set_result(False)

    def pending_confirmations(self):
        """Return all published messages that have yet to be acked, nacked, or
        returned.

        :return: [(int, Published)]

        """
        return sorted([(idx, msg)
                       for idx, msg in enumerate(self.published_messages)
                       if not msg.future.done()],
                      key=lambda x: x[1].delivery_tag)

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
