import logging
import os
import ssl
import typing

import pika
import pika.channel
import pika.frame
from pika import exceptions, spec
from pika.adapters import asyncio_connection

from rejected import models, state

LOGGER = logging.getLogger(__name__)


class Connection(state.State):

    STATE_CLOSED = 0x08

    def __init__(self, name: str, config: models.Connection,
                 consumer_name: str, should_consume: bool,
                 publisher_confirmations: bool, callbacks: models.Callbacks):
        super().__init__()
        self.callbacks = callbacks
        self.channel = None
        self.config = config
        self.should_consume = should_consume
        self.consumer_tag = '{}-{}'.format(consumer_name, os.getpid())
        self.name = name
        self.publisher_confirm = publisher_confirmations
        self.connection = self.connect()

        # Override STOPPED with CLOSED
        self.STATES[0x08] = 'CLOSED'

    @property
    def is_closed(self) -> bool:
        return self.is_stopped

    def connect(self) -> asyncio_connection.AsyncioConnection:
        self.set_state(self.STATE_CONNECTING)
        connection = asyncio_connection.AsyncioConnection(
            self._connection_parameters)
        connection.add_on_close_callback(self.on_closed)
        connection.add_on_open_callback(self.on_open)
        connection.add_on_open_error_callback(self.on_open_error)
        return connection

    def shutdown(self) -> None:
        if self.is_stopping:
            LOGGER.debug('Connection %s is already stopping', self.name)
            return

        self.set_state(self.STATE_STOPPING)
        LOGGER.debug('Connection %s is stopping', self.name)
        if self.is_active:
            LOGGER.debug('Connection %s is sending a Basic.Cancel to RabbitMQ',
                         self.name)
            self.channel.basic_cancel(self.on_consumer_cancelled,
                                      self.consumer_tag)
        else:
            self.channel.close()

    def on_open(self,
                connection: asyncio_connection.AsyncioConnection) -> None:
        """Invoked when the connection is opened"""
        LOGGER.debug('Connection %s is open (%r)', self.name, connection)
        self.connection = connection
        try:
            self.connection.channel(on_open_callback=self.on_channel_open)
        except exceptions.ConnectionClosed:
            LOGGER.warning('Channel open on closed connection')
            self.set_state(self.STATE_CLOSED)
            self.callbacks.on_closed(self.name)
            return

    def on_open_error(self, *args, **kwargs) -> None:
        LOGGER.error('Connection %s failure %r %r', self.name, args, kwargs)
        self.on_failure()

    def on_closed(self, _conn: asyncio_connection.AsyncioConnection,
                  error: typing.Optional[BaseException]) -> None:
        if self.is_connecting:
            LOGGER.error('Connection %s failure while connecting (%s)',
                         self.name, error)
            self.on_failure()
        elif not self.is_closed:
            self.set_state(self.STATE_CLOSED)
            LOGGER.info('Connection %s closed (%s) %s', self.name, error)
            self.callbacks.on_closed(self.name)

    def on_channel_open(self, channel: pika.channel.Channel) -> None:
        """This method is invoked by pika when the channel has been opened. It
        will change the state to IDLE, add the callbacks and set up the channel
        to start consuming.

        """
        LOGGER.debug('Connection %s channel is now open', self.name)
        self.set_state(self.STATE_IDLE)
        self.channel = channel
        self.channel.add_on_close_callback(self.on_channel_closed)
        self.channel.add_on_cancel_callback(self.on_consumer_cancelled)
        self.channel.add_on_return_callback(self.on_return)
        if self.publisher_confirm:
            self.channel.confirm_delivery(self.on_confirmation)
        self.callbacks.on_ready(self.name)

    def on_channel_closed(self, _channel: pika.channel.Channel,
                          reply_code: int, reply_text: str) -> None:
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shut down the object.

        :param _channel: The AMQP Channel
        :param reply_code: The AMQP reply code
        :param reply_text: The AMQP reply text

        """
        del self.channel
        if reply_code <= 0 or reply_code == 404:
            LOGGER.error('Channel Error (%r): %s', reply_code, reply_text
                         or 'unknown')
            self.on_failure()
        elif self.is_stopping:
            LOGGER.debug('Connection %s closing', self.name)
            self.connection.close()
        elif self.is_running:
            LOGGER.warning('Connection %s channel was closed: (%s) %s',
                           self.name, reply_code, reply_text)
            try:
                self.connection.channel(on_open_callback=self.on_channel_open)
            except exceptions.ConnectionClosed as error:
                LOGGER.warning('Error raised while creating new channel: %s',
                               error)
                self.on_failure()
            else:
                self.set_state(self.STATE_CONNECTING)

    def on_failure(self) -> None:
        LOGGER.info('Connection failure, terminating connection')
        self.set_state(self.STATE_CLOSED)
        try:
            self.connection.close()
        except AttributeError:
            pass
        del self.connection
        self.callbacks.on_connection_failure(self.name)

    def consume(self, queue_name: str, no_ack: bool,
                prefetch_count: int) -> None:
        self.set_state(self.STATE_ACTIVE)
        self.channel.basic_qos(self.on_qos_set, 0, prefetch_count, False)
        self.channel.basic_consume(consumer_callback=self.on_delivery,
                                   queue=queue_name,
                                   no_ack=no_ack,
                                   consumer_tag=self.consumer_tag)

    def on_qos_set(self, frame: pika.frame.Method) -> None:
        """Invoked by pika when the QoS is set"""
        LOGGER.debug('Connection %s QoS was set: %r', self.name, frame)

    def on_consumer_cancelled(self, _frame: pika.frame.Method) -> None:
        """Invoked by pika when a ``Basic.Cancel`` or ``Basic.CancelOk``
        is received.

        """
        LOGGER.info('Connection %s consumer has been cancelled', self.name)
        if not self.is_stopping:
            self.set_state(self.STATE_STOPPING)
        self.channel.close()

    def on_confirmation(self, frame: pika.frame.Method) -> None:
        """Invoked by pika when RabbitMQ responds to a Basic.Publish RPC
        command, passing in either a Basic.Ack or Basic.Nack frame with
        the delivery tag of the message that was published. The delivery tag
        is an integer counter indicating the message number that was sent
        on the channel via Basic.Publish.

        """
        delivered = frame.NAME.split('.')[1].lower() == 'ack'
        LOGGER.debug(
            'Connection %s received delivery confirmation '
            '(Delivered: %s)', self.name, delivered)
        self.callbacks.on_confirmation(self.name, delivered,
                                       frame.method.delivery_tag)

    def on_delivery(self, channel: pika.channel.Channel,
                    method: spec.Basic.Deliver,
                    properties: pika.spec.BasicProperties,
                    body: bytes) -> None:
        self.callbacks.on_delivery(self.name, channel, method, properties,
                                   body)

    def on_return(self, channel: pika.channel.Channel,
                  method: spec.Basic.Deliver,
                  properties: pika.spec.BasicProperties, body: bytes) -> None:
        self.callbacks.on_return(self.name, channel, method, properties, body)

    @property
    def _connection_parameters(self) -> pika.ConnectionParameters:
        """Return connection parameters for a pika connection."""
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_options = pika.SSLOptions(context) if self.config.ssl else None
        return pika.ConnectionParameters(
            self.config.hostname,
            self.config.port,
            self.config.virtual_host,
            pika.PlainCredentials(self.config.username,
                                  str(self.config.password)),
            ssl_options=ssl_options,
            heartbeat=self.config.heartbeat_interval)
