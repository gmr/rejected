import logging
import os
import ssl
import typing

import pika
import pika.channel
import pika.exceptions
import pika.frame
import pika.spec
from pika.adapters import asyncio_connection

from rejected import models, state

LOGGER = logging.getLogger(__name__)


class Connection(state.State):
    HB_INTERVAL: typing.ClassVar[int] = 300
    STATE_CLOSED: typing.ClassVar[int] = 0x08
    STATES: typing.ClassVar[dict[int, str]] = {
        **state.State.STATES,
        STATE_CLOSED: 'Closed',
    }

    def __init__(
        self,
        name: str,
        config: models.ConnectionConfig,
        consumer_name: str,
        should_consume: bool,
        publisher_confirmations: bool,
        callbacks: models.Callbacks,
    ):
        super().__init__()
        self.blocked: bool = False
        self.callbacks: models.Callbacks = callbacks
        self.channel: pika.channel.Channel | None = None
        self.config: models.ConnectionConfig = config
        self.should_consume: bool = should_consume
        self.consumer_tag: str = f'{consumer_name}-{os.getpid()}'
        self.name: str = name
        self.publisher_confirm: bool = publisher_confirmations
        self.connection: asyncio_connection.AsyncioConnection = self.connect()

    def connect(self) -> asyncio_connection.AsyncioConnection:
        """Setup the AsyncioConnection which connects to RabbitMQ
        automatically with connection callbacks for when the connection is
        opened, when there is an error opening a connection or when a
        previously opened connection is closed.

        """
        self.set_state(self.STATE_CONNECTING)
        return asyncio_connection.AsyncioConnection(
            self._connection_parameters,
            on_open_callback=self.on_open,
            on_open_error_callback=self.on_open_error,
            on_close_callback=self.on_closed,
        )

    @property
    def is_closed(self) -> bool:
        return self.is_stopped

    def shutdown(self) -> None:
        if self.is_shutting_down:
            LOGGER.debug('Connection %s is already shutting down', self.name)
            return

        self.set_state(self.STATE_SHUTTING_DOWN)
        LOGGER.debug('Connection %s is shutting down', self.name)
        if self.is_active and self.channel:
            LOGGER.debug(
                'Connection %s is sending a Basic.Cancel to RabbitMQ',
                self.name,
            )
            self.channel.basic_cancel(
                self.consumer_tag, self.on_consumer_cancelled
            )
        elif self.channel:
            self.channel.close()

    def on_open(
        self, connection: asyncio_connection.AsyncioConnection
    ) -> None:
        """Invoked when the connection is opened

        :type connection: pika.adapters.asyncio_connection.AsyncioConnection

        """
        LOGGER.debug('Connection %s is open (%r)', self.name, connection)
        self.connection = connection
        try:
            self.connection.channel(on_open_callback=self.on_channel_open)
        except pika.exceptions.ConnectionClosed:
            LOGGER.warning('Channel open on closed connection')
            self.set_state(self.STATE_CLOSED)
            self.callbacks.on_closed(self.name)
            return
        self.connection.add_on_connection_blocked_callback(
            self.on_blocked,  # type: ignore[arg-type]
        )
        self.connection.add_on_connection_unblocked_callback(
            self.on_unblocked,  # type: ignore[arg-type]
        )

    def on_open_error(
        self,
        _connection: asyncio_connection.AsyncioConnection,
        error: BaseException | str,
    ) -> None:
        LOGGER.error('Connection %s failure: %r', self.name, error)
        self.on_failure()

    def on_closed(
        self,
        _connection: asyncio_connection.AsyncioConnection,
        reason: BaseException | str,
    ) -> None:
        if self.is_connecting:
            LOGGER.error(
                'Connection %s failure while connecting: %r', self.name, reason
            )
            self.on_failure()
        elif not self.is_closed:
            self.set_state(self.STATE_CLOSED)
            LOGGER.info('Connection %s closed: %r', self.name, reason)
            self.callbacks.on_closed(self.name)

    def on_blocked(
        self,
        _conn: asyncio_connection.AsyncioConnection,
        method: pika.frame.Method[pika.spec.Connection.Blocked],
    ) -> None:
        LOGGER.warning('Connection %s is blocked: %r', self.name, method)
        self.blocked = True
        self.callbacks.on_blocked(self.name)

    def on_unblocked(
        self,
        _conn: asyncio_connection.AsyncioConnection,
        method: pika.frame.Method[pika.spec.Connection.Unblocked],
    ) -> None:
        LOGGER.warning('Connection %s is unblocked: %r', self.name, method)
        self.blocked = False
        self.callbacks.on_unblocked(self.name)

    def on_channel_open(self, channel: pika.channel.Channel) -> None:
        """This method is invoked by pika when the channel has been opened. It
        will change the state to IDLE, add the callbacks and setup the channel
        to start consuming.

        """
        LOGGER.debug('Connection %s channel is now open', self.name)
        self.set_state(self.STATE_IDLE)
        self.channel = channel
        channel.add_on_close_callback(self.on_channel_closed)
        channel.add_on_cancel_callback(self.on_consumer_cancelled)
        channel.add_on_return_callback(self.on_return)
        if self.publisher_confirm:
            channel.confirm_delivery(ack_nack_callback=self.on_confirmation)
        self.callbacks.on_ready(self.name)

    def on_channel_closed(
        self, _channel: pika.channel.Channel, closing_reason: Exception
    ) -> None:
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel _channel: The AMQP Channel
        :param Exception closing_reason: The channel closed exception

        """
        del self.channel

        if isinstance(closing_reason, pika.exceptions.ChannelClosed):
            reply_code = closing_reason.reply_code
            reply_text = closing_reason.reply_text
        else:
            reply_code = 0
            reply_text = str(closing_reason) or 'unknown'

        if reply_code <= 0 or reply_code == 404:
            LOGGER.error(
                'Channel Error (%r): %s', reply_code, reply_text or 'unknown'
            )
            self.on_failure()
        elif self.is_shutting_down:
            LOGGER.debug('Connection %s closing', self.name)
            self.connection.close()
        elif self.is_running:
            LOGGER.warning(
                'Connection %s channel was closed: (%s) %s',
                self.name,
                reply_code,
                reply_text,
            )
            try:
                self.connection.channel(on_open_callback=self.on_channel_open)
            except (
                pika.exceptions.ConnectionWrongStateError,
                pika.exceptions.ConnectionClosed,
            ) as error:
                LOGGER.warning(
                    'Error raised while creating new channel: %s', error
                )
                self.on_failure()
            else:
                self.set_state(self.STATE_CONNECTING)

    def on_failure(self) -> None:
        LOGGER.info('Connection failure, terminating connection')
        self.set_state(self.STATE_CLOSED)
        try:
            self.connection.close()
        except (AttributeError, pika.exceptions.ConnectionWrongStateError):
            pass
        del self.connection
        self.callbacks.on_connection_failure(self.name)

    def consume(
        self, queue_name: str, no_ack: bool, prefetch_count: int
    ) -> None:
        """Configure quality of service and issue Basic.Consume command

        :param queue_name: The queue to consume from. Use the empty string
            to specify the most recent server-named queue for this channel
        :param no_ack: if set to True, automatic acknowledgement mode
            will be used (see http://www.rabbitmq.com/confirms.html).
            This corresponds with the 'no_ack' parameter in the basic.consume
            AMQP 0.9.1 method
        :param prefetch_count: Specifies a prefetch window in terms of
            whole messages.

        """
        self.set_state(self.STATE_ACTIVE)
        assert self.channel is not None
        self.channel.basic_qos(
            callback=self.on_qos_set,
            prefetch_size=0,
            prefetch_count=prefetch_count,
            global_qos=False,
        )
        self.channel.basic_consume(
            queue=queue_name,
            on_message_callback=self.on_delivery,
            auto_ack=no_ack,
            consumer_tag=self.consumer_tag,
        )

    def on_qos_set(
        self, method: pika.frame.Method[pika.spec.Basic.QosOk]
    ) -> None:
        """Invoked by pika when the QoS is set"""
        LOGGER.debug('Connection %s QoS was set: %r', self.name, method)

    def on_consumer_cancelled(
        self, _method: pika.frame.Method[pika.spec.Basic.CancelOk]
    ) -> None:
        """Invoked by pika when a ``Basic.CancelOk`` is received."""
        LOGGER.info('Connection %s consumer has been cancelled', self.name)
        if not self.is_shutting_down:
            self.set_state(self.STATE_SHUTTING_DOWN)
        elif self.channel:
            self.channel.close()

    def on_confirmation(
        self,
        method: pika.frame.Method[pika.spec.Basic.Ack | pika.spec.Basic.Nack],
    ) -> None:
        """Invoked by pika when RabbitMQ responds to a Basic.Publish RPC
        command, passing in either a Basic.Ack or Basic.Nack frame with
        the delivery tag of the message that was published. The delivery tag
        is an integer counter indicating the message number that was sent
        on the channel via Basic.Publish.

        """
        delivered = isinstance(method.method, pika.spec.Basic.Ack)
        LOGGER.debug(
            'Connection %s received delivery confirmation (Delivered: %s)',
            self.name,
            delivered,
        )
        self.callbacks.on_confirmation(
            self.name, delivered, method.method.delivery_tag
        )

    def on_delivery(
        self,
        channel: pika.channel.Channel,
        method: pika.spec.Basic.Deliver,
        properties: pika.BasicProperties,
        body: bytes,
    ) -> None:
        """Received on delivery of a message from RabbitMQ."""
        self.callbacks.on_delivery(
            self.name, channel, method, properties, body
        )

    def on_return(
        self,
        channel: pika.channel.Channel,
        method: pika.spec.Basic.Return,
        properties: pika.BasicProperties,
        body: bytes,
    ) -> None:
        """Received on return of a message from RabbitMQ."""
        self.callbacks.on_return(self.name, channel, method, properties, body)

    @property
    def _connection_parameters(self) -> pika.ConnectionParameters:
        """Return connection parameters for a pika connection."""
        return pika.ConnectionParameters(
            self.config.host,
            self.config.port,
            self.config.vhost,
            pika.PlainCredentials(self.config.user, self.config.password),
            ssl_options=self._ssl_options,  # type: ignore[arg-type]
            frame_max=self.config.frame_max,
            socket_timeout=self.config.socket_timeout,
            heartbeat=self.config.heartbeat_interval,
        )

    @property
    def _ssl_options(self) -> pika.SSLOptions | None:
        """Return the `pika.SSLOptions` parameter for the pika connection

        The expected ssl_options values in the config are:

            * ca_certs
            * ca_path
            * ca_data
            * prototcol
            * certfile
            * keyfile
            * password
            * ciphers

        """
        if not self.config.ssl_options:
            return None

        protocol = self.config.ssl_options.protot, ssl.PROTOCOL_TLS_CLIENT)
        if isinstance(protocol, str):
            protocol = getattr(ssl, protocol)
        context = ssl.SSLContext(protocol)

        # Load a set of certification authority (CA) certificates
        if any(
            [
                ssl_options.get('ca_certs'),
                ssl_options.get('ca_path'),
                ssl_options.get('ca_data'),
            ]
        ):
            context.load_verify_locations(
                ssl_options.get('ca_certs'),
                ssl_options.get('ca_path'),
                ssl_options.get('ca_data'),
            )

        # Load a private key and the corresponding certificate
        if ssl_options.get('certfile'):
            certfile = ssl_options['certfile']
            keyfile = ssl_options.get('keyfile')
            password = ssl_options.get('password')
            context.load_cert_chain(certfile, keyfile, password)

        # Set the available ciphers for sockets created with this context
        if ssl_options.get('ciphers'):
            context.set_ciphers(ssl_options['ciphers'])

        return pika.SSLOptions(context=context)
