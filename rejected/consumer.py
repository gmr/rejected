"""
The :py:class:`Consumer` provides the backward-compatible consumer class
for rejected 3.x style consumers. :py:class:`TransactionConsumer` provides
a new concurrent consumer that receives a
:class:`~rejected.models.ProcessingContext`.

Both extend :class:`_Consumer` which implements the core contract.

"""

import asyncio
import contextlib
import datetime
import logging
import sys
import time
import typing
import uuid

import pika
from pika import channel
from pika import exceptions as pika_exceptions

from . import codecs, exceptions, log, models
from . import measurement as measurement_mod

LOGGER = logging.getLogger(__name__)

try:
    import sentry_sdk
except ImportError:
    sentry_sdk = None

# Re-export for backward compatibility
AVRO_DATUM_MIME_TYPE = codecs.AVRO_DATUM_MIME_TYPE

DEFAULT_CHANNEL = 'default'
_DROPPED_MESSAGE = 'X-Rejected-Dropped'
_PROCESSING_EXCEPTIONS = 'X-Processing-Exceptions'
_EXCEPTION_FROM = 'X-Exception-From'

_UNSET = object()


class _Consumer:
    """Base consumer class implementing the core contract.

    Not intended to be used directly — extend :class:`Consumer` or
    :class:`TransactionConsumer`.

    """

    DROP_EXCHANGE: typing.ClassVar[str | None] = None
    DROP_INVALID_MESSAGES: typing.ClassVar[bool] = False
    MESSAGE_TYPE: typing.ClassVar[str | None] = None
    ERROR_EXCHANGE: typing.ClassVar[str] = 'errors'
    ERROR_MAX_RETRY: typing.ClassVar[int | None] = None
    MESSAGE_AGE_KEY: typing.ClassVar[str] = 'message_age'
    ACK_PROCESSING_EXCEPTIONS: typing.ClassVar[bool] = False

    def __init__(
        self,
        settings: typing.Any,
        process: typing.Any,
        drop_invalid_messages: bool | None = None,
        message_type: str | None = None,
        error_exchange: str | None = None,
        error_max_retry: int | None = None,
        drop_exchange: str | None = None,
    ) -> None:
        self._channels: dict[str, channel.Channel] = {}
        self._correlation_id: str | None = None
        self._drop_exchange = drop_exchange or self.DROP_EXCHANGE
        self._drop_invalid = (
            self.DROP_INVALID_MESSAGES
            if drop_invalid_messages is None
            else drop_invalid_messages
        )
        self._error_exchange = error_exchange or self.ERROR_EXCHANGE
        self._error_max_retry = (
            self.ERROR_MAX_RETRY
            if error_max_retry is None
            else error_max_retry
        )
        self._process = process
        self._settings = settings
        self._initialized = False

        self._logger = logging.getLogger(
            settings.get('_import_module', __name__)
        )
        self.logger = log.CorrelationAdapter(self._logger, self)

        self.set_sentry_context('consumer', self.name)

    # --- Lifecycle hooks (override in subclasses) ---

    async def initialize(self) -> None:
        """Called once before the first message is processed."""
        pass

    async def shutdown(self) -> None:
        """Called when the process is stopping."""
        self.logger.debug('shutdown invoked')

    async def on_blocked(self, name: str) -> None:
        """Called when a connection is blocked."""
        self.logger.debug('Connection %s has been blocked', name)

    async def on_unblocked(self, name: str) -> None:
        """Called when a connection is unblocked."""
        self.logger.debug('Connection %s has been unblocked', name)

    def on_confirmation(
        self, name: str, delivered: bool, delivery_tag: str
    ) -> None:
        """Called when a message is confirmed by RabbitMQ."""
        pass

    # --- Core execute flow ---

    async def execute(self, ctx: models.ProcessingContext) -> models.Result:
        """Entry point called by the process for each message.

        Handles initialization, pre-validation, then delegates to
        :meth:`_run_consumer` which subclasses override.

        """
        if not self._initialized:
            await self.initialize()
            self._initialized = True

        result = self._pre_execute(ctx)
        if result is not None:
            ctx.result = result
            return result

        result = await self._run_consumer(ctx)
        ctx.result = result
        return result

    def _pre_execute(
        self, ctx: models.ProcessingContext
    ) -> models.Result | None:
        """Validate the message before processing.

        Returns a Result if the message should be dropped/rejected
        without calling the consumer, or None to proceed.

        """
        msg = ctx.message

        # Ensure correlation ID
        self._correlation_id = (
            msg.correlation_id or msg.message_id or str(uuid.uuid4())
        )

        if msg.message_type:
            self.set_sentry_context('type', msg.message_type)

        # Validate message type
        if self.MESSAGE_TYPE:
            expected = self.MESSAGE_TYPE
            if isinstance(expected, (tuple, list, set)):
                supported = msg.message_type in expected
            else:
                supported = msg.message_type == expected
            if not supported:
                self.logger.warning(
                    'Received unsupported message type: %s', msg.message_type
                )
                if self._drop_invalid:
                    if self._drop_exchange:
                        self._republish_dropped_message(ctx, 'invalid type')
                    return models.Result.MESSAGE_DROP
                return models.Result.MESSAGE_EXCEPTION

        # Check error retry limit
        if self._error_max_retry and _PROCESSING_EXCEPTIONS in (
            msg.headers or {}
        ):
            raw_count = msg.headers[_PROCESSING_EXCEPTIONS]
            count = (
                int(raw_count)
                if isinstance(raw_count, (int, float, str))
                else 0
            )
            if count >= self._error_max_retry:
                self.logger.warning(
                    'Dropping message with %i deaths due to ERROR_MAX_RETRY',
                    count,
                )
                if self._drop_exchange:
                    self._republish_dropped_message(
                        ctx, f'max retries ({count})'
                    )
                return models.Result.MESSAGE_DROP

        return None

    async def _run_consumer(
        self, ctx: models.ProcessingContext
    ) -> models.Result:
        """Override in subclasses to implement the processing flow."""
        raise NotImplementedError

    async def _handle_execution(
        self,
        ctx: models.ProcessingContext,
        handler: typing.Callable[[], typing.Awaitable[None]],
    ) -> models.Result:
        """Wrap a handler with standard error handling.

        :param ctx: The processing context
        :param handler: Async callable that runs prepare/process

        """
        try:
            await handler()
        except KeyboardInterrupt:
            self.logger.debug('CTRL-C')
            self._process.reject(ctx, True)
            self._process.stop()
            return models.Result.MESSAGE_REQUEUE
        except pika_exceptions.ChannelClosed as error:
            self.logger.critical(
                'Channel closed while processing %s: %s',
                ctx.message.delivery_tag,
                error,
            )
            ctx.measurement.set_tag('exception', error.__class__.__name__)
            return models.Result.MESSAGE_REQUEUE
        except pika_exceptions.ConnectionClosed as error:
            self.logger.critical(
                'Connection closed while processing %s: %s',
                ctx.message.delivery_tag,
                str(error),
            )
            ctx.measurement.set_tag('exception', error.__class__.__name__)
            return models.Result.MESSAGE_REQUEUE
        except exceptions.ConsumerException as error:
            self.logger.error(
                'ConsumerException processing delivery %s: %s',
                ctx.message.delivery_tag,
                str(error),
            )
            ctx.measurement.set_tag('exception', error.__class__.__name__)
            if error.metric:
                ctx.measurement.set_tag('error', error.metric)
            return models.Result.CONSUMER_EXCEPTION
        except exceptions.MessageException as error:
            self.logger.info(
                'MessageException processing delivery %s: %s',
                ctx.message.delivery_tag,
                str(error),
            )
            ctx.measurement.set_tag('exception', error.__class__.__name__)
            if error.metric:
                ctx.measurement.set_tag('error', error.metric)
            return models.Result.MESSAGE_EXCEPTION
        except exceptions.ProcessingException as error:
            self.logger.warning(
                'ProcessingException processing delivery %s: %s',
                ctx.message.delivery_tag,
                str(error),
            )
            ctx.measurement.set_tag('exception', error.__class__.__name__)
            if error.metric:
                ctx.measurement.set_tag('error', error.metric)
            self._republish_processing_error(
                ctx, error.metric or error.__class__.__name__
            )
            return models.Result.PROCESSING_EXCEPTION
        except NotImplementedError as error:
            self._log_exception(
                ctx,
                'NotImplementedError processing delivery %s: %s',
                ctx.message.delivery_tag,
                error,
            )
            ctx.measurement.set_tag('exception', 'UnhandledException')
            return models.Result.UNHANDLED_EXCEPTION
        except Exception as error:
            self._log_exception(
                ctx,
                'Exception processing delivery %s: %s',
                ctx.message.delivery_tag,
                str(error),
            )
            ctx.measurement.set_tag('exception', 'UnhandledException')
            return models.Result.UNHANDLED_EXCEPTION

        return models.Result.MESSAGE_ACK

    # --- Utilities ---

    @property
    def correlation_id(self) -> str | None:
        return self._correlation_id

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @property
    def settings(self) -> typing.Any:
        return self._settings

    def message_age_key(self) -> str:
        return self.MESSAGE_AGE_KEY

    async def publish_message(
        self,
        exchange: str,
        routing_key: str,
        properties: dict[str, typing.Any],
        body: typing.Any,
        no_serialization: bool = False,
        no_encoding: bool = False,
        channel_name: str | None = None,
    ) -> None:
        """Publish a message to RabbitMQ.

        Encoding and serialization are handled by the process's
        :class:`~rejected.codecs.Codec` instance.

        """
        codec = self._process.codec if self._process else None
        if codec:
            ct = (
                properties.get('content_type')
                if not no_serialization
                else None
            )
            ce = (
                properties.get('content_encoding') if not no_encoding else None
            )
            try:
                body = await codec.encode(body, ct, ce, properties.get('type'))
            except codecs.EncodeError as err:
                raise exceptions.ConsumerException(str(err)) from err

        self._publish_channel(channel_name).basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            properties=self._get_pika_properties(properties),
            body=body,
        )

    def send_exception_to_sentry(self, exc_info: typing.Any) -> None:
        self._process.send_exception_to_sentry(exc_info)

    def set_sentry_context(self, tag: str, value: str) -> None:
        if sentry_sdk and self._process and self._process.sentry_client:
            sentry_sdk.set_tag(tag, value)

    def unset_sentry_context(self, tag: str) -> None:
        if sentry_sdk and self._process and self._process.sentry_client:
            sentry_sdk.get_isolation_scope().remove_tag(tag)

    def require_setting(
        self, name: str, feature: str = 'this feature'
    ) -> None:
        if name not in self.settings:
            raise ValueError(
                f'You must define the "{name}" setting to use {feature}'
            )

    def set_channel(self, name: str, chan: channel.Channel) -> None:
        self._channels[name] = chan

    @staticmethod
    def _measurement_of(
        ctx: models.ProcessingContext | None,
    ) -> 'measurement_mod.Measurement | None':
        """Return the measurement from a context, or None."""
        return ctx.measurement if ctx else None

    def stats_add_duration(
        self,
        key: str,
        duration: float,
        ctx: models.ProcessingContext | None = None,
    ) -> None:
        m = self._measurement_of(ctx)
        if m:
            m.add_duration(key, duration)

    def stats_incr(
        self,
        key: str,
        value: int = 1,
        ctx: models.ProcessingContext | None = None,
    ) -> None:
        m = self._measurement_of(ctx)
        if m:
            m.incr(key, value)

    def stats_set_tag(
        self,
        key: str,
        value: str | bool | int = 1,
        ctx: models.ProcessingContext | None = None,
    ) -> None:
        m = self._measurement_of(ctx)
        if m:
            m.set_tag(key, value)

    def stats_set_value(
        self,
        key: str,
        value: int | float = 1,
        ctx: models.ProcessingContext | None = None,
    ) -> None:
        m = self._measurement_of(ctx)
        if m:
            m.set_value(key, value)

    @contextlib.contextmanager
    def stats_track_duration(
        self, key: str, ctx: models.ProcessingContext | None = None
    ) -> typing.Generator[None, None, None]:
        start = time.monotonic()
        try:
            yield
        finally:
            self.stats_add_duration(key, time.monotonic() - start, ctx)

    # Avro schema loading is now handled by the Codec class in codecs.py

    # --- Internal helpers ---

    def _log_exception(
        self, ctx: models.ProcessingContext, msg_format: str, *args: typing.Any
    ) -> None:
        self.logger.exception(msg_format, *args, exc_info=True)
        if self._process:
            self._process.send_exception_to_sentry(sys.exc_info(), ctx)

    @staticmethod
    def _get_pika_properties(
        properties_in: dict[str, typing.Any] | None,
    ) -> pika.BasicProperties:
        props = pika.BasicProperties()
        for key in properties_in or {}:
            value = (properties_in or {}).get(key)
            if value is not None:
                setattr(props, key, value)
        return props

    def _publish_channel(self, name: str | None = None) -> channel.Channel:
        """Return the channel to publish on.

        Subclasses should override to provide a default channel when
        ``name`` is None.

        """
        if not name:
            raise ValueError('channel name is required')
        try:
            return self._channels[name]
        except KeyError:
            raise ValueError(f'Channel {name} not found') from None

    def _republish(
        self,
        ctx: models.ProcessingContext,
        exchange: str,
        extra_headers: dict[str, typing.Any],
    ) -> None:
        """Republish the current message to the given exchange."""
        msg = ctx.message
        headers = dict(msg.headers) if msg.headers else {}
        headers['X-Original-Exchange'] = msg.exchange or ''
        headers['X-Original-Queue'] = self._process.queue_name
        headers.update(extra_headers)
        ctx.channel.basic_publish(
            exchange=exchange,
            routing_key=msg.routing_key or '',
            body=ctx.raw_body or msg.body,
            properties=pika.BasicProperties(headers=headers),
        )

    def _republish_dropped_message(
        self, ctx: models.ProcessingContext, reason: str
    ) -> None:
        self._republish(
            ctx,
            self._drop_exchange or '',
            {
                'X-Dropped-By': self.name,
                'X-Dropped-Reason': reason,
                'X-Dropped-Timestamp': datetime.datetime.now(
                    tz=datetime.UTC
                ).isoformat(),
            },
        )

    def _republish_processing_error(
        self, ctx: models.ProcessingContext, error: str
    ) -> None:
        headers: dict[str, typing.Any] = {}
        if error:
            headers['X-Processing-Exception'] = error
        msg_headers = ctx.message.headers or {}
        raw_prev = msg_headers.get(_PROCESSING_EXCEPTIONS, 0)
        prev = int(raw_prev) if isinstance(raw_prev, (int, float, str)) else 0
        headers[_PROCESSING_EXCEPTIONS] = prev + 1
        self._republish(ctx, self._error_exchange or '', headers)


class Consumer(_Consumer):
    """Backward-compatible consumer for rejected 3.x.

    Processes one message at a time (locked). Message properties are
    accessible via ``self.body``, ``self.content_type``, etc.
    Override ``prepare()``, ``process()``, ``finish()``.

    """

    def __init__(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        super().__init__(*args, **kwargs)
        self._context: models.ProcessingContext | None = None
        self._message_body: typing.Any = _UNSET
        self._lock: asyncio.Lock | None = None

    async def prepare(self) -> None:
        """Called before process. Override to add pre-processing."""
        pass

    async def process(self) -> None:
        """Implement your consumer logic here."""
        raise NotImplementedError

    async def finish(self) -> None:
        """Called after process. Override to add post-processing."""
        pass

    async def on_finish(self) -> None:
        """Called after processing completes."""
        self.logger.debug('on_finish invoked')

    async def _run_consumer(
        self, ctx: models.ProcessingContext
    ) -> models.Result:
        if self._lock is None:
            self._lock = asyncio.Lock()
        async with self._lock:
            self._context = ctx
            self._message_body = _UNSET
            try:
                return await self._handle_execution(
                    ctx, self._process_standard
                )
            finally:
                await self.on_finish()
                self._context = None
                self._message_body = _UNSET

    async def _process_standard(self) -> None:
        await self.prepare()
        await self.process()

    # --- Quick-access properties (read from self._context) ---

    @property
    def app_id(self) -> str | None:
        if not self._context:
            return None
        return self._context.message.app_id

    @property
    def body(self) -> typing.Any:
        if not self._context:
            return None
        if self._message_body is not _UNSET:
            return self._message_body
        msg = self._context.message
        self._message_body = msg.body
        return self._message_body

    @property
    def content_encoding(self) -> str | None:
        if not self._context:
            return None
        return self._context.message.content_encoding

    @property
    def content_type(self) -> str | None:
        if not self._context:
            return None
        return self._context.message.content_type

    @property
    def exchange(self) -> str | None:
        if not self._context:
            return None
        return self._context.message.exchange

    @property
    def expiration(self) -> str | None:
        if not self._context:
            return None
        return self._context.message.expiration

    @property
    def headers(self) -> dict[str, typing.Any] | None:
        if not self._context:
            return None
        return self._context.message.headers or {}

    @property
    def message_id(self) -> str | None:
        if not self._context:
            return None
        return self._context.message.message_id

    @property
    def message_type(self) -> str | None:
        if not self._context:
            return None
        return self._context.message.message_type

    @property
    def priority(self) -> int | None:
        if not self._context:
            return None
        return self._context.message.priority

    @property
    def properties(self) -> dict[str, typing.Any] | None:
        if not self._context:
            return None
        return self._context.message.model_dump()

    @property
    def redelivered(self) -> bool | None:
        if not self._context:
            return None
        return self._context.message.redelivered

    @property
    def reply_to(self) -> str | None:
        if not self._context:
            return None
        return self._context.message.reply_to

    @property
    def returned(self) -> bool | None:
        if not self._context:
            return None
        return self._context.message.returned

    @property
    def routing_key(self) -> str | None:
        if not self._context:
            return None
        return self._context.message.routing_key

    @property
    def timestamp(self) -> datetime.datetime | None:
        if not self._context:
            return None
        return self._context.message.timestamp

    @property
    def user_id(self) -> str | None:
        if not self._context:
            return None
        return self._context.message.user_id

    def _publish_channel(self, name: str | None = None) -> channel.Channel:
        if name:
            return super()._publish_channel(name)
        if self._context:
            return typing.cast(channel.Channel, self._context.channel)
        raise ValueError('No channel available for publishing')

    # --- Stats helpers that auto-use self._context ---

    def stats_add_duration(
        self,
        key: str,
        duration: float,
        ctx: models.ProcessingContext | None = None,
    ) -> None:
        super().stats_add_duration(key, duration, ctx or self._context)

    def stats_incr(
        self,
        key: str,
        value: int = 1,
        ctx: models.ProcessingContext | None = None,
    ) -> None:
        super().stats_incr(key, value, ctx or self._context)

    def stats_set_tag(
        self,
        key: str,
        value: str | bool | int = 1,
        ctx: models.ProcessingContext | None = None,
    ) -> None:
        super().stats_set_tag(key, value, ctx or self._context)

    def stats_set_value(
        self,
        key: str,
        value: int | float = 1,
        ctx: models.ProcessingContext | None = None,
    ) -> None:
        super().stats_set_value(key, value, ctx or self._context)

    @contextlib.contextmanager
    def stats_track_duration(
        self, key: str, ctx: models.ProcessingContext | None = None
    ) -> typing.Generator[None, None, None]:
        start = time.monotonic()
        try:
            yield
        finally:
            self.stats_add_duration(
                key, time.monotonic() - start, ctx or self._context
            )


class TransactionConsumer(_Consumer):
    """Concurrent consumer that receives a ProcessingContext.

    No lock — multiple messages may be processed concurrently.
    Override ``prepare(ctx)``, ``process(ctx)``, ``finish(ctx)``.

    """

    async def prepare(self, ctx: models.ProcessingContext) -> None:
        """Called before process. Override to add pre-processing."""
        pass

    async def process(self, ctx: models.ProcessingContext) -> None:
        """Implement your consumer logic here."""
        raise NotImplementedError

    async def finish(self, ctx: models.ProcessingContext) -> None:
        """Called after process. Override to add post-processing."""
        pass

    async def _run_consumer(
        self, ctx: models.ProcessingContext
    ) -> models.Result:
        return await self._handle_execution(
            ctx, lambda: self._process_transactional(ctx)
        )

    async def _process_transactional(
        self, ctx: models.ProcessingContext
    ) -> None:
        await self.prepare(ctx)
        await self.process(ctx)
        await self.finish(ctx)


# Re-export exception classes for backward compat
ConsumerException = exceptions.ConsumerException
MessageException = exceptions.MessageException
ProcessingException = exceptions.ProcessingException
RejectedException = exceptions.RejectedException
