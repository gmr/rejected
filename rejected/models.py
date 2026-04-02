"""Pydantic message model for the FunctionalConsumer."""

import datetime
import typing

import pydantic


class Message(pydantic.BaseModel):
    """A fully deserialized AMQP message.

    All properties are pre-populated and the body is already deserialized
    based on the message's ``content_type`` and ``content_encoding``.

    Passed to :meth:`~rejected.consumer.FunctionalConsumer.prepare`,
    :meth:`~rejected.consumer.FunctionalConsumer.process`, and
    :meth:`~rejected.consumer.FunctionalConsumer.finish`.

    """

    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)

    app_id: str | None = None
    body: typing.Any = None
    content_encoding: str | None = None
    content_type: str | None = None
    correlation_id: str | None = None
    exchange: str = ''
    expiration: str | None = None
    headers: dict[str, typing.Any] = pydantic.Field(default_factory=dict)
    message_id: str | None = None
    message_type: str | None = None
    priority: int | None = None
    redelivered: bool = False
    reply_to: str | None = None
    returned: bool = False
    routing_key: str = ''
    timestamp: datetime.datetime | int | None = None
    user_id: str | None = None
