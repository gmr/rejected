# API Reference

## Consumer

The primary base classes for building message consumers.

### Consumer

::: rejected.consumer.Consumer
    options:
      members:
        - process
        - prepare
        - finish
        - on_finish
        - initialize
        - shutdown
        - on_blocked
        - on_unblocked
        - on_confirmation
        - publish_message
        - set_sentry_context
        - unset_sentry_context
        - send_exception_to_sentry
        - require_setting
        - stats_add_duration
        - stats_incr
        - stats_set_tag
        - stats_set_value
        - stats_track_duration
        - body
        - app_id
        - content_encoding
        - content_type
        - correlation_id
        - exchange
        - expiration
        - headers
        - message_id
        - message_type
        - name
        - priority
        - properties
        - redelivered
        - reply_to
        - returned
        - routing_key
        - settings
        - timestamp
        - user_id

### TransactionConsumer

::: rejected.consumer.TransactionConsumer
    options:
      members:
        - process
        - prepare
        - finish
        - initialize
        - shutdown

## Exceptions

::: rejected.exceptions.ConsumerException

::: rejected.exceptions.MessageException

::: rejected.exceptions.ProcessingException

::: rejected.exceptions.RejectedException

## Models

### Message

::: rejected.models.Message

### ProcessingContext

::: rejected.models.ProcessingContext

### Result

::: rejected.models.Result

## Testing

::: rejected.testing.AsyncTestCase
    options:
      members:
        - get_consumer
        - get_settings
        - create_context
        - process_message
        - published_messages
        - measurement

::: rejected.testing.PublishedMessage

## Measurement

::: rejected.measurement.Measurement

## Mixins

::: rejected.mixins.GarbageCollectorMixin
