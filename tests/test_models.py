import datetime
import decimal
import typing
import unittest

import pydantic

from rejected import models


def _make_message(headers: typing.Any) -> models.Message:
    return models.Message(
        delivery_tag=1,
        exchange='ex',
        routing_key='k',
        body=b'',
        app_id=None,
        content_encoding=None,
        content_type=None,
        correlation_id=None,
        delivery_mode=None,
        expiration=None,
        headers=headers,
        message_id=None,
        type=None,
        priority=None,
        redelivered=False,
        reply_to=None,
        timestamp=None,
        user_id=None,
    )


class MessageHeadersTestCase(unittest.TestCase):
    def test_scalar_values(self) -> None:
        headers = {
            'bool': True,
            'int': 1,
            'float': 1.5,
            'str': 'value',
            'bytes': b'value',
            'decimal': decimal.Decimal('1.5'),
            'timestamp': datetime.datetime.now(tz=datetime.UTC),
            'void': None,
        }
        self.assertEqual(_make_message(headers).headers, headers)

    def test_nested_table(self) -> None:
        headers = {'table': {'nested': {'key': 'value'}, 'count': 1}}
        self.assertEqual(_make_message(headers).headers, headers)

    def test_array_of_scalars(self) -> None:
        headers = {'array': ['a', 1, True, None]}
        self.assertEqual(_make_message(headers).headers, headers)

    def test_x_death_array_of_tables(self) -> None:
        """RabbitMQ sets ``x-death`` on every dead-lettered message as an
        array of tables, which pika decodes to a list of dicts."""
        headers = {
            'x-death': [
                {
                    'count': 1,
                    'reason': 'rejected',
                    'queue': 'q',
                    'time': datetime.datetime.now(tz=datetime.UTC),
                    'exchange': 'ex',
                    'routing-keys': ['k'],
                }
            ],
            'x-first-death-exchange': 'ex',
            'x-first-death-queue': 'q',
            'x-first-death-reason': 'rejected',
        }
        self.assertEqual(_make_message(headers).headers, headers)

    def test_empty_headers(self) -> None:
        self.assertEqual(_make_message({}).headers, {})

    def test_non_dict_headers_rejected(self) -> None:
        for value in (['x-death'], 'value', 1, None):
            with self.subTest(value=value):
                with self.assertRaises(pydantic.ValidationError):
                    _make_message(value)
