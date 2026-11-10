"""Tests for rejected.models"""

import datetime
import decimal
import unittest

import pydantic

from rejected import models


def _message(headers):
    return models.Message(
        delivery_tag=1,
        exchange='ex',
        routing_key='rk',
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


class FieldTableTests(unittest.TestCase):
    def test_none_headers(self):
        self.assertIsNone(_message(None).headers)

    def test_empty_headers(self):
        self.assertEqual(_message({}).headers, {})

    def test_scalar_values_keep_type(self):
        """Each scalar header value keeps its exact type after validation."""
        now = datetime.datetime.now(datetime.UTC)
        headers = {
            'bool': True,
            'int': 42,
            'float': 1.5,
            'decimal': decimal.Decimal('3.14'),
            'str': 'value',
            'bytes': b'\x00\x01',
            'datetime': now,
            'none': None,
        }
        result = _message(headers).headers
        self.assertEqual(result, headers)
        for key, value in headers.items():
            self.assertIs(type(result[key]), type(value), key)

    def test_nested_table_and_array(self):
        headers = {
            'x-death': [
                {
                    'count': 1,
                    'reason': 'rejected',
                    'routing-keys': ['rk'],
                    'nested': {'deep': [True, None, b'x']},
                }
            ]
        }
        self.assertEqual(_message(headers).headers, headers)

    def test_invalid_value_rejected(self):
        with self.assertRaises(pydantic.ValidationError):
            _message({'bad': object()})

    def test_invalid_nested_value_rejected(self):
        with self.assertRaises(pydantic.ValidationError):
            _message({'outer': {'inner': [object()]}})

    def test_non_str_key_rejected(self):
        with self.assertRaises(pydantic.ValidationError):
            _message({1: 'value'})
