"""Tests for rejected.connection"""

import ssl
import typing
import unittest
from unittest import mock

from rejected import config as config_module
from rejected import connection as connection_mod
from rejected import models

_CONFIG_RAW: dict[str, typing.Any] = {
    'Connections': {
        'MockConnection': {
            'host': 'localhost',
            'port': 5672,
            'user': 'guest',
            'pass': 'guest',
            'vhost': '/',
        },
        'MockSSLConnection': {
            'host': 'remotehost',
            'port': 5672,
            'user': 'guest',
            'pass': 'guest',
            'vhost': '/',
            'ssl_options': {'protocol': ssl.PROTOCOL_TLS},
        },
    }
}


def _config() -> config_module.Config:
    return config_module.Config.model_validate(_CONFIG_RAW)


def _callbacks() -> models.Callbacks:
    return models.Callbacks(
        on_ready=mock.Mock(),
        on_connection_failure=mock.Mock(),
        on_closed=mock.Mock(),
        on_blocked=mock.Mock(),
        on_unblocked=mock.Mock(),
        on_confirmation=mock.Mock(),
        on_delivery=mock.Mock(),
        on_return=mock.Mock(),
    )


def _make_connection(
    name: str = 'MockConnection',
) -> connection_mod.Connection:
    cfg = _config().connections[name]
    with mock.patch.object(
        connection_mod.asyncio_connection, 'AsyncioConnection'
    ):
        return connection_mod.Connection(
            name, cfg, 'consumer', True, False, _callbacks()
        )


class ShutdownTests(unittest.TestCase):
    def test_shutdown_is_noop_when_closed(self) -> None:
        """#74 shutting down an already-closed connection is a no-op."""
        conn = _make_connection()
        conn.set_state(conn.STATE_CLOSED)
        conn.connection = mock.Mock()
        conn.channel = None
        conn.shutdown()
        conn.connection.close.assert_not_called()
        self.assertTrue(conn.is_closed)

    def test_shutdown_guards_connection_close(self) -> None:
        """#74 a raising connection.close() is swallowed."""
        conn = _make_connection()
        conn.set_state(conn.STATE_CONNECTING)
        conn.channel = None
        conn.connection = mock.Mock()
        conn.connection.close.side_effect = (
            connection_mod.pika.exceptions.ConnectionWrongStateError()
        )
        conn.shutdown()  # must not raise

    def test_on_failure_keeps_connection_attribute(self) -> None:
        """#74 on_failure no longer deletes self.connection."""
        conn = _make_connection()
        conn.connection = mock.Mock()
        conn.on_failure()
        self.assertTrue(hasattr(conn, 'connection'))


class SSLOptionsTests(unittest.TestCase):
    def test_non_default_protocol_enables_hostname_check(self) -> None:
        """#78 the legacy SSL path verifies the hostname."""
        conn = _make_connection('MockSSLConnection')
        opts = conn._ssl_options
        assert opts is not None
        self.assertTrue(opts.context.check_hostname)
        self.assertEqual(opts.context.verify_mode, ssl.CERT_REQUIRED)
