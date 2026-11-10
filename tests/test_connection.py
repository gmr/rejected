"""Tests for rejected.connection"""

import asyncio
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
            name,
            cfg,
            'consumer',
            True,
            False,
            _callbacks(),
            ioloop=mock.Mock(spec=asyncio.AbstractEventLoop),
        )


class EventLoopBindingTests(unittest.TestCase):
    """Process creates its connections before calling ``run_forever()``.
    pika 1.4 picks the loop with ``asyncio.get_running_loop()`` and falls
    back to a brand new loop when none is running, so unless the loop is
    passed explicitly the connect workflow is scheduled on a loop that
    never runs and the consumer hangs in Connecting forever.

    """

    def setUp(self) -> None:
        super().setUp()
        self.loop = asyncio.new_event_loop()
        self.addCleanup(self.loop.close)

    def test_connection_workflow_runs_on_the_process_loop(self) -> None:
        cfg = _config().connections['MockConnection']
        conn = connection_mod.Connection(
            'MockConnection',
            cfg,
            'consumer',
            True,
            False,
            _callbacks(),
            ioloop=self.loop,
        )
        self.addCleanup(conn.connection.close)
        self.assertIs(conn.ioloop, self.loop)
        self.assertIs(conn.connection._nbio.get_native_ioloop(), self.loop)
        # The connect workflow must already be scheduled on our loop.
        self.assertTrue(self.loop._ready or self.loop._scheduled)

    def test_reconnect_runs_on_the_process_loop(self) -> None:
        cfg = _config().connections['MockConnection']
        conn = connection_mod.Connection(
            'MockConnection',
            cfg,
            'consumer',
            True,
            False,
            _callbacks(),
            ioloop=self.loop,
        )
        self.addCleanup(conn.connection.close)
        conn.set_state(conn.STATE_CLOSED)
        reconnected = conn.connect()
        self.addCleanup(reconnected.close)
        self.assertIs(reconnected._nbio.get_native_ioloop(), self.loop)


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
