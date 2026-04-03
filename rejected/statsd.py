"""
Statsd Client that takes configuration first from the rejected
configuration file, falling back to environment variables, and finally
default values.

Environment Variables:

 - STATSD_HOST
 - STATSD_PORT
 - STATSD_PREFIX

"""

import logging
import os
import socket
import typing

LOGGER = logging.getLogger(__name__)


class Client:
    """A simple statsd client that buffers counters to emit fewer UDP
    packets than once per incr.

    """

    DEFAULT_HOST: typing.ClassVar[str] = 'localhost'
    DEFAULT_PORT: typing.ClassVar[int] = 8125
    DEFAULT_PREFIX: typing.ClassVar[str] = 'rejected'
    PAYLOAD_HOSTNAME: typing.ClassVar[str] = '{}.{}.{}.{}:{}|{}\n'
    PAYLOAD_NO_HOSTNAME: typing.ClassVar[str] = '{}.{}.{}:{}|{}\n'

    def __init__(
        self,
        consumer_name: str,
        settings: dict[str, typing.Any],
        failure_callback: typing.Callable[[], None],
    ) -> None:
        self._connected: bool = False
        self._consumer_name: str = consumer_name
        self._failure_callback = failure_callback
        self._hostname: str = socket.gethostname().split('.')[0]
        self._settings_in = settings
        self._settings: dict[str, typing.Any] = {}

        self._address: tuple[str, int] = (
            str(self._setting('host', self.DEFAULT_HOST)),
            int(self._setting('port', self.DEFAULT_PORT)),
        )
        self._prefix: str = str(self._setting('prefix', self.DEFAULT_PREFIX))
        self._tcp_writer: socket.socket | None = None
        self._udp_sock: socket.socket | None = None
        if self._setting('tcp', False):
            self._tcp_writer = self._tcp_socket()
        else:
            self._udp_sock = self._udp_socket()

    def add_timing(self, key: str, value: float = 0) -> None:
        """Add a timer value to statsd for the specified key.

        :param key: The key to add the timing to
        :param value: The value of the timing in seconds

        """
        self._send(key, value * 1000, 'ms')

    def incr(self, key: str, value: int = 1) -> None:
        """Increment the counter value in statsd.

        :param key: The key to increment
        :param value: The value to increment by

        """
        self._send(key, value, 'c')

    def set_gauge(self, key: str, value: int | float) -> None:
        """Set a gauge value in statsd.

        :param key: The key to set the value for
        :param value: The value to set

        """
        self._send(key, value, 'g')

    def stop(self) -> None:
        """Close the socket if connected via TCP."""
        if self._tcp_writer:
            try:
                self._tcp_writer.close()
            except OSError:
                pass
            self._tcp_writer = None

    def _build_payload(
        self, key: str, value: int | float, metric_type: str
    ) -> str:
        """Build the statsd payload string."""
        if self._setting('include_hostname', True):
            return self.PAYLOAD_HOSTNAME.format(
                self._prefix,
                self._hostname,
                self._consumer_name,
                key,
                value,
                metric_type,
            )
        return self.PAYLOAD_NO_HOSTNAME.format(
            self._prefix, self._consumer_name, key, value, metric_type
        )

    def _send(self, key: str, value: int | float, metric_type: str) -> None:
        """Send the specified value to the statsd daemon."""
        payload = self._build_payload(key, value, metric_type)
        LOGGER.debug('Sending statsd payload: %r', payload)
        try:
            if self._tcp_writer:
                self._tcp_writer.send(payload.encode('utf-8'))
            elif self._udp_sock:
                self._udp_sock.sendto(payload.encode('utf-8'), self._address)
        except OSError as error:  # pragma: nocover
            if self._connected:
                LOGGER.exception('Error sending statsd metric: %s', error)
                self._connected = False
                self._failure_callback()

    def _setting(self, key: str, default: typing.Any) -> typing.Any:
        """Return the setting, checking config, then the appropriate
        environment variable, falling back to the default, caching the
        results.

        """
        if key not in self._settings:
            value = self._settings_in.get(
                key, os.environ.get(f'STATSD_{key}'.upper(), default)
            )
            self._settings[key] = value
        return self._settings[key]

    def _tcp_on_closed(self) -> None:
        """Invoked when the socket is closed."""
        LOGGER.warning('Disconnected from statsd, reconnecting')
        self._connected = False
        self._tcp_writer = self._tcp_socket()

    def _tcp_socket(self) -> socket.socket | None:
        """Connect to statsd via TCP and return the socket handle."""
        sock = socket.socket(
            socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP
        )
        try:
            sock.connect(self._address)
        except OSError as error:
            LOGGER.error(
                'Failed to connect via TCP, triggering shutdown: %s', error
            )
            self._failure_callback()
            return None
        sock.setblocking(False)
        LOGGER.debug('Connected to statsd at %s via TCP', self._address)
        self._connected = True
        return sock

    @staticmethod
    def _udp_socket() -> socket.socket:
        """Return the UDP socket handle."""
        return socket.socket(
            socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP
        )
