"""
Statsd Client that takes configuration first from the rejected configuration
file, falling back to environment variables, and finally default values.

Environment Variables:

 - STATSD_HOST
 - STATSD_PORT
 - STATSD_PREFIX

"""
import logging
import os
import socket

from tornado import iostream

LOGGER = logging.getLogger(__name__)


class Client(object):
    """A simple statsd client that buffers counters to emit fewer UDP packets
    than once per incr.

    """
    DEFAULT_HOST = 'localhost'
    DEFAULT_PORT = 8125
    DEFAULT_PREFIX = 'rejected'
    PAYLOAD_HOSTNAME = '{}.{}.{}.{}:{}|{}\n'
    PAYLOAD_NO_HOSTNAME = '{}.{}.{}:{}|{}\n'

    def __init__(self, consumer_name, settings):
        """

        :param str consumer_name: The name of the consumer for this client
        :param dict settings: statsd Settings

        """
        self._consumer_name = consumer_name
        self._hostname = socket.gethostname().split('.')[0]
        self._settings_in = settings
        self._settings = {}

        self._address = (self._setting('host', self.DEFAULT_HOST),
                         int(self._setting('port', self.DEFAULT_PORT)))
        self._prefix = self._setting('prefix', self.DEFAULT_PREFIX)
        self._tcp_sock, self._udp_sock = None, None
        if self._setting('tcp', False):
            self._tcp_sock = self._tcp_socket()
        else:
            self._udp_sock = self._udp_socket()

    def add_timing(self, key, value=0):
        """Add a timer value to statsd for the specified key

        :param str key: The key to add the timing to
        :param int or float value: The value of the timing in seconds

        """
        return self._send(key, value * 1000, 'ms')

    def incr(self, key, value=1):
        """Increment the counter value in statsd

        :param str key: The key to increment
        :param int value: The value to increment by, defaults to 1

        """
        return self._send(key, value, 'c')

    def set_gauge(self, key, value):
        """Set a gauge value in statsd

        :param str key: The key to set the value for
        :param int or float value: The value to set

        """
        return self._send(key, value, 'g')

    def stop(self):
        """Close the socket if connected via TCP."""
        if self._tcp_sock:
            self._tcp_sock.close()

    def _build_payload(self, key, value, metric_type):
        """Return the """
        if self._setting('include_hostname', True):
            return self.PAYLOAD_HOSTNAME.format(
                self._prefix, self._hostname, self._consumer_name, key, value,
                metric_type)
        return self.PAYLOAD_NO_HOSTNAME.format(
            self._prefix, self._consumer_name, key, value,
            metric_type)

    def _send(self, key, value, metric_type):
        """Send the specified value to the statsd daemon via UDP without a
        direct socket connection.

        :param str key: The key name to send
        :param int or float value: The value for the key

        """
        payload = self._build_payload(key, value, metric_type)
        LOGGER.debug('Sending statsd payload: %r', payload)
        try:
            if self._tcp_sock:
                return self._tcp_sock.write(payload.encode('utf-8'))
            else:
                self._udp_sock.sendto(payload.encode('utf-8'), self._address)
        except (OSError, socket.error) as error:  # pragma: nocover
            LOGGER.exception('Error sending statsd metric: %s', error)

    def _setting(self, key, default):
        """Return the setting, checking config, then the appropriate
        environment variable, falling back to the default, caching the
        results.

        :param str key: The key to get
        :param any default: The default value if not set
        :return: str

        """
        if key not in self._settings:
            value = self._settings_in.get(
                key, os.environ.get('STATSD_{}'.format(key).upper(), default))
            self._settings[key] = value
        return self._settings[key]

    def _tcp_on_closed(self):
        """Invoked when the socket is closed."""
        LOGGER.warning('Disconnected from statsd, reconnecting')
        self._tcp_sock = self._tcp_socket()

    def _tcp_on_connected(self):
        """Invoked when the IOStream is connected"""
        LOGGER.debug('Connected to statsd at %s via TCP', self._address)

    def _tcp_socket(self):
        """Connect to statsd via TCP and return the IOStream handle.

        :rtype: iostream.IOStream

        """
        sock = iostream.IOStream(socket.socket(
            socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP))
        try:
            sock.connect(self._address, self._tcp_on_connected)
        except (OSError, socket.error) as error:
            LOGGER.error('Failed to connect via TCP: %s', error)
        sock.set_close_callback(self._tcp_on_closed)
        return sock

    @staticmethod
    def _udp_socket():
        """Return the UDP socket handle

        :rtype: socket.socket

        """
        return socket.socket(socket.AF_INET, socket.SOCK_DGRAM,
                             socket.IPPROTO_UDP)
