"""Prometheus metrics exporter for rejected.

Exposes per-consumer metrics via an HTTP endpoint that Prometheus scrapes.
Requires ``rejected[prometheus]`` to be installed.

"""

import logging

try:
    import prometheus_client
except ImportError:
    prometheus_client = None

LOGGER = logging.getLogger(__name__)

_messages_processed = None
_messages_failed = None
_messages_redelivered = None
_consumer_processes = None
_previous: dict[str, dict[str, int]] = {}
_started = False


def start(port: int) -> None:
    """Start the Prometheus HTTP metrics server.

    :param int port: The port to listen on

    """
    global _messages_processed, _messages_failed
    global _messages_redelivered, _consumer_processes, _started

    if not prometheus_client:
        LOGGER.error(
            'prometheus_client is not installed; install rejected[prometheus]'
        )
        return

    if _started:
        LOGGER.warning('Prometheus exporter already running')
        return

    _messages_processed = prometheus_client.Counter(
        'rejected_messages_processed_total',
        'Total messages processed',
        ['consumer'],
    )
    _messages_failed = prometheus_client.Counter(
        'rejected_messages_failed_total',
        'Total messages that resulted in errors',
        ['consumer'],
    )
    _messages_redelivered = prometheus_client.Counter(
        'rejected_messages_redelivered_total',
        'Total redelivered messages',
        ['consumer'],
    )
    _consumer_processes = prometheus_client.Gauge(
        'rejected_consumer_processes',
        'Number of active consumer processes',
        ['consumer'],
    )

    prometheus_client.start_http_server(port)
    _started = True
    LOGGER.info('Prometheus metrics server started on port %d', port)


def update(stats: dict) -> None:
    """Update Prometheus metrics from the MCP stats dict.

    Computes deltas from the previous poll cycle to increment Counters.

    :param dict stats: The stats dict from MCP.calculate_stats()

    """
    if not _started:
        return

    consumers = stats.get('consumers', {})
    for name, data in consumers.items():
        prev = _previous.get(name, {})
        for key, counter in (
            ('processed', _messages_processed),
            ('failed', _messages_failed),
            ('redelivered', _messages_redelivered),
        ):
            current = data.get(key, 0)
            delta = current - prev.get(key, 0)
            if delta > 0:
                counter.labels(consumer=name).inc(delta)
        _consumer_processes.labels(consumer=name).set(data.get('processes', 0))
        _previous[name] = {
            'processed': data.get('processed', 0),
            'failed': data.get('failed', 0),
            'redelivered': data.get('redelivered', 0),
        }
