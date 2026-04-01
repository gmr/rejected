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

_metrics: dict = {}
_previous: dict[str, dict[str, float]] = {}
_started = False

# Counter keys from process.Process that map to Prometheus Counters.
# Each entry is (process counter key, prometheus metric name, help text).
_COUNTER_DEFS = [
    ('acked', 'rejected_messages_acked_total', 'Total messages acknowledged'),
    ('dropped', 'rejected_messages_dropped_total', 'Total messages dropped'),
    (
        'failed',
        'rejected_messages_failed_total',
        'Total messages that resulted in errors',
    ),
    (
        'nacked',
        'rejected_messages_nacked_total',
        'Total messages negatively acknowledged',
    ),
    (
        'processed',
        'rejected_messages_processed_total',
        'Total messages processed',
    ),
    (
        'redelivered',
        'rejected_messages_redelivered_total',
        'Total redelivered messages',
    ),
    (
        'requeued',
        'rejected_messages_requeued_total',
        'Total messages requeued',
    ),
    (
        'processing_time',
        'rejected_processing_seconds_total',
        'Total time spent processing messages in seconds',
    ),
]

_EXCEPTION_TYPES = [
    'consumer_exception',
    'message_exception',
    'processing_exception',
    'unhandled_exception',
]


def start(port: int) -> None:
    """Start the Prometheus HTTP metrics server.

    :param int port: The port to listen on

    """
    global _started

    if not prometheus_client:
        LOGGER.error(
            'prometheus_client is not installed; install rejected[prometheus]'
        )
        return

    if _started:
        LOGGER.warning('Prometheus exporter already running')
        return

    for key, name, help_text in _COUNTER_DEFS:
        _metrics[key] = prometheus_client.Counter(
            name, help_text, ['consumer']
        )

    _metrics['exceptions'] = prometheus_client.Counter(
        'rejected_exceptions_total',
        'Total consumer exceptions',
        ['consumer', 'type'],
    )

    _metrics['processes'] = prometheus_client.Gauge(
        'rejected_consumer_processes',
        'Number of active consumer processes',
        ['consumer'],
    )

    prometheus_client.start_http_server(port)
    _started = True
    LOGGER.info('Prometheus metrics server started on port %d', port)


def update(stats: dict) -> None:
    """Update Prometheus metrics from the MCP stats dict.

    Computes deltas from the previous poll to increment Counters.

    :param dict stats: The stats dict from MCP.calculate_stats()

    """
    if not _started:
        return

    consumers = stats.get('consumers', {})
    for name, data in consumers.items():
        prev = _previous.get(name, {})

        for key, _, _ in _COUNTER_DEFS:
            current = data.get(key, 0)
            delta = current - prev.get(key, 0)
            if delta > 0:
                _metrics[key].labels(consumer=name).inc(delta)

        for exc_type in _EXCEPTION_TYPES:
            current = data.get(exc_type, 0)
            delta = current - prev.get(exc_type, 0)
            if delta > 0:
                _metrics['exceptions'].labels(
                    consumer=name, type=exc_type
                ).inc(delta)

        _metrics['processes'].labels(consumer=name).set(
            data.get('processes', 0)
        )

        _previous[name] = dict(data)
