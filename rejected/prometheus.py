"""Prometheus metrics exporter for rejected.

Exposes per-consumer metrics via an HTTP endpoint that Prometheus scrapes.
Requires ``rejected[prometheus]`` to be installed.

"""

import logging
import re

try:
    import prometheus_client
except ImportError:
    prometheus_client = None

LOGGER = logging.getLogger(__name__)

_metrics: dict = {}
_previous: dict[str, dict[str, float]] = {}
_started = False

# Counter keys from process.Process that map to Prometheus Counters.
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

_DURATION_BUCKETS = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.075,
    0.1,
    0.25,
    0.5,
    0.75,
    1.0,
    2.5,
    5.0,
    7.5,
    10.0,
    float('inf'),
)

_AGE_BUCKETS = (
    0.1,
    0.5,
    1.0,
    5.0,
    10.0,
    30.0,
    60.0,
    300.0,
    600.0,
    1800.0,
    3600.0,
    float('inf'),
)

_SAFE_NAME_RE = re.compile(r'[^a-zA-Z0-9_]')


def _safe_name(key: str) -> str:
    """Convert an arbitrary key into a valid Prometheus metric name."""
    return _SAFE_NAME_RE.sub('_', key)


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

    prometheus_client.start_http_server(port)
    _started = True
    LOGGER.info('Prometheus metrics server started on port %d', port)

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

    _metrics['duration'] = prometheus_client.Histogram(
        'rejected_processing_duration_seconds',
        'Per-message processing duration in seconds',
        ['consumer'],
        buckets=_DURATION_BUCKETS,
    )

    _metrics['message_age'] = prometheus_client.Histogram(
        'rejected_message_age_seconds',
        'Age of messages at time of processing in seconds',
        ['consumer'],
        buckets=_AGE_BUCKETS,
    )


def _get_custom_histogram(key: str) -> 'prometheus_client.Histogram':
    """Get or create a custom duration Histogram for ad-hoc consumer stats."""
    metric_key = f'custom_duration_{key}'
    if metric_key not in _metrics:
        safe = _safe_name(key)
        _metrics[metric_key] = prometheus_client.Histogram(
            f'rejected_custom_{safe}_seconds',
            f'Custom duration: {key}',
            ['consumer'],
            buckets=_DURATION_BUCKETS,
        )
    return _metrics[metric_key]


def _get_custom_counter(key: str) -> 'prometheus_client.Counter':
    """Get or create a custom Counter for ad-hoc consumer stats."""
    metric_key = f'custom_counter_{key}'
    if metric_key not in _metrics:
        safe = _safe_name(key)
        _metrics[metric_key] = prometheus_client.Counter(
            f'rejected_custom_{safe}_total',
            f'Custom counter: {key}',
            ['consumer'],
        )
    return _metrics[metric_key]


def _get_custom_gauge(key: str) -> 'prometheus_client.Gauge':
    """Get or create a custom Gauge for ad-hoc consumer stats."""
    metric_key = f'custom_gauge_{key}'
    if metric_key not in _metrics:
        safe = _safe_name(key)
        _metrics[metric_key] = prometheus_client.Gauge(
            f'rejected_custom_{safe}', f'Custom gauge: {key}', ['consumer']
        )
    return _metrics[metric_key]


def observe(
    consumer_name: str,
    durations: list[float],
    message_ages: list[float],
    custom_durations: dict[str, list[float]] | None = None,
    custom_counters: dict[str, int] | None = None,
    custom_gauges: dict[str, float] | None = None,
) -> None:
    """Record per-message observations for Histograms and custom metrics.

    :param str consumer_name: The consumer name
    :param list durations: Processing duration observations (seconds)
    :param list message_ages: Message age observations (seconds)
    :param dict custom_durations: Ad-hoc duration observations from
        Consumer.stats_add_duration / stats_track_duration
    :param dict custom_counters: Ad-hoc counter increments from
        Consumer.stats_incr
    :param dict custom_gauges: Ad-hoc gauge values from
        Consumer.stats_set_value

    """
    if not _started:
        return

    duration_hist = _metrics['duration'].labels(consumer=consumer_name)
    for value in durations:
        duration_hist.observe(value)

    age_hist = _metrics['message_age'].labels(consumer=consumer_name)
    for value in message_ages:
        age_hist.observe(value)

    for key, values in (custom_durations or {}).items():
        hist = _get_custom_histogram(key).labels(consumer=consumer_name)
        for value in values:
            hist.observe(value)

    for key, value in (custom_counters or {}).items():
        if value > 0:
            _get_custom_counter(key).labels(consumer=consumer_name).inc(value)

    for key, value in (custom_gauges or {}).items():
        _get_custom_gauge(key).labels(consumer=consumer_name).set(value)


def update(stats: dict) -> None:
    """Update Prometheus counters and gauges from the MCP stats dict.

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
