import unittest
from unittest import mock

try:
    import prometheus_client
except ImportError:
    prometheus_client = None

from rejected import models, prometheus


@unittest.skipUnless(prometheus_client, 'prometheus_client is not installed')
class _RequiresPrometheusClient(unittest.TestCase):
    pass


class PrometheusConfigTestCase(unittest.TestCase):
    def test_address_defaults_to_localhost(self):
        self.assertEqual(models.PrometheusConfig().address, '127.0.0.1')

    def test_address_configurable(self):
        config = models.PrometheusConfig(address='0.0.0.0')
        self.assertEqual(config.address, '0.0.0.0')


class PrometheusTestCase(_RequiresPrometheusClient):
    def setUp(self):
        self._saved_metrics = prometheus._metrics
        self._saved_started = prometheus._started
        prometheus._metrics = {}
        prometheus._started = True

    def tearDown(self):
        for collector in prometheus._metrics.values():
            try:
                prometheus_client.REGISTRY.unregister(collector)
            except KeyError:
                pass
        prometheus._metrics = self._saved_metrics
        prometheus._started = self._saved_started


class CustomMetricCacheTestCase(PrometheusTestCase):
    def test_keys_sanitizing_to_same_name_share_metric(self):
        first = prometheus._get_custom_counter('db.query')
        second = prometheus._get_custom_counter('db-query')
        # Both sanitize to db_query; must be the exact same object so the
        # second call does not re-register and raise Duplicated timeseries.
        self.assertIs(first, second)

    def test_histogram_cache_by_safe_name(self):
        first = prometheus._get_custom_histogram('a.b')
        second = prometheus._get_custom_histogram('a-b')
        self.assertIs(first, second)

    def test_gauge_cache_by_safe_name(self):
        first = prometheus._get_custom_gauge('x.y')
        second = prometheus._get_custom_gauge('x-y')
        self.assertIs(first, second)


class ObserveTestCase(PrometheusTestCase):
    def setUp(self):
        super().setUp()
        self._created = [
            prometheus_client.Histogram(
                'test_duration_seconds', 'help', ['consumer']
            ),
            prometheus_client.Histogram(
                'test_message_age_seconds', 'help', ['consumer']
            ),
        ]
        prometheus._metrics['duration'] = self._created[0]
        prometheus._metrics['message_age'] = self._created[1]

    def tearDown(self):
        for collector in self._created:
            try:
                prometheus_client.REGISTRY.unregister(collector)
            except KeyError:
                pass
        super().tearDown()

    def test_observe_records_without_error(self):
        prometheus.observe(
            'c',
            durations=[0.1],
            message_ages=[1.0],
            custom_counters={'db.query': 1, 'db-query': 2},
        )
        counter = prometheus._get_custom_counter('db.query')
        value = counter.labels(consumer='c')._value.get()
        self.assertEqual(value, 3)

    def test_observe_swallows_errors(self):
        prometheus._metrics['duration'] = mock.Mock()
        prometheus._metrics['duration'].labels.side_effect = ValueError(
            'Duplicated timeseries'
        )
        # Must not propagate into the caller (MCP stats collection).
        prometheus.observe('c', durations=[0.1], message_ages=[])

    def test_observe_noop_when_not_started(self):
        prometheus._started = False
        prometheus._metrics['duration'] = mock.Mock()
        prometheus.observe('c', durations=[0.1], message_ages=[])
        prometheus._metrics['duration'].labels.assert_not_called()


class StartAddressTestCase(PrometheusTestCase):
    def setUp(self):
        super().setUp()
        prometheus._started = False

    def test_default_binds_localhost(self):
        with mock.patch.object(
            prometheus_client, 'start_http_server'
        ) as start:
            prometheus.start(9123)
        start.assert_called_once_with(9123, addr='127.0.0.1')

    def test_explicit_address(self):
        with mock.patch.object(
            prometheus_client, 'start_http_server'
        ) as start:
            prometheus.start(9123, address='0.0.0.0')
        start.assert_called_once_with(9123, addr='0.0.0.0')
