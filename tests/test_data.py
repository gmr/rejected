"""Tests for rejected.measurement"""

import unittest

from rejected import measurement


class TestMeasurement(unittest.TestCase):
    def setUp(self):
        self.m = measurement.Measurement()

    def test_incr(self):
        self.m.incr('foo')
        self.assertEqual(self.m.counters['foo'], 1)

    def test_incr_by(self):
        self.m.incr('foo', 5)
        self.assertEqual(self.m.counters['foo'], 5)

    def test_decr(self):
        self.m.incr('foo', 10)
        self.m.decr('foo', 3)
        self.assertEqual(self.m.counters['foo'], 7)

    def test_add_duration(self):
        self.m.add_duration('bar', 1.5)
        self.assertEqual(self.m.durations['bar'], [1.5])

    def test_add_duration_appends(self):
        self.m.add_duration('bar', 1.0)
        self.m.add_duration('bar', 2.0)
        self.assertEqual(self.m.durations['bar'], [1.0, 2.0])

    def test_set_tag(self):
        self.m.set_tag('key', 'value')
        self.assertEqual(self.m.tags['key'], 'value')

    def test_set_tag_bool(self):
        self.m.set_tag('flag', True)
        self.assertTrue(self.m.tags['flag'])

    def test_set_value(self):
        self.m.set_value('gauge', 42.5)
        self.assertEqual(self.m.values['gauge'], 42.5)

    def test_track_duration(self):
        with self.m.track_duration('timed'):
            pass
        self.assertEqual(len(self.m.durations['timed']), 1)
        self.assertGreaterEqual(self.m.durations['timed'][0], 0)
