import unittest

from rejected import utils


class PercentileTestCase(unittest.TestCase):

    def test_90th_percentile(self):
        values = [43, 54, 56, 61, 62, 66, 68, 69, 69, 70, 71, 72, 77, 78,
                  79, 85, 87, 88, 89, 93, 95, 96, 98, 99, 99]
        self.assertEqual(utils.percentile(values, 90), 98)

    def test_50th_percentile(self):
        values = [43, 54, 56, 61, 62, 66, 68, 69, 69, 70, 71, 72, 77, 78,
                  79, 85, 87, 88, 89, 93, 95, 96, 98, 99, 99]
        self.assertEqual(utils.percentile(values, 50), 77)
