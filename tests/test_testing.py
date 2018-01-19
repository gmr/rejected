# coding=utf-8
"""Tests for rejected.testing"""
import uuid

from rejected import data, testing

from . import common


class ConsumerLifecycleTests(testing.AsyncTestCase):
    def get_consumer(self):
        return common.TestConsumer

    def tearDown(self):
        super(ConsumerLifecycleTests, self).tearDown()
        self.assertTrue(self.consumer.called_shutdown)

    @testing.gen_test
    def test_contract_met(self):
        result = yield self.process_message({'id': str(uuid.uuid4())})
        self.assertIsInstance(result, data.Measurement)
        self.assertEqual(result, self.measurement)
        self.assertTrue(self.consumer.called_initialize)
        self.assertTrue(self.consumer.called_prepare)
        self.assertTrue(self.consumer.called_process)
        self.assertTrue(self.consumer.called_on_finish)
