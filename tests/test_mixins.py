import mock
import uuid

from rejected import mixins, testing

from . import common


class TestConsumer(mixins.GarbageCollectorMixin,
                   common.TestConsumer):
    pass


class GarbageCollectorMixinTestCase(testing.AsyncTestCase):

    def get_consumer(self):
        return TestConsumer

    def get_settings(self):
        return {
            'gc_collection_frequency': 5
        }

    @testing.gen_test
    def test_gc_collect_invoked(self):
        with mock.patch('gc.collect') as collect:
            for iteration in range(0, 10):
                yield self.process_message(
                    {'id': iteration, 'value': str(uuid.uuid4())})
            self.assertEqual(collect.call_count, 2)

    @testing.gen_test
    def test_collection_cycle_setter(self):
        self.consumer.collection_cycle = 2
        with mock.patch('gc.collect') as collect:
            for iteration in range(0, 10):
                yield self.process_message(
                    {'id': iteration, 'value': str(uuid.uuid4())})
            self.assertEqual(collect.call_count, 5)

