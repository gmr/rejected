"""
Common Classes and Such

"""
from rejected import consumer
from tornado import gen


class TestConsumer(consumer.Consumer):

    def __init__(self, *args, **kwargs):
        self.called_initialize = False
        self.called_prepare = False
        self.called_process = False
        self.called_on_finish = False
        self.called_shutdown = False
        super(TestConsumer, self).__init__(*args, **kwargs)

    def initialize(self):
        self.called_initialize = True

    @gen.coroutine
    def prepare(self):
        self.called_prepare = True
        return super(TestConsumer, self).prepare()

    def process(self):
        self.called_process = True

    def on_finish(self):
        self.called_on_finish = True

    def shutdown(self):
        self.called_shutdown = True
