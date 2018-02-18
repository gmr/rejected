"""
Common Classes and Such

"""
from rejected import consumer


class TestConsumer(consumer.Consumer):

    def __init__(self, *args, **kwargs):
        self.called_initialize = False
        self.called_prepare = False
        self.called_process = False
        self.called_on_finish = False
        self.called_shutdown = False
        self.exc = None
        super(TestConsumer, self).__init__(*args, **kwargs)
        self.logger.debug('In __init__')

    def initialize(self):
        self.logger.debug('In initialize')
        self.called_initialize = True

    def prepare(self):
        self.logger.debug('In prepare')
        self.called_prepare = True
        return super(TestConsumer, self).prepare()

    def process(self):
        self.logger.debug('In process')
        self.called_process = True

    def on_finish(self, exc=None):
        self.logger.debug('In on_finish(exc=%r)', exc)
        self.called_on_finish = True
        self.exc = exc

    def shutdown(self):
        self.logger.debug('In shutdown')
        self.called_shutdown = True
