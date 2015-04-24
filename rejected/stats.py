"""
Stats class that wraps the collections.Counter object and transparently
passes calls to increment and add_timing if statsd is enabled.

"""
try:
    import backport_collections as collections
except ImportError:
    import collections
from rejected import statsd


class Stats(object):

    def __init__(self, name, consumer_name, queue, statsd_cfg):
        self.name = name
        self.consumer_name = name
        self.statsd = None
        if statsd_cfg.get('enabled', False):
            self.statsd = statsd.StatsdClient(consumer_name, statsd_cfg)
        self.counter = collections.Counter()
        self.previous = collections.Counter()
        self.reporting_queue = queue

    def __getitem__(self, item):
        return self.counter.get(item)

    def __setitem__(self, item, value):
        self.counter[item] = value

    def add_timing(self, item, duration):
        if self.statsd:
            self.statsd.add_timing(item, duration)

    def get(self, item):
        return self.counter.get(item)

    def diff(self, item):
        return self.counter[item] - self.previous[item]

    def incr(self, key, value=1):
        self.counter[key] += value
        if self.statsd:
            self.statsd.incr(key, value)

    def report(self):
        """Submit the stats data to both the MCP stats queue and statsd"""
        values = dict()
        for item in self.counter.keys():
            values[item] = self.diff(item)
            if self.statsd:
                self.statsd.incr(item, values[item])
        self.previous.update(self.counter)
        self.reporting_queue.put({
            'name': self.name,
            'consumer_name': self.consumer_name,
            'counts': values
        }, True)
