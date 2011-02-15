"""
Master Control Process
"""
import logging
import threading
import multiprocessing
import signal
import time


import rejected.client as client
import rejected.exceptions as exception
import rejected.patterns as patterns
import rejected.utils as utils

class MCP(patterns.rejected_object):

    def __init__(self, config):
        self.config = config
        self.processes = dict()
        self.stopped = True

    @utils.log_method_call
    def start(self):
        """
        Block and run here
        """
        # Our consumer list to iterate through
        self.consumers = list()
        self.stop_list = list()

        self.stopped = False

        if 'Consumers' not in self.config:
            raise exception.InvalidConfiguration('Missing Consumers')

        # Loop through and deal with our consumers
        for key in self.config['Consumers']:
            config = self.config['Consumers'][key]
            if 'processes' in config:
                processes = config['processes'].get('min', 1)
            else:
                processes = 1

            for x in xrange(0, processes):
                stop_event = multiprocessing.Event()
                self.stop_list.append(stop_event)
                name = '%s-%i' % (key, x)
                process = Yori(name=name,
                               kwargs={'config': config, 'stop': stop_event})
                process.start()
                self.consumers.append(process)
                process.join(0)

            # Block until we have no active consumers or we're stopped
            children_connected = True
            while not self.stopped or children_connected:
                time.sleep(1)
                for consumer in self.consumers:
                    children_connected = consumer.is_alive()
                    if children_connected:
                        continue

    @utils.log_method_call
    def stop(self):
        for event in self.stop_list:
            logging.debug("Sending stop event %s" % event)
            event.set()

        # Wait for everything to stop
        stopped = False
        while not stopped:
            for consumer in self.consumers:
                stopped = consumer.is_alive()

        # Let our main blocking loop know we're stopped
        self.stopped = True


class Yori(multiprocessing.Process):

    @utils.log_method_call
    def run(self):
        signal.signal(signal.SIGTERM, self._terminate)

        self.children = []
        config = self._kwargs['config']
        stop = self._kwargs['stop']

        # We can just not have a threads section in the config
        if 'threads' in config:
            # But if we have it use the min or 1 if we don't have that
            threads = config['threads'].get('min', 1)
        else:
            # Default of 1
            threads = 1

        for x in xrange(0, threads):
            name = 'Thread-%i' % x
            thread = Sark(name=name, kwargs={'config': config})
            thread.start()
            self.children.append(thread)
            thread.join(0)

        # Wait for our parent to tell us we're done
        try:
            stop.wait()
        except KeyboardInterrupt:
            pass

        # We've been signaled, kill off our children
        self._terminate()

    @utils.log_method_call
    def _terminate(self, signum=0, frame=None):

        for child in self.children:
            logging.debug("Stopping: %s" % child)
            child.stop()

        # Wait for the connections to close
        closed = False
        while not closed:
            for child in self.children:
                closed = not child.connected
            time.sleep(1)
        logging.debug("Process stopping")


class Sark(threading.Thread):

    @utils.log_method_call
    def run(self):
        self.config = self._Thread__kwargs['config']
        self.connection = client.Connection(self.config['broker'],
                                            self._on_connected)

        # This blocks until we're no longer running the IOLoop in connection
        self.connection.start()

    @utils.log_method_call
    def stop(self):
        if hasattr(self, 'connection') and \
           hasattr(self.connection, 'close'):
            self.connection.close()

    @property
    def connected(self):
        if not hasattr(self, 'connection')or \
           hasattr(self.connection, 'is_open') and \
           not self.connection.is_open:
            return False
        return True

    @utils.log_method_call
    def _on_connected(self, connection, channel):

        self.connection = connection
        self.channel = channel

        # Add a callback so we can stop the ioloop
        self.connection.add_on_close_callback(self._on_closed)

        # Make sure there is an exchange in the config
        if 'exchange' not in self.config:
            message = 'Missing exchange for Consumer: %s' % key
            raise exception.InvalidConfiguration(message)

        # Add the exchange
        self.exchange = client.Exchange(self.channel,
                                        self.config['exchange'])

        # Add the queue or list of queues
        self.queues = list()

        # If we have an explicit definition or we didn't define queues
        if 'queue' in self.config or 'queues' not in self.config:

            # We have explicit configuration
            if 'queue' in self.config:
                queue = client.Queue(self.channel,
                                     self.config['queue'])

            # We don't care about the queue name and want defaults
            else:
                queue = client.Queue(self.channel)

            # Append to the list
            self.queues.append(queue)

        if 'queues' in self.config:
            for queue_ in self.config['queues']:
                queue = client.Queue(self.channel,
                                     self.config['queues'][queue_])
                self.queues.append(queue)

    @utils.log_method_call
    def _on_closed(self, frame):
        if hasattr(self, 'connection'):
            self.connection.ioloop.stop()
            del(self.connection)
