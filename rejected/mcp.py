"""
Master Control Process
"""

import threading
import multiprocessing


import rejected.client as client
import rejected.exceptions as exception
import rejected.patterns as patterns
import rejected.utils as utils

class MCP(patterns.rejected_object):

    def __init__(self, config):
        self.config = config
        pass

    @utils.log
    def start(self):
        """
        Block and run here
        """
        # Our consumer list to iterate through
        consumers = list()

        if 'Consumers' not in self.config:
            raise exception.InvalidConfiguration('Missing Consumers')

        # Loop through and deal with our consumers
        for key in self.config['Consumers']:

            # A dict for this consumers objects
            consumer = dict()

            # Shortcut the config to make coding easier
            config = self.config['Consumers'][key]

            # We'll probably kick off processes and threads here

            # Make sure there is an exchange in the config
            if 'exchange' not in config:
                message = 'Missing exchange for Consumer: %s' % key
                raise exception.InvalidConfiguration(message)

            # Add the exchange
            consumer['exchange'] = client.Exchange(config['exchange'])

            # Add the queue or list of queues
            consumer['queues'] = list()

            # If we have an explicit definition or we didn't define queues
            if 'queue' in config or 'queues' not in config:

                # We have explicit configuration
                if 'queue' in config:
                    queue = client.Queue(config['queue'])

                # We don't care about the queue name and want defaults
                else:
                    queue = client.Queue()

                # Append to the list
                consumer['queues'].append(queue)

            if 'queues' in config:
                for queue_ in config['queues']:
                    queue = client.Queue(config['queues'][queue_])
                    consumer['queues'].append(queue)



            consumers.append(consumer)

        print consumers
