#!/usr/bin/env python
# encoding: utf-8
"""
Monitoring options to get queue depths for auto-ramp-up and ramp-back of consumers

Should other alternatives to Alice present themselves, this file is where they
should go.

Created by Gavin M. Roy on 2009-09-10.
Copyright (c) 2009 Insider Guides, Inc. All rights reserved.
"""

import json
import logging
import sys
import os
import time
import urllib

class Alice:
    """
    Alice is an interface to the awesome Alice service for RabbitMQ
    http://github.com/auser/alice/
    """
    def __init__(self):
        logging.debug('Alice: Monitor created')
        self.cache = {}

    def get_queue_depth(self, host = 'localhost', port = '9999', queue_name = 'test' ):

        # If port is coming in as None, use the default
        if port is None:
            port = '9999'

        # Check our cache
        cache_name = '%s-%s' % ( host, queue_name )
        if cache_name in self.cache:
            if time.time() - self.cache[cache_name]['timestamp'] < 10:
                logging.debug( 'Alice: Returning cached values for "%s": consumers: %i, depth: %i' %
                               ( cache_name,
                                 self.cache[cache_name]['consumers'],
                                 self.cache[cache_name]['depth'] ) )
                return self.cache[cache_name]

        # Get the queue data by passing in various flags
        url = 'http://%s:%s/queues/root/name/consumers/messages/messages_ready' % ( host, port )
        logging.debug('Alice: Querying %s' % url)
        response = urllib.urlopen(url)
        data = json.loads(response.read())

        # Loop through the queues and try and find the one we're looking for
        for queue in data['queues']:
            if queue['name'] == queue_name:
                self.cache[cache_name] = {'timestamp': time.time(), 'consumers': int(queue['consumers']), 'depth': int(queue['messages']) }
                logging.debug('Alice: Caching and returning values for "%s": consumers: %i, depth: %i' %
                               ( cache_name,
                                 self.cache[cache_name]['consumers'],
                                 self.cache[cache_name]['depth'] ) )
                return self.cache[cache_name]

        # We didn't find the queue in the list, so return 0's
        return { 'consumers': 0, 'depth': 0}

class Rabbit(object):
    def __init__(self):
        logging.debug("Rabbit: Monitor created")
        self.cache = {}

    def check_cache(self, cache_name):
        if cache_name in self.cache:
            if time.time() - self.cache[cache_name]['timestamp'] < 10:
                logging.debug('Rabbit: Returning cached values for "%s": consumers: %i, depth: %i' %
                              ( cache_name,
                                self.cache[cache_name]['consumers'],
                                self.cache[cache_name]['depth']))
                return self.cache[cache_name]
            else:
                return None
        else:
            return None

    def get_queue_depth(self, host = 'localhost', port = '55672', vhost = '%2f', queue_name = 'test'):
        cache_name = '%s-%s' % (host, queue_name)
        cache_val = self.check_cache(cache_name)
        if cache_val is not None:
            return  cache_val

        url = 'http://%s:%s/api/queues/%s/%s' % (host, port, vhost, queue_name)
        logging.debug('Rabbit: Querying %s' % url)
        try:
            response = urllib.urlopen(url)
            data = json.loads(response.read())
        except Exception as out:
            logging.debug("Error or no data from API")
            return { 'consumers': 0, 'depth': 0 }

        self.cache[cache_name] = {'timestamp': time.time(), 'consumers': int(data['consumers']),
                                    'depth': int(data['messages_ready'])}
        logging.debug('Rabbit: Caching and returning values for "%s": consumers: %i, depth: %i' %
                      ( cache_name,
                        self.cache[cache_name]['consumers'],
                        self.cache[cache_name]['depth']))
        return self.cache[cache_name]

