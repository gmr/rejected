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
import urllib

class alice:
    
    def __init__(self):
        logging.debug('Alice Monitor created')
        
    def get_queue_depth(self, host = 'localhost', queue_name = 'test' ):
        
        # Get the queue data by passing in various flags
        url = 'http://%s:8161/queues/root/name/consumers/messages/messages_ready' % host
        logging.debug('Querying %s' % url)
        response = urllib.urlopen(url)
        data = json.loads(response.read())
        
        # Loop through the queues and try and find the one we're looking for
        for queue in data['queues']:
            if queue['name'] == queue_name:
                return {'consumers': queue['consumers'], 'depth': queue['messages'] }

        # We didn't find the queue in the list, so return 0's
        return { 'consumers': 0, 'depth': 0}