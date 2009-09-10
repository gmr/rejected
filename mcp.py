#!/usr/bin/env python
# encoding: utf-8
"""
The Master Control Program Class

Maintains the stack of threads and controls the behavior of consuming.

Created by Gavin M. Roy on 2009-09-10.
Copyright (c) 2009 Insider Guides, Inc.. All rights reserved.
"""

import logging
import sys
import os
import unittest

from monitors import alice

class mcp:

    def __init__(self, config, options):
        logging.debug('MCP Created')
        self.alice = alice()
        self.config = config
        self.threads = []
        pass

    def connect(self, connection, configuration):
        """ Connect to an AMQP Broker  """

        return amqp.Connection( host ='%s:%s' % ( configuration['host'], configuration['port'] ),
                                userid =  configuration['user'], 
                                password =  configuration['pass'], 
                                ssl =  configuration['ssl'],
                                virtual_host =  configuration['vhost'] )                               

    def poll(self):
        logging.debug('MCP Polling')
        self.alice.getQueueDepth()
        pass

    def shutdown(self):
        logging.debug('MCP Shutting Down')
        pass
        
    def start(self):
        logging.debug('MCP Starting Up')
        pass

class mcpTests(unittest.TestCase):
    def setUp(self):
        pass


if __name__ == '__main__':
    unittest.main()