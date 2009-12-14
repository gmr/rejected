#!/usr/bin/env python
# encoding: utf-8
"""
enqueue-test.py

Created by Gavin M. Roy on 2009-09-11.
Copyright (c) 2009 Insider Guides, Inc.. All rights reserved.
"""

import amqplib.client_0_8 as amqp
import sys
import os
import zlib

def main():
    conn = amqp.Connection( host="localhost:5672 ", userid="guest",
                            password="guest", virtual_host="/", insist=False )
    chan = conn.channel()

    chan.queue_declare( queue="TestQueue", durable=True,
                        exclusive=False, auto_delete=False )

    chan.exchange_declare( exchange="Test", type="direct", durable=True,
                           auto_delete=False )
    
    chan.queue_bind( queue="TestQueue", exchange="Test",
                     routing_key="Test.TestQueue" )
    
    for i in xrange(0, 1000):
        
        content = '<p>Lorem ipsum dolor sit amet, consectetuer adipiscing elit. Etiam sit amet elit vitae arcu interdum ullamcorper. Nullam ultrices, nisi quis scelerisque convallis, augue neque tempor enim, et mattis justo nibh eu elit. Quisque ultrices gravida pede. Mauris accumsan vulputate tellus. Phasellus condimentum bibendum dolor. Mauris sed ipsum. Phasellus in diam. Nam sapien ligula, consectetuer id, hendrerit in, cursus sed, leo. Nam tincidunt rhoncus urna. Aliquam id massa ut nibh bibendum imperdiet. Curabitur neque mauris, porta vel, lacinia quis, placerat ultrices, orci.</p>'
        message = zlib.compress(content, 9)
    
    	msg = amqp.Message(message)
    	chan.basic_publish(msg,exchange="Test",routing_key="Test.TestQueue")    

if __name__ == '__main__':
    main()

