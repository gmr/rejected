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


def main():
    conn = amqp.Connection(host="mq07:5672 ", userid="guest",
        password="guest", virtual_host="/", insist=False)
    chan = conn.channel()

    chan.queue_declare(queue="Hotmail", durable=True,
        exclusive=False, auto_delete=False)
    chan.exchange_declare(exchange="Email", type="direct", durable=True,
            auto_delete=False,)
    
    chan.queue_bind(queue="Hotmail", exchange="Email",
                  routing_key="Email.Hotmail")
    
    for i in range(0, 100000):
    	msg = amqp.Message("Test message %i!" % i)
    	msg.properties["delivery_mode"] = 2
    	chan.basic_publish(msg,exchange="Email",routing_key="Email.Hotmail")    
    
    pass


if __name__ == '__main__':
    main()

