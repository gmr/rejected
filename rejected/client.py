# -*- coding: utf-8 -*-
"""
RabbitMQ/Pika Client
"""
import logging
import pika
import rejected.exceptions as exception
import rejected.patterns as patterns
import rejected.utils as utils
import time


class Connection(patterns.rejected_object):

    @utils.log
    def __init__(self, config, on_connected_callback):

        parameters = pika.ConnectionParameters()
        parameters.host = config.get('host', 'localhost')
        parameters.port = config.get('port', 5672)
        parameters.vhost = config.get('vhost', '/')
        parameters.heartbeat = config.get('heartbeat', 0)

        user = config.get('user', 'guest')
        password = config.get('password', 'guest')
        parameters.credentials = pika.PlainCredentials(user, password)

        self.connection_type = config.get('type', 'SelectConnection')

        if self.connection_type == 'SelectConnection':
            from pika.adapters import SelectConnection
            self.connection = SelectConnection(parameters,
                                               on_connected_callback)

        if self.connection_type == 'TornadoConnection':
            from pika.adapters import TornadoConnection
            self.connection = TornadoConnection(parameters,
                                                on_connected_callback)

    @property
    def connected(self):
        return self.connection.is_alive

    @utils.log
    def start(self):
        self.connection.ioloop.start()

class Exchange(patterns.rejected_object):

    @utils.log
    def __init__(self, config=dict()):
        """
        Expects a dictionary with the following parameters:

        name:        Exchange name

        type:        Queue type, one of direct, topic, fanout
                     Default: direct

        auto_delete: Auto-delete exchange when disconnected.
                     Default: False

        durable:     Is a durable exchange, will survive RabbitMQ restarts.
                     Default: True
        """
        if not 'name' in config:
            raise exception.InvalidConfiguration("Missing exchange name")

        self.name = config['name']
        self.type = config.get("type", 'direct')
        self.auto_delete = config.get('auto_delete', False)
        self.durable = config.get('durable', True)


class Queue(patterns.rejected_object):

    @utils.log
    def __init__(self, config=dict()):
        """
        Expects a dictionary with the following optional parameters:

        name:        Queue name
                     Default: Auto name the queue using Queue._auto_name

        auto_delete: Auto-delete queue when disconnected
                     Default: True if auto named, otherwise False

        durable:     Is a durable queue, will survive RabbitMQ restarts.
                     Default: False

        exclusive:   Is an exclusive consumer
                     Default: True if auto named otherwise False
        """

        # Setup our queue name if we do not have one declared or auto=True
        if 'name' in config:
            self.name = config['name']
        else:
            self.name = self._auto_name()  # Auto name the queue
            # Overwrite possible entries for auto named behavior
            config['auto_delete'] = True

        self.auto_delete = config.get('auto_delete', True)
        self.durable = config.get('durable', False)
        self.exclusive = config.get('exclusive', self.auto_delete)

    @utils.log
    def _auto_name(self):
        """
        Generate a queue name
        """
        # Get our multiprocessing and threading classes to return our context
        # for the queuename
        import multiprocessing
        import threading

        return '%s.%s.%s.%s' % (utils.application_name(),
                                utils.hostname(),
                                multiprocessing.current_process().name,
                                threading.current_thread().name)

    def declare(self):
        pass

    def _on_declare_ok(self):
        pass
