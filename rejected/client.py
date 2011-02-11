# -*- coding: utf-8 -*-
"""
RabbitMQ/Pika Client
"""

import rejected.exceptions as exception
import rejected.patterns as patterns
import rejected.utils as utils


class Exchange(patterns.rejected_object):

    @utils.log
    def __init__(self, config=dict()):
        """
        Expects a dictionary with the following parameters:

        name:        Queue name

        type:        Queue type, one of direct, topic, fanout
                     Default: direct

        auto_delete: Auto-delete queue when disconnected
                     Default: True if auto named, otherwise False

        durable:     Is a durable queue, will survive RabbitMQ restarts.
                     Default: False
        """
        if 'name' in config:
            self.name = config['name']
        else:
            raise exception.InvalidConfiguration("Missing exchange name")

        if 'type' in config:
            self.excusive = config['type']
        else:
            self.exclusive = 'direct'

        if 'auto_delete' in config:
            self.auto_delete = config['auto_delete']
        else:
            self.auto_delete = True

        if 'durable' in config:
            self.durable = config['durable']
        else:
            self.durable = False


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
            config['durable'] = False
            config['exclusive'] = True

        if 'auto_delete' in config:
            self.auto_delete = config['auto_delete']
        else:
            self.auto_delete = True

        if 'durable' in config:
            self.durable = config['durable']
        else:
            self.durable = False

        if 'exclusive' in config:
            self.excusive = config['exclusive']
        else:
            self.exclusive = False

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
