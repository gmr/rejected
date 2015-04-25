Rejected
========

Rejected is a AMQP consumer daemon and message processing framework. It allows
for rapid development of message processing consumers by handling all of the
core functionality of communicating with RabbitMQ and management of consumer
processes.

Rejected runs as a master process with multiple consumer configurations that are
each run it an isolated process. It has the ability to collect statistical
data from the consumer processes and report on it.

|Version| |Downloads| |Status| |License|

Features
--------

- Dynamic QoS
- Automatic exception handling including connection management and consumer restarting
- Smart consumer classes that can automatically decode and deserialize message bodies based upon message headers
- Metrics logging and submission to statsd
- Built-in profiling of consumer code
- Ability to write asynchronous code in consumers allowing for parallel communication with external resources

Documentation
-------------

https://rejected.readthedocs.org

Example Consumers
-----------------
.. code:: python

    from rejected import consumer
    import logging

    LOGGER = logging.getLogger(__name__)


    class Test(consumer.Consumer):
        def process(self, message):
            LOGGER.debug('In Test.process: %s' % message.body)

Async Consumer
^^^^^^^^^^^^^^
To make a consumer async, you can decorate the
`Consumer.prepare <http://rejected.readthedocs.org/en/latest/api_consumer.html#rejected.consumer.Consumer.prepare>`_
and `Consumer.process <http://rejected.readthedocs.org/en/latest/api_consumer.html#rejected.consumer.Consumer.process>`_
methods using Tornado's
`@gen.coroutine <http://www.tornadoweb.org/en/stable/gen.html#tornado.gen.coroutine>`_.
Asynchronous consumers to handle multiple messages in the same process, but
rather allow you to use asynchronous clients like
`Tornado's <http://tornadoweb.org>`_
`AsyncHTTPClient <http://www.tornadoweb.org/en/stable/httpclient.html>`_ and the
`Queries <http://queries.readthedocs.org/en/latest/tornado_session.html>`_
PostgreSQL library to perform parallel tasks using coroutines.

.. code:: python

    import logging

    from rejected import consumer

    from tornado import gen
    from tornado import httpclient


    class AsyncExampleConsumer(consumer.Consumer):

        @gen.coroutine
        def process(self):
            LOGGER.debug('Message: %r', self.body)
            http_client = httpclient.AsyncHTTPClient()
            results = yield [http_client.fetch('http://www.github.com'),
                             http_client.fetch('http://www.reddit.com')]
            LOGGER.info('Length: %r', [len(r.body) for r in results])


Example Configuration
---------------------
.. code:: yaml

    %YAML 1.2
    ---
    Application:
      poll_interval: 10.0
      log_stats: True
      statsd:
        enabled: True
        host: localhost
        port: 8125
        prefix: applications.rejected
      Connections:
        rabbitmq:
          host: localhost
          port: 5672
          user: guest
          pass: guest
          ssl: False
          vhost: /
          heartbeat_interval: 300
      Consumers:
        example:
          consumer: rejected.example.Consumer
          connections: [rabbitmq]
          qty: 2
          queue: generated_messages
          dynamic_qos: True
          ack: True
          max_errors: 100
          config:
            foo: True
            bar: baz

     Daemon:
       user: rejected
       group: daemon
       pidfile: /var/run/rejected/example.%(pid)s.pid

     Logging:
       version: 1
       formatters:
         verbose:
           format: "%(levelname) -10s %(asctime)s %(process)-6d %(processName) -15s %(name) -25s %(funcName) -20s: %(message)s"
           datefmt: "%Y-%m-%d %H:%M:%S"
         syslog:
           format: "%(levelname)s <PID %(process)d:%(processName)s> %(name)s.%(funcName)s(): %(message)s"
       filters: []
       handlers:
         console:
           class: logging.StreamHandler
           formatter: verbose
           debug_only: true
         syslog:
           class: logging.handlers.SysLogHandler
           facility: local6
           address: /var/run/syslog
           #address: /dev/log
           formatter: syslog
       loggers:
         my_consumer:
           level: INFO
           propagate: true
           handlers: [console, syslog]
         rejected:
           level: INFO
           propagate: true
           handlers: [console, syslog]
         urllib3:
           level: ERROR
           propagate: true
       disable_existing_loggers: false
       incremental: false


Version History
---------------
Available at https://rejected.readthedocs.org/en/latest/history.html

.. |Version| image:: https://badge.fury.io/py/rejected.svg?
   :target: http://badge.fury.io/py/rejected

.. |Status| image:: https://travis-ci.org/gmr/rejected.svg?branch=master
   :target: https://travis-ci.org/gmr/rejected

.. |Downloads| image:: https://pypip.in/d/rejected/badge.svg?
   :target: https://pypi.python.org/pypi/rejected

.. |License| image:: https://pypip.in/license/rejected/badge.svg?
   :target: https://rejected.readthedocs.org
