Rejected
========
Rejected is a AMQP consumer daemon and message processing framework. It allows
for rapid development of message processing consumers by handling all of the
core functionality of communicating with RabbitMQ and management of consumer
processes.

Features Include
----------------
 - Optional elastic scaling of consumers based upon demand
 - Specify a min # of consumers, max # of consumers and threshold to spawn new threads
 - Internal statistics gathering and exposure

Requirements
------------
 - pika 0.9.5
 - pyyaml

Example Processor
-----------------
    from rejected import processor

    class Test:
        def process(self, message):
            self._logger.debug('In Test.process: %s' % message.body)
            return True

Example Configuration
---------------------

    %YAML 1.2
    ---
    Logging:
        format: "%(levelname) -10s %(asctime)s  %(process)  -6d %(name) -30s: %(message)s"
        level: debug
        handler: syslog
        syslog:
          address: /dev/log
          facility: local6
        loggers:
            - [urllib3.connectionpool, debug]
            - [myyearbook.platform, debug]

    poll_interval: 60

    Connections:
        localhost:
            host: localhost
            port: 5672
            user: guest
            pass: guest
            ssl: False
            vhost: /

    Consumers:
        overdraft:
            import: myprocessor
            processor: Processor
            connections: [localhost]
            queue: test
            compressed: False
            no_ack: False
            min: 1
            max: 1
            max_errors: 5
            republish_on_error: False
            threshold: 1000
