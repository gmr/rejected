Rejected
========
Rejected is a AMQP consumer daemon and message processing framework. It allows
for rapid development of message processing consumers by handling all of the
core functionality of communicating with RabbitMQ and management of consumer
processes.

Example Processor
-----------------
    from rejected import processor

    class Test(processor.Processor):
        def process(self, message):
            self._logger.debug('In Test.process: %s' % message.body)

Example Configuration
---------------------

     %YAML 1.2
     ---
     Application:
       monitor: true
       poll_interval: 60
       Connections:
         rabbitmq:
           host: rabbitmq
           port: 5672
           user: rejected
           pass: secret
           ssl: False
           vhost: /
       Consumers:
         test_processor:
           import: test_processor
           processor: Test
           connections: [rabbitmq]
           queue: test_queue

     Daemon:
         user: rejected
         group: daemon
         pidfile: /var/run/rejected/example.%(pid)s.pid

     Logging:
         version: 1
         formatters:
             verbose:
               format: '%(levelname) -10s %(asctime)s %(process)-6d %(processName) -15s %(name) -25s %(funcName) -20s: %(message)s'
               datefmt: '%Y-%m-%d %H:%M:%S'
             syslog:
               format: " %(levelname)s <PID %(process)d:%(processName)s> %(name)s.%(funcName)s(): %(message)s"
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
             rejected:
                 level: DEBUG
                 propagate: true
                 handlers: [console, syslog]
             urllib3:
                 level: ERROR
                 propagate: true
         disable_existing_loggers: false
         incremental: false
