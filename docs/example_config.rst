.. _config_example:

Configuration Example
=====================
The following example will configure rejected to a consumer that connects to two
different RabbitMQ servers, running two instances per connection, for a total
of four consumer processes. It will consume from a queue named ``generated_messages``
and provides configuration for the consumer code itself that would consist of a dict
with the keys ``foo`` and ``bar``.

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
      Connections:
        rabbit1:
          host: rabbit1
          port: 5672
          user: rejected
          pass: password
          ssl: False
          vhost: /
          heartbeat_interval: 300
        rabbit2:
          host: rabbit2
          port: 5672
          user: rejected
          pass: password
          ssl: False
          vhost: /
          heartbeat_interval: 300
      Consumers:
        example:
          consumer: example.Consumer
          connections: [rabbit1, rabbit2]
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
      pidfile: /var/run/rejected.pid

    Logging:
      version: 1
      formatters:
        verbose:
          format: "%(levelname) -10s %(asctime)s %(process)-6d %(processName) -25s %(name) -30s %(funcName) -30s: %(message)s"
          datefmt: "%Y-%m-%d %H:%M:%S"
        syslog:
          format: "%(levelname)s <PID %(process)d:%(processName)s> %(name)s.%(funcName)s(): %(message)s"
      filters: []
      handlers:
        console:
          class: logging.StreamHandler
          formatter: verbose
          debug_only: false
        syslog:
          class: logging.handlers.SysLogHandler
          facility: daemon
          address: /var/run/syslog
          #address: /dev/log
          formatter: syslog
      loggers:
        example:
          level: INFO
          propagate: true
          handlers: [console, syslog]
        helper:
          level: INFO
          propagate: true
          handlers: [console, syslog]
        rejected:
          level: INFO
          propagate: false
          handlers: [console, syslog]
        rejected.consumer:
          level: INFO
          propagate: false
          handlers: [console, syslog]
      root:
        level: INFO
        propagate: true
        handlers: [console, syslog]
      disable_existing_loggers: true
      incremental: false
