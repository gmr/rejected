Configuration File Syntax
=========================
The rejected configuration uses `YAML <http://yaml.org>`_ as the markup language.
YAML's format, like Python code is whitespace dependent for control structure in
blocks. If you're having problems with your rejected configuration, the first
thing you should do is ensure that the YAML syntax is correct. `yamllint.com <http://yamllint.com>`_
is a good resource for validating that your configuration file can be parsed.

The configuration file is split into three main sections: Application, Daemon, and Logging.

The :ref:`example configuration <config_example>` file provides a good starting
point for creating your own configuration file.

.. _application:

Application
-----------
The application section of the configuration is broken down into multiple top-level options:

+---------------+-----------------------------------------------------------------------------------------+
| poll_interval | How often rejected should poll consumer processes for stats data in seconds (int/float) |
+---------------+-----------------------------------------------------------------------------------------+
| log_stats     | Should stats data be logged via Python's standard logging mechanism (bool)              |
+---------------+-----------------------------------------------------------------------------------------+
| `statsd`_     | Enable and configure statsd metric submission (obj)                                     |
+---------------+-----------------------------------------------------------------------------------------+
| `Connections`_| A subsection with a list of RabbitMQ connection information for consumers (list)        |
+---------------+-----------------------------------------------------------------------------------------+
| `Consumers`_  | Where each consumer type is configured (obj)                                            |
+---------------+-----------------------------------------------------------------------------------------+

statsd
^^^^^^
enabled: True
host: localhost
port: 8125

Connections
^^^^^^^^^^^

    rabbitmq:
      host: localhost
      port: 5672
      user: guest
      pass: guest
      ssl: False
      vhost: /
      heartbeat_interval: 300

Consumers
^^^^^^^^^




.. _daemon:

Daemon
------
This section contains the settings required to run the application as a daemon. They are as follows:

user
    The username to run as when the process is daemonized
group [optional]
    The group name to switch to when the process is daemonized
pidfile
    The pidfile to write when the process is daemonized

.. _logging:

Logging
-------
rejected uses :py:mod:`logging.config.dictConfig <logging.config>` to create a flexible method for configuring the python standard logging module. If rejected is being run in Python 2.6, `logutils.dictconfig.dictConfig <https://pypi.python.org/pypi/logutils>`_ is used instead.

The following basic example illustrates all of the required sections in the dictConfig format, implemented in YAML::

    version: 1
    formatters: []
    verbose:
      format: '%(levelname) -10s %(asctime)s %(process)-6d %(processName) -15s %(name) -10s %(funcName) -20s: %(message)s'
      datefmt: '%Y-%m-%d %H:%M:%S'
    handlers:
      console:
        class: logging.StreamHandler
        formatter: verbose
        debug_only: True
    loggers:
      rejected:
        handlers: [console]
        level: INFO
        propagate: true
      myconsumer:
        handlers: [console]
        level: DEBUG
        propagate: true
    disable_existing_loggers: true
    incremental: false

.. NOTE::
    The debug_only node of the Logging > handlers > console section is not part of the standard dictConfig format. Please see the :ref:`caveats` section below for more information.

.. _caveats:

Logging Caveats
^^^^^^^^^^^^^^^
In order to allow for customizable console output when running in the foreground and no console output when daemonized, a ``debug_only`` node has been added to the standard dictConfig format in the handler section. This method is evaluated when logging is configured and if present, it is removed prior to passing the dictionary to dictConfig if present.

If the value is set to true and the application is not running in the foreground, the configuration for the handler and references to it will be removed from the configuration dictionary.

Troubleshooting
^^^^^^^^^^^^^^^
If you find that your application is not logging anything or sending output to the terminal, ensure that you have created a logger section in your configuration for your consumer package. For example if your Consumer instance is named ``myconsumer.MyConsumer`` make sure there is a ``myconsumer`` logger in the logging configuration.
