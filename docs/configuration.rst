Configuration File Syntax
=========================
Rejected's :command:`consumerd` configuration uses `YAML <http://yaml.org>`_ as
the markup language. YAML's format, like Python code is whitespace dependent for
control structure in blocks. If you're having problems with your rejected
configuration, the first thing you should do is ensure that the YAML syntax is
correct. `yamllint.com <http://yamllint.com>`_ is a good resource for validating
that your configuration file can be parsed.

The configuration file is split into three main sections: ``Application``,
``Daemon``, and ``Logging``.

The :ref:`example configuration <config_example>` file provides a good starting
point for creating your own configuration file.

.. _application:

Application
-----------
The application section of the configuration is broken down into multiple top-level options:

+---------------+-----------------------------------------------------------------------------------------+
| poll_interval | How often rejected should poll consumer processes for status in seconds (int/float)     |
+---------------+-----------------------------------------------------------------------------------------+
| sentry_dsn    | If Sentry support is installed, optionally set a global DSN for all consumers (str)     |
+---------------+-----------------------------------------------------------------------------------------+
| `stats`_      | Enable and configure statsd metric submission (obj)                                     |
+---------------+-----------------------------------------------------------------------------------------+
| `Connections`_| A subsection with RabbitMQ connection information for consumers (obj)                   |
+---------------+-----------------------------------------------------------------------------------------+
| `Consumers`_  | Where each consumer type is configured (obj)                                            |
+---------------+-----------------------------------------------------------------------------------------+

stats
^^^^^
+-------+----------------------------------------------------------------------------------------+
| stats |                                                                                        |
+=======+===============+========================================================================+
|       | log           | Toggle  top-level logging of consumer process stats (bool)             |
+-------+---------------+------------------------------------------------------------------------+
|       | `influxdb`_   | Configure the submission of per-message measurements to InfluxDB (obj) |
+-------+---------------+------------------------------------------------------------------------+
|       | `statsd`_     | Configure the submission of per-message measurements to statsd (obj)   |
+-------+---------------+------------------------------------------------------------------------+

influxdb
^^^^^^^^
+------------------+------------------------------------------------------------------------------------------------------+
| stats > influxdb |                                                                                                      |
+==================+==========+===========================================================================================+
|                  | scheme   | The scheme to use when submitting metrics to the InfluxDB server. Default: ``http`` (str) |
+------------------+----------+-------------------------------------------------------------------------------------------+
|                  | host     | The hostname or ip address of the InfluxDB server. Default: ``localhost`` (str)           |
+------------------+----------+-------------------------------------------------------------------------------------------+
|                  | port     | The port of the influxdb server. Default: ``8086`` (int)                                  |
+------------------+----------+-------------------------------------------------------------------------------------------+
|                  | user     | An optional username to use when submitting measurements. (str)                           |
+------------------+----------+-------------------------------------------------------------------------------------------+
|                  | password | An optional password to use when submitting measurements. (str)                           |
+------------------+----------+-------------------------------------------------------------------------------------------+
|                  | database | The InfluxDB database to submit measurements to. Default: ``rejected`` (str)              |
+------------------+----------+-------------------------------------------------------------------------------------------+

statsd
^^^^^^
+----------------+-------------------------------------------------------------------------------------------+
| stats > statsd |                                                                                           |
+================+=========+=================================================================================+
|                | enabled          | Toggle statsd reporting off and on (bool)                              |
+----------------+------------------+------------------------------------------------------------------------+
|                | prefix           | An optional prefix to use when creating the statsd metric path (str)   |
+----------------+------------------+------------------------------------------------------------------------+
|                | host             | The hostname or ip address of the statsd server (str)                  |
+----------------+------------------+------------------------------------------------------------------------+
|                | port             | The port of the statsd server. Default: ``8125`` (int)                 |
+----------------+------------------+------------------------------------------------------------------------+
|                | include_hostname | Include the hostname in the measurement path. Default: ``True`` (bool) |
+----------------+------------------+------------------------------------------------------------------------+

Connections
^^^^^^^^^^^
Each RabbitMQ connection entry should be a nested object with a unique name with connection attributes.

+-----------------+-------------------------------------------------------------------------------------+
| Connection Name |                                                                                     |
+=================+=====================+===============================================================+
|                 | host                | The hostname or ip address of the RabbitMQ server (str)       |
|                 +---------------------+---------------------------------------------------------------+
|                 | port                | The port of the RabbitMQ server (int)                         |
|                 +---------------------+---------------------------------------------------------------+
|                 | vhost               | The virtual host to connect to (str)                          |
|                 +---------------------+---------------------------------------------------------------+
|                 | user                | The username to connect as (str)                              |
|                 +---------------------+---------------------------------------------------------------+
|                 | pass                | The password to use (str)                                     |
|                 +---------------------+---------------------------------------------------------------+
|                 | ssl                 | Optional: whether to connect via SSL (boolean) default: False |
|                 +---------------------+---------------------------------------------------------------+
|                 | heartbeat_interval  | Optional: the AMQP heartbeat interval (int) default: 300 sec  |
+-----------------+---------------------+---------------------------------------------------------------+

Consumers
^^^^^^^^^
Each consumer entry should be a nested object with a unique name with consumer attributes.

+---------------+-----------------------------------------------------------------------------------------------------------+
| Consumer Name |                                                                                                           |
+===============+=======================+===================================================================================+
|               | consumer              | The package.module.Class path to the consumer code (str)                          |
|               +-----------------------+-----------------------------------------------------------------------------------+
|               | connections           | The connections to connect to (list) - See `Consumer Connections`_                |
|               +-----------------------+-----------------------------------------------------------------------------------+
|               | qty                   | The number of consumers per connection to run (int)                               |
|               +-----------------------+-----------------------------------------------------------------------------------+
|               | queue                 | The RabbitMQ queue name to consume from (str)                                     |
|               +-----------------------+-----------------------------------------------------------------------------------+
|               | ack                   | Explicitly acknowledge messages (no_ack = not ack) (bool)                         |
|               +-----------------------+-----------------------------------------------------------------------------------+
|               | max_errors            | Number of errors encountered before restarting a consumer (int)                   |
|               +-----------------------+-----------------------------------------------------------------------------------+
|               | sentry_dsn            | If Sentry support is installed, set a consumer specific sentry DSN (str)          |
|               +-----------------------+-----------------------------------------------------------------------------------+
|               | drop_exchange         | The exchange to publish a message to when it is dropped. If not specified,        |
|               |                       | dropped messages are not republished anywhere.                                    |
|               +-----------------------+-----------------------------------------------------------------------------------+
|               | drop_invalid_messages | Drop a message if the type property doesn't match the specified message type (str)|
|               +-----------------------+-----------------------------------------------------------------------------------+
|               | message_type          | Used to validate the message type of a message before processing. This attribute  |
|               |                       | can be set to a string that is matched against the AMQP message type or a list of |
|               |                       | acceptable message types. (str, array)                                            |
|               +-----------------------+-----------------------------------------------------------------------------------+
|               | error_exchange        | The exchange to publish messages that raise                                       |
|               |                       | :exc:`~rejected.consumer.ProcessingException` to (str)                            |
|               +-----------------------+-----------------------------------------------------------------------------------+
|               | error_max_retry       | The number of :exc:`~rejected.consumer.ProcessingException` raised on a message   |
|               |                       | before a message is dropped. If not specified messages will never be dropped (int)|
|               +-----------------------+-----------------------------------------------------------------------------------+
|               | influxdb_measurement  | When using InfluxDB, the measurement name for per-message measurements.           |
|               |                       | Defaults to the consumer name. (str)                                              |
|               +-----------------------+-----------------------------------------------------------------------------------+
|               | config                | Free-form key-value configuration section for the consumer (obj)                  |
+---------------+-----------------------+-----------------------------------------------------------------------------------+

Consumer Connections
^^^^^^^^^^^^^^^^^^^^
The consumer connections configuration allows for one or more connections to be
made by a single consumer. This configuration section supports two formats. If
a list of connection names are specified, the consumer will connect to and consume
from the all of the specified connections.

.. code:: yaml

    Consumer Name:
        connections:
          - connection1
          - connection2

If the ``connections`` list include structured values, additional settings can be
set. For example, you may want to consume from one RabbitMQ broker and publish to
another, as is illustrated below:

.. code:: yaml

    Consumer Name:
        connections:
          - name: connection1
            consume: True
            publisher_confirmation: False
          - name: connection2
            consume: False
            publisher_confirmation: True

In the above example, the consumer will have two connections, ``connection1`` and
``connection2``. It will only consume from ``connection1`` but can publish
messages ``connection2`` by specifying the connection name in the
:py:meth:`~rejected.consumer.Consumer.publish_message` method.

Structured Connections
!!!!!!!!!!!!!!!!!!!!!!

When specifying a structured consumer connection, the following attributes are
available.

+-----------------------------+---------------------------------------------------------------------------------------------+
| Consumer Name > connections |                                                                                             |
+=============================+========================+====================================================================+
|                             | name                   | The connection name, as specified in the Connections section of    |
|                             |                        | the application configuration.                                     |
|                             +------------------------+--------------------------------------------------------------------+
|                             | consume                | Specify if the connection should consume on the connection. (bool) |
+-----------------------------+------------------------+--------------------------------------------------------------------+
|                             | publisher_confirmation | Enable publisher confirmations. (bool)                             |
+-----------------------------+------------------------+--------------------------------------------------------------------+

.. _daemon:

Daemon
------
This section contains the settings required to run the application as a daemon. They are as follows:

+---------+---------------------------------------------------------------------------+
| user    | The username to run as when the process is daemonized (bool)              |
+---------+---------------------------------------------------------------------------+
| group   | Optional The group name to switch to when the process is daemonized (str) |
+---------+---------------------------------------------------------------------------+
| pidfile | The pidfile to write when the process is daemonized (str)                 |
+---------+---------------------------------------------------------------------------+


.. _logging:

Logging
-------
rejected uses :py:mod:`logging.config.dictConfig <logging.config>` to create a flexible method for configuring the python standard logging module. If rejected is being run in Python 2.6, `logutils.dictconfig.dictConfig <https://pypi.python.org/pypi/logutils>`_ is used instead.

The following basic example illustrates all of the required sections in the dictConfig format, implemented in YAML:

.. code:: yaml

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
