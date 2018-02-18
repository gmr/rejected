.. rejected documentation master file, created by
   sphinx-quickstart on Wed Dec 17 10:31:58 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

rejected
========
Rejected is a AMQP consumer daemon and message processing framework for Python 2 and 3.
It allows for rapid development of consumer applications by handling all of the
core functionality of communicating with RabbitMQ and management of application
processes.

Features
--------

- Automatic exception handling including connection management and consumer restarting
- Smart consumer classes that can automatically decode and deserialize message bodies based upon message headers
- Metrics logging and submission to statsd and InfluxDB
- Built-in profiling of consumer code
- Ability to write asynchronous code in consumers allowing for parallel communication with external resources
- Testing framework to ease testing of consumer applications

Getting Started
---------------

.. toctree::
   :maxdepth: 1

   installation
   consumers
   testing

Configuration and Usage
-----------------------

.. toctree::
   :maxdepth: 1

   configuration
   example_config
   cli

Class Documentation
-------------------

.. toctree::
   :maxdepth: 2

   api_consumers
   api_data
   api_mixins
   api_exceptions
   api_testing
   api_internal

Issues
------
Please report any issues to the Github repo at `https://github.com/gmr/rejected/issues <https://github.com/gmr/rejected/issues>`_

Source
------
rejected source is available on Github at  `https://github.com/gmr/rejected <https://github.com/gmr/rejected>`_

License
-------
rejected is released under the `BSD 3-clause License <https://github.com/gmr/rejected/blob/master/LICENSE>`_

Version History
---------------
.. toctree::
   :maxdepth: 1

   history

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
