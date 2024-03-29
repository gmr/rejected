.. rejected documentation master file, created by
   sphinx-quickstart on Wed Dec 17 10:31:58 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

rejected
========
Rejected is a AMQP consumer daemon and message processing framework. It allows
for rapid development of message processing consumers by handling all of the
core functionality of communicating with RabbitMQ and management of consumer
processes.

Rejected runs as a master process with multiple consumer configurations that are
each run it an isolated process. It has the ability to collect statistical
data from the consumer processes and report on it.

Rejected supports Python 2.7 and 3.4+.

|Version| |Status| |Climate| |License|

Features
--------

- Automatic exception handling including connection management and consumer restarting
- Smart consumer classes that can automatically decode and deserialize message bodies based upon message headers
- Metrics logging and submission to statsd and InfluxDB
- Built-in profiling of consumer code
- Ability to write asynchronous code in consumers allowing for parallel communication with external resources

Installation
------------
rejected is available from the `Python Package Index <https://preview-pypi.python.org/project/rejected/>`_
and can be installed by running :command:`pip install rejected`.

For additional dependencies for optional features:

  - To install HTML support, run :command:`pip install rejected[html]`
  - To install InfluxDB support, run :command:`pip install rejected[influxdb]`
  - To install MessagePack support, run :command:`pip install rejected[msgpack]`
  - To install Sentry support, run :command:`pip install rejected[sentry]`
  - For testing, including all dependencies, run :command:`pip install rejected[testing]`


Getting Started
---------------

.. toctree::
   :glob:
   :maxdepth: 2

   consumer_howto
   configuration
   example_config
   cli

API Documentation
-----------------

.. toctree::
   :glob:
   :maxdepth: 2

   consumer
   api_testing

Issues
------
Please report any issues to the Github repo at `https://github.com/gmr/rejected/issues <https://github.com/gmr/rejected/issues>`_

Source
------
rejected source is available on Github at  `https://github.com/gmr/rejected <https://github.com/gmr/rejected>`_

Version History
---------------
See :doc:`history`

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


.. |Version| image:: https://img.shields.io/pypi/v/rejected.svg?
   :target: https://pypi.python.org/pypi/rejected

.. |Status| image:: https://img.shields.io/travis/gmr/rejected.svg?
   :target: https://travis-ci.org/gmr/rejected

.. |License| image:: https://img.shields.io/pypi/l/rejected.svg?
   :target: https://rejected.readthedocs.org

.. |Climate| image:: https://img.shields.io/codeclimate/github/gmr/rejected.svg?
   :target: https://codeclimate.com/github/gmr/rejected
