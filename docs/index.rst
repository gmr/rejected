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

|Version| |Downloads| |Status| |Climate| |License|

Features
--------

- Dynamic QoS
- Automatic exception handling including connection management and consumer restarting
- Smart consumer classes that can automatically decode and deserialize message bodies based upon message headers
- Metrics logging and submission to statsd
- Built-in profiling of consumer code

Installation
------------
rejected is available from the `Python Package Index <https://preview-pypi.python.org/project/rejected/>`_ and can be installed by running :command:`easy_install rejected` or :command:`pip install rejected`

Getting Started
---------------

.. toctree::
   :glob:
   :maxdepth: 2

   consumer_howto
   configuration
   cli

API Documentation
-----------------

.. toctree::
   :glob:
   :maxdepth: 2

   consumer

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

.. |Downloads| image:: https://img.shields.io/pypi/dm/rejected.svg?
   :target: https://pypi.python.org/pypi/rejected

.. |License| image:: https://img.shields.io/pypi/l/rejected.svg?
   :target: https://rejected.readthedocs.org

.. |Climate| image:: https://img.shields.io/codeclimate/github/gmr/rejected.svg?
   :target: https://codeclimate.com/github/gmr/rejected
