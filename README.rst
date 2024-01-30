Rejected
========

Rejected is a AMQP consumer daemon and message processing framework. It allows
for rapid development of message processing consumers by handling all of the
core functionality of communicating with RabbitMQ and management of consumer
processes.

Rejected runs as a master process with multiple consumer configurations that are
each run it an isolated process. It has the ability to collect statistical
data from the consumer processes and report on it.

Rejected 4.0+ supports Python 3.12+.

|Version| |Status| |Coverage| |License|

Features
--------

- Automatic exception handling including connection management and consumer restarting
- Smart consumer classes that can automatically decode and deserialize message bodies based upon message headers
- Metrics logging and submission to statsd and InfluxDB
- Built-in profiling of consumer code
- Ability to write asynchronous code in consumers allowing for parallel communication with external resources

Documentation
-------------

https://rejected.readthedocs.io

Example Consumers
-----------------
.. code:: python

    from rejected import consumer
    import logging

    LOGGER = logging.getLogger(__name__)


    class Test(consumer.Consumer):

        async def process(self, message):
            LOGGER.debug('In Test.process: %s' % message.body)


Version History
---------------
Available at https://rejected.readthedocs.org/en/latest/history.html

.. |Version| image:: https://img.shields.io/pypi/v/rejected.svg?
   :target: https://pypi.python.org/pypi/rejected

.. |Status| image:: https://img.shields.io/travis/gmr/rejected.svg?
   :target: https://travis-ci.org/gmr/rejected

.. |Coverage| image:: https://img.shields.io/codecov/c/github/gmr/rejected.svg?
   :target: https://codecov.io/github/gmr/rejected?branch=master

.. |License| image:: https://img.shields.io/pypi/l/rejected.svg?
   :target: https://rejected.readthedocs.org
