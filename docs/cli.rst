Command-Line Options
====================
The :command:`rejected` command line application allows you to spawn the rejected process
as a daemon. Additionally it has options for running interactively (``-f``), which
along with the ``-o`` switch for specifying a single consumer to run and ``-q``
to specify quantity, makes for easier debugging.

If you specify ``-P /path/to/write/data/to``, rejected will automatically enable
:py:mod:`cProfile`, writing the profiling data to the path specified. This can
be used in conjunction with `graphviz <http://www.graphviz.org/>`_ to diagram
code execution and hotspots.

Usage
-----

.. code-block:: none

   usage: rejected [-h] [-c CONFIG] [-f] [-P PROFILE] [-o CONSUMER]
                   [-p PREPEND_PATH] [-q QUANTITY] [--version]

   RabbitMQ consumer framework

   optional arguments:
     -h, --help            show this help message and exit
     -c CONFIG, --config CONFIG
                           Path to the configuration file
     -f, --foreground      Run the application interactively
     -P PROFILE, --profile PROFILE
                           Profile the consumer modules, specifying the output
                           directory.
     -o CONSUMER, --only CONSUMER
                           Only run the consumer specified
     -p PREPEND_PATH, --prepend-path PREPEND_PATH
                           Prepend the python path with the value.
     -q QUANTITY, --qty QUANTITY
                           Run the specified quantity of consumer processes when
                           used in conjunction with -o
     --version             show program's version number and exit
