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

Application
-----------
The application section

Daemon
------

Logging
-------
Foo
