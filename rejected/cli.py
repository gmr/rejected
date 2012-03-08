"""
cli.py

"""
__author__ = 'Gavin M. Roy'
__email__ = 'gmr@myyearbook.com'
__since__ = '2011-07-22'

import daemon
import grp
import logging
import logging_config
import optparse
from os import path
import signal
import sys
import yaml

# Windows doesn't support pwd
try:
    import pwd
except ImportError:
    pwd = None

from rejected import mcp
from rejected import __version__


class CLI(object):

    def __init__(self):
        """Create a new instance of the CLI Object, checking the CLI
        options and configuration.

        """
        self._logger = logging.getLogger(__name__)

        # Daemon context if needed
        self._context = None

        # Create a new instance of the parser
        self._parser = self._option_parser()

        # Add the options to the option parser
        self._add_parser_options()

        # Get the command line options
        self._options = self._get_options()

        # Configure the various aspects
        self._config = self._load_configuration()

        # Get the logging object
        self._logging = self._logging_config()

        # Setup the logging
        self._logging.setup()

        # Specify the signal map
        self._signal_map = {signal.SIGTERM: self.stop}

    def _add_parser_options(self):
        """Add options to the parser"""
        self._parser.add_option('-c', '--config',
                                action='callback',
                                callback=self._validate_config,
                                type='string',
                                help='Specify the configuration file to load.')

        self._parser.add_option('-f', '--foreground',
                                action='store_true',
                                dest='foreground',
                                default=False,
                                help='Run in the foreground in debug mode.')

        self._parser.add_option('-p', '--prepend-path',
                                action='store',
                                default=None,
                                dest='prepend_path',
                                help='Prepend the python path with the ' \
                                     'specified value.')

    def _change_group(self, group_name):
        """Change the group to the specified value.

        :param str group_name: The group name to change to


        """
        self._context.gid = self._group_id(group_name)

    def _change_user(self, user_name):
        """Change the user to the specified value.

        :param str user_name: The user to change to

        """
        self._context.uid = self._user_id(user_name)

    def _daemon_context(self):
        """Return an instance of the DaemonContext object

        :returns: daemon.DaemonContext

        """
        return daemon.DaemonContext()

    def _get_options(self):
        """Return the CLI option values.

        :returns optparse.Values: The optparse.Values

        """
        options, _args = self._parser.parse_args()
        return options

    def _group_id(self, group_name):
        """Return the group name for the specified group_name.

        :param str group_name: The group to get the ID for.
        :returns: int

        """
        self._logger.debug('Fetching group id for %s', group_name)
        return grp.getgrnam(group_name).gr_gid

    def _load_configuration(self):
        """Returns the configuration as a dictionary.

        :returns: dict
        :raises: RuntimeError

        """
        yaml_document = self._read_file(self._options.config)

        # If the file is empty, raise a RuntimeError
        if not yaml_document:
            raise RuntimeError('Configuration file not not be loaded')

        # Parse the YAML document
        configuration = self._parse_yaml(yaml_document)

        # Make sure the file is parsed
        if not configuration:
            raise RuntimeError('Configuration file could not be parsed')

        # Return the configuration
        return configuration

    def _logging_config(self):
        """Return an instance of a logging_config.Logging object.

        :returns: logging_config.Logging

        """
        return logging_config.Logging(self._config.get('Logging', dict()),
                                      debug=self._options.foreground)

    def _master_control_program(self):
        """Return an instance of the MasterControlProgram.

        :returns: rejected.mcp.MasterControlProgram

        """
        return mcp.MasterControlProgram(self._config)

    def _option_parser(self):
        """Parse commandline options.

        :returns: optparse.OptionParser

        """
        usage = 'usage: %prog -c <configfile> [options]'
        version_string = '%%prog %s' % __version__
        description = 'Rejected is a RabbitMQ consumer daemon'

        # Create our parser and setup our command line options
        return optparse.OptionParser(usage=usage,
                                     version=version_string,
                                     description=description)

    def _parse_yaml(self, document):
        """Parse the YAML document returning the resulting python dictionary.

        :param str document: The YAML document
        :returns: dict

        """
        try:
            return yaml.load(document)
        except yaml.YAMLError as error:
            self._logger.error('Error parsing the YAML document: %s', error)
            return dict()

    def _prepend_python_path(self, path):  #pragma: no cover
        """Add the specified value to the python path.

        :param str path: The path to append

        """
        self._logger.debug('Prepending "%s" to the python path.', path)
        sys.path.insert(0, path)

    def _read_file(self, filename):  #pragma: no cover
        """Read the contents of filename returning the value.

        :param str filename: The file to read
        :returns: str

        """
        try:
            with open(filename, 'r') as handle:
                return handle.read()
        except IOError as error:
            self._logger.error('Error reading %s: %s', filename, error)
            return None

    def _run(self):
        """Continue the run process blocking on MasterControlProgram.run"""
        # Setup the master control program
        self._mcp = self._master_control_program()

        # This will block until it has completed
        self._mcp.run()

        # Let the debugger know we are done
        self._logger.debug('Exiting CLI._run')

    def _run_daemon(self):
        """Run the process as a daemon."""
        # Create a daemon context object
        self._context = self._daemon_context()

        # Setup the signal map for handling clean shutdowns
        self._setup_signal_map()

        # Change the group the application is running as if configured for it
        if self._config.get('group'):
            self._change_group(self._config['group'])

        # Change the user rejected runs as if the OS supports it & is configured
        if pwd and self._config.get('user'):
            self._change_user(self._config['user'])

        # Kick of the spawned child process
        with self._context:

            # This will block on the MCP
            self._run()

    def _setup_logging(self):
        """Setup the logging-config object with the logging section of the
        configuration.

        """
        self._logging.setup()

    def _setup_signal_map(self):
        """Setup the signal map to handle signals when running for clean
        shutdowns when running as a daemon

        """
        self._context.signal_map = self._signal_map

    def _setup_signals(self):  # pragma: no cover
        """Setup the signal handler if we're running interactively."""
        for signal_num in self._signal_map:
            signal.signal(signal_num, self._signal_map[signal_num])

    def _user_id(self, username):
        """Return the user_id fro the specified user name.

        :param str username: The user to find the uid for.
        :returns: int

        """
        self._logger.debug('Fetching user id for %s', username)
        return pwd.getpwnam(username).pw_uid

    def _validate_config(self, option, opt_str, value, parser, *args, **kwargs):
        """Validate the config value from the command line options parser.

        :param optparse.Option option: Option instance calling the callback
        :param str opt_str: String representation of the option
        :param str value: The value specified
        :param optparse.OptionParser parser: The parser
        :param list args: Additional arguments
        :param dict kwargs: Additional kwargs

        :raises: optparse.OptionValueError

        """
        self._logger.debug('Validation: %r %r %r %r %r %r',
                           option, opt_str, value, parser, args, kwargs)

        # Make sure the value is specified
        if not value:
            raise optparse.OptionValueError('Configuration file not specified.')

        # Make sure the file exists to load
        if not path.exists(value):
            raise optparse.OptionValueError('Configuration file not found: %s' %
                                            value)

        # We made it this far set the option value
        setattr(parser.values, option.dest, value)

    def run(self):
        """Run the rejected Application.

        """
        # If the app was invoked to specified to prepend the path, do so now
        if self._options.prepend_path:
            self._prepend_python_path(self._options.prepend_path)

        # No need to daemonize
        if self._options.foreground:
            # Setup signals so the apps shuts down cleanly when in foreground
            self._setup_signals()
            return self._run()

        # Run the daemonized instance
        return self._run_daemon()

    def stop(self):
        """Stop the CLI Application"""
        self._logger.info('Shutdown request received')
        if self._mcp:
            self._logger.debug('Letting MCP know to shutdown')
            self._mcp.shutdown()


def _error(message):  #pragma: no cover
    """Write out an error and call sys.exit indicating an error.

    :param str message: The message to print out

    """
    sys.stderr.write('ERROR: %s\n' % message)
    sys.exit(1)


def _get_cli():  #pragma: no cover
    """Return an instance of the CLI object

    :returns: CLI

    """
    return CLI()


def main():
    """Called when invoking the command line script."""
    cli_object = _get_cli()
    try:
        cli_object.run()
    except KeyboardInterrupt:
        cli_object.stop()
    except RuntimeError as error:
        _error(str(error))


if __name__ == '__main__':
    main()
