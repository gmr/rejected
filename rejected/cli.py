"""
cli.py

"""
__author__ = 'Gavin M. Roy'
__email__ = 'gmr@myyearbook.com'
__since__ = '2011-07-22'

import logging
import optparse
from os import path
import sys

from . import mcp
from . import utils
from . import version


def parse_options():
    """Parse commandline options.

    :returns: Tuple of OptionParser object, options and arguments
    """
    usage = "usage: %prog -c <configfile> [options]"
    version_string = "%%prog %s" % version
    description = "Rejected is a RabbitMQ consumer daemon"

    # Create our parser and setup our command line options
    parser = optparse.OptionParser(usage=usage,
                                   version=version_string,
                                   description=description)

    parser.add_option("-c", "--config",
                        action="store", type="string",
                        help="Specify the configuration file to load.")

    parser.add_option("-f", "--foreground",
                      action="store_true", dest="foreground", default=False,
                      help="Run in the foreground in debug mode.")

    parser.add_option("-p", "--processor-path", default="/opt/processors",
                      action="store", dest="processor_path",
                      help="Set the non-package processor path. \
                            Default: /opt/processors")

    options, arguments = parser.parse_args()
    return parser, options, arguments


def main():
    """Main commandline invocation function."""
    # Setup a logger
    logger = logging.getLogger('rejected.cli')

    # Parse our options and arguments
    parser, options, args = parse_options()

    # We need a config file or we can't proceed
    if options.config is None:
        sys.stderr.write('\nERROR: Missing configuration file\n\n')
        parser.print_help()
        sys.exit(1)

    # Insert the processor path
    sys.path.insert(0, path.normpath(options.processor_path))

    # Load the YAML file
    config = utils.load_configuration_file(options.config)

    # Set the logging module config options
    if 'Logging' in config:
        utils.setup_logging(config["Logging"], options.foreground)

    # Daemonize the agent if not in foreground mode
    if not options.foreground:
        utils.daemonize(user=config.get('user', None),
                        pidfile=config.get('pidfile', None))
    else:
        logger.info('rejected v%s has started in interactive mode', version)

    # Handle signals
    utils.setup_signals()

    # Setup the master control program
    mcp_ = mcp.MasterControlProgram(config)

    # Set the shutdown handler to mcp.MasterControlProgram.shutdown()
    utils.shutdown_handler(mcp_.shutdown)

    # Have the Master Control Process poll
    try:
        mcp_.run()
    except KeyboardInterrupt:
        # Ctrl-C was caught
        logger.info('KeyboardInterrupt caught, stopping')

        # Shutdown
        mcp_.shutdown()
