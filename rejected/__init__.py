# -*- coding: utf-8 -*-
"""
rejected main functional method, responsible for starting the application and
coordinating the actions it needs to take
"""

import optparse
import sys
import rejected.mcp as mcp
import rejected.utils as utils

__version__ = '2.0.0'


def main(*args):

    # Setup optparse
    usage = "usage: %prog -c <configfile> [options]"
    version_string = "%%prog %s" % __version__
    description = "rejected is a consumer daemon and development framework"

    # Create our parser and setup our command line options
    parser = optparse.OptionParser(usage=usage, version=version_string,
                                   description=description)

    parser.add_option("-c", "--config",
                      action="store", dest="config",
                      help="Specify the configuration file for use")

    parser.add_option("-f", "--foreground",
                      action="store_true", dest="foreground", default=False,
                      help="Run interactively in console")

    # Parse our options and arguments
    options, args = parser.parse_args()

    # No configuration file?
    if options.config is None:
        sys.stderr.write('\nERROR: Missing configuration file\n\n')
        parser.print_help()
        sys.exit(1)

    # Load in the YAML config file
    config = utils.load_configuration_file(options.config)

    # Setup our logging
    utils.setup_logging(config['Logging'], options.foreground)

    # Setup our signal handlers
    utils.setup_signals()

    # Daemonize if we need to
    if not options.foreground:
        try:
            utils.daemonize()
        except OSError as e:
            sys.stderr.write('Could not daemonize: %d (%s)\n' % (e.errno, e.strerror))
            return e.errno

    m = mcp.MCP(config)
    utils.children.append(m)
    try:
        m.start()
    except KeyboardInterrupt:
        m.stop()

    print 'OUT'
    sys.exit(0)

    # Initialize the

    return 0
