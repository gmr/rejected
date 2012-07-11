"""
OS Level controlling class invokes startup, shutdown and handles signals.

"""
import clihelper
import logging
import sys
import time

from rejected import mcp
from rejected import __version__

logger = logging.getLogger(__name__)


class Controller(clihelper.Controller):
    """Rejected Controller application that invokes the MCP and handles all
    of the OS level concerns.

    """
    def _master_control_program(self):
        """Return an instance of the MasterControlProgram.

        :rtype: rejected.mcp.MasterControlProgram

        """
        return mcp.MasterControlProgram(self._get_application_config(),
                                        consumer=self._options.consumer)

    def _prepend_python_path(self, path):  #pragma: no cover
        """Add the specified value to the python path.

        :param str path: The path to append

        """
        logger.debug('Prepending "%s" to the python path.', path)
        sys.path.insert(0, path)

    def _setup(self):
        """Continue the run process blocking on MasterControlProgram.run"""
        # If the app was invoked to specified to prepend the path, do so now
        if self._options.prepend_path:
            self._prepend_python_path(self._options.prepend_path)

    def run(self):
        """Run the rejected Application"""
        self._setup()

        # Setup the master control program
        self._mcp = self._master_control_program()

        # Block on the MCP run
        self._mcp.run()

    def _cleanup(self):
        """Stop the child processes and threads"""
        logger.info('Shutdown cleanup request received')
        if hasattr(self, '_mcp') and self._mcp.is_running:
            self._mcp.stop_processes()
            time.sleep(1)
            del self._mcp


def _cli_options(parser):
    """Add options to the parser

    :param optparse.OptionParser parser: The option parser to add options to

    """
    parser.add_option('-o', '--only',
                      action='store',
                      default=None,
                      dest='consumer',
                      help='Only run the consumer specified')
    parser.add_option('-p', '--prepend-path',
                      action='store',
                      default=None,
                      dest='prepend_path',
                      help='Prepend the python path with the value.')

def main():
    """Called when invoking the command line script."""
    clihelper.setup('rejected', 'RabbitMQ consumer framework', __version__)
    clihelper.run(Controller, _cli_options)


if __name__ == '__main__':
    main()
