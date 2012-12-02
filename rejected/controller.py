"""
OS Level controlling class invokes startup, shutdown and handles signals.

"""
import clihelper
import logging
import signal
import sys

from rejected import mcp
from rejected import __version__

LOGGER = logging.getLogger(__name__)


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
        LOGGER.debug('Prepending "%s" to the python path.', path)
        sys.path.insert(0, path)

    def _setup(self):
        """Continue the run process blocking on MasterControlProgram.run"""
        # If the app was invoked to specified to prepend the path, do so now
        if self._options.prepend_path:
            self._prepend_python_path(self._options.prepend_path)

    def _shutdown(self):
        """Shutdown the MCP and child processes cleanly"""
        self._set_state(self._STATE_SHUTTING_DOWN)
        signal.setitimer(signal.ITIMER_PROF, 0, 0)
        self._mcp.stop_processes()
        if self._mcp.is_running:
            LOGGER.debug('Waiting up to 3 seconds for MCP to shut things down')
            signal.setitimer(signal.ITIMER_REAL, 3, 0)
            signal.pause()

        # Force MCP to stop
        if self._mcp.is_running:
            LOGGER.warning('MCP is taking too long, requesting process kills')
            self._mcp.stop_processes()
            del self._mcp
        else:
            LOGGER.info('MCP exited cleanly')

        # Change our state
        self._shutdown_complete()

    def run(self):
        """Run the rejected Application"""
        self._setup()
        self._mcp = self._master_control_program()
        self._mcp.run()


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
