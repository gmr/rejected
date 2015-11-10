"""
OS Level controlling class invokes startup, shutdown and handles signals.

"""
import helper
import logging
from helper import parser
import signal
import sys

from rejected import mcp

LOGGER = logging.getLogger(__name__)


class Controller(helper.Controller):
    """Rejected Controller application that invokes the MCP and handles all
    of the OS level concerns.

    """
    def __init__(self, *args, **kwargs):
        super(Controller, self).__init__(*args, **kwargs)
        self._mcp = None

    def _master_control_program(self):
        """Return an instance of the MasterControlProgram.

        :rtype: rejected.mcp.MasterControlProgram

        """
        return mcp.MasterControlProgram(self.config,
                                        consumer=self.args.consumer,
                                        profile=self.args.profile,
                                        quantity=self.args.quantity)

    @staticmethod
    def _prepend_python_path(path):  # pragma: no cover
        """Add the specified value to the python path.

        :param str path: The path to append

        """
        LOGGER.debug('Prepending "%s" to the python path.', path)
        sys.path.insert(0, path)

    def setup(self):
        """Continue the run process blocking on MasterControlProgram.run"""
        # If the app was invoked to specified to prepend the path, do so now
        if self.args.prepend_path:
            self._prepend_python_path(self.args.prepend_path)

    def stop(self):
        """Shutdown the MCP and child processes cleanly"""
        LOGGER.info('Shutting down controller')
        self.set_state(self.STATE_STOP_REQUESTED)

        # Clear out the timer
        signal.setitimer(signal.ITIMER_PROF, 0, 0)

        self._mcp.stop_processes()

        if self._mcp.is_running:
            LOGGER.info('Waiting up to 3 seconds for MCP to shut things down')
            signal.setitimer(signal.ITIMER_REAL, 3, 0)
            signal.pause()
            LOGGER.info('Post pause')

        # Force MCP to stop
        if self._mcp.is_running:
            LOGGER.warning('MCP is taking too long, requesting process kills')
            self._mcp.stop_processes()
            del self._mcp
        else:
            LOGGER.info('MCP exited cleanly')

        # Change our state
        self._stopped()
        LOGGER.info('Shutdown complete')

    def run(self):
        """Run the rejected Application"""
        self.setup()
        self._mcp = self._master_control_program()
        try:
            self._mcp.run()
        except KeyboardInterrupt:
            LOGGER.info('Caught CTRL-C, shutting down')
        if self.is_running:
            self.stop()


def add_parser_arguments():
    """Add options to the parser"""
    argparser = parser.get()
    argparser.add_argument('-P', '--profile',
                           action='store',
                           default=None,
                           dest='profile',
                           help='Profile the consumer modules, specifying '
                           'the output directory.')
    argparser.add_argument('-o', '--only',
                           action='store',
                           default=None,
                           dest='consumer',
                           help='Only run the consumer specified')
    argparser.add_argument('-p', '--prepend-path',
                           action='store',
                           default=None,
                           dest='prepend_path',
                           help='Prepend the python path with the value.')
    argparser.add_argument('-q', '--qty',
                           action='store',
                           type=int,
                           default=None,
                           dest='quantity',
                           help='Run the specified quantity of consumer '
                           'processes when used in conjunction with -o')


def main():
    """Called when invoking the command line script."""
    add_parser_arguments()
    parser.description('RabbitMQ consumer framework')
    helper.start(Controller)


if __name__ == '__main__':
    main()
