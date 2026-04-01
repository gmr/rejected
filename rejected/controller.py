"""
OS Level controlling class: CLI entry point, signal handling, MCP lifecycle.
"""

import argparse
import logging
import logging.config
import os
import signal
import sys

try:
    import sentry_sdk
    import sentry_sdk.integrations.logging
except ImportError:
    sentry_sdk = None

from . import __version__, mcp
from . import config as config_module

LOGGER = logging.getLogger(__name__)


class Controller:
    """Manages the MCP lifecycle and OS-level signal handling."""

    def __init__(self, args: argparse.Namespace, cfg: config_module.Config):
        self.args = args
        self.config = cfg
        self._mcp = None
        self._reload_requested = False
        self._shutdown_requested = False
        self._sentry_client = False
        if sentry_sdk and cfg.sentry_dsn:
            init_kwargs = {
                'dsn': cfg.sentry_dsn,
                'send_default_pii': False,
                'integrations': [
                    sentry_sdk.integrations.logging.LoggingIntegration(
                        level=None, event_level=None
                    )
                ],
            }
            if os.environ.get('ENVIRONMENT'):
                init_kwargs['environment'] = os.environ['ENVIRONMENT']
            sentry_sdk.init(**init_kwargs)
            self._sentry_client = True

    def run(self):
        """Run the application: set up signals, start MCP, block until done.

        Loops on SIGHUP to reload config and restart consumers without
        dropping the process.
        """
        self._setup_signals()
        if self.args.prepend_path:
            sys.path.insert(0, self.args.prepend_path)

        while not self._shutdown_requested:
            self._reload_requested = False
            self._mcp = mcp.MasterControlProgram(
                self.config,
                consumer=self.args.consumer,
                profile=self.args.profile,
                quantity=self.args.quantity,
            )
            try:
                self._mcp.run()
            except KeyboardInterrupt:
                LOGGER.info('Caught CTRL-C, shutting down')
                break
            except Exception:
                exc_info = sys.exc_info()
                if self._sentry_client:
                    LOGGER.debug('Sending exception to sentry')
                    sentry_sdk.capture_exception(exc_info)
                raise

            if not self._reload_requested:
                break

            LOGGER.info('Reloading configuration from %s', self.args.config)
            try:
                self.config = config_module.load(self.args.config)
                if self.config.logging:
                    logging.config.dictConfig(self.config.logging)
            except (FileNotFoundError, ValueError) as exc:
                LOGGER.error(
                    'Failed to reload configuration: %s — restarting with '
                    'previous config',
                    exc,
                )

    def _setup_signals(self):
        signal.signal(signal.SIGHUP, self._on_sighup)
        signal.signal(signal.SIGTERM, self._on_sigterm)

    def _on_sighup(self, _signum, _frame):
        LOGGER.info('Received SIGHUP — reloading configuration')
        self._reload_requested = True
        if self._mcp:
            self._mcp.stop_processes()

    def _on_sigterm(self, _signum, _frame):
        LOGGER.info('Received SIGTERM, initiating shutdown')
        self._shutdown_requested = True
        if self._mcp:
            self._mcp.stop_processes()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog='rejected',
        description='RabbitMQ consumer framework',
    )
    parser.add_argument(
        '-c',
        '--config',
        required=True,
        dest='config',
        metavar='FILE',
        help='Path to the configuration file (YAML or TOML)',
    )
    parser.add_argument(
        '-P',
        '--profile',
        default=None,
        dest='profile',
        metavar='DIR',
        help='Profile consumer modules, writing output to this directory',
    )
    parser.add_argument(
        '-o',
        '--only',
        default=None,
        dest='consumer',
        metavar='CONSUMER',
        help='Only run the named consumer',
    )
    parser.add_argument(
        '-p',
        '--prepend-path',
        default=None,
        dest='prepend_path',
        metavar='PATH',
        help='Prepend PATH to sys.path before importing consumers',
    )
    parser.add_argument(
        '-q',
        '--qty',
        type=int,
        default=None,
        dest='quantity',
        metavar='N',
        help='Override the consumer quantity (use with -o)',
    )
    parser.add_argument(
        '--version',
        action='version',
        version=f'%(prog)s {__version__}',
    )
    return parser


def main():
    """CLI entry point."""
    parser = _build_parser()
    args = parser.parse_args()

    try:
        cfg = config_module.load(args.config)
    except (FileNotFoundError, ValueError) as exc:
        sys.exit(f'Error: {exc}')

    if args.consumer is not None and args.consumer not in cfg.consumers:
        parser.error(f'Unknown consumer: {args.consumer}')
    if args.quantity is not None and args.consumer is None:
        parser.error('--qty requires --only')

    try:
        if cfg.logging:
            logging.config.dictConfig(cfg.logging)
        else:
            logging.basicConfig(
                level=logging.INFO,
                format='%(levelname)-8s %(name)s: %(message)s',
            )
    except (ValueError, TypeError, AttributeError, ImportError) as exc:
        sys.exit(f'Error: invalid logging configuration: {exc}')

    ctrl = Controller(args, cfg)
    ctrl.run()


if __name__ == '__main__':
    main()
