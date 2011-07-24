# -*- coding: utf-8 -*-
"""
Functions used mainly in startup and shutdown of rejected

"""
import logging
import os
import os.path
import signal
import sys
import traceback
import yaml

# Windows doesn't support this
try:
    import pwd
except ImportError:
    pwd = None

from functools import wraps
from socket import gethostname

# Logger
LOGGER = logging.getLogger('rejected.utils')

# Callback handlers
_SHUTDOWN_HANDLER = None

# Application state for shutdown
RUNNING = False

# Default group for files
_DEFAULT_GID = 1

# Set logging levels dictionary
LEVELS = {'debug':    logging.DEBUG,
          'info':     logging.INFO,
          'warning':  logging.WARNING,
          'error':    logging.ERROR,
          'critical': logging.CRITICAL}


def log_method_call(method):
    """
    Logging decorator to send the method and arguments to logging.debug
    """
    @wraps(method)
    def debug_log(*args, **kwargs):

        if logging.getLogger('').getEffectiveLevel() == logging.DEBUG:

            # Get the class name of what was passed to us
            try:
                class_name = args[0].__class__.__name__
            except AttributeError:
                class_name = 'Unknown'
            except IndexError:
                class_name = 'Unknown'

            # Build a list of arguments to send to the logging
            log_args = list()
            for x in xrange(1, len(args)):
                log_args.append(args[x])
            if len(kwargs) > 1:
                log_args.append(kwargs)

            # If we have arguments, log them as well, otherwise just the method
            if log_args:
                LOGGER.debug("%s.%s(%r) Called", class_name, method.__name__,
                             log_args)
            else:
                LOGGER.debug("%s.%s() Called", class_name, method.__name__)

        # Actually execute the method
        return method(*args, **kwargs)

    # Return the debug_log function to the python stack for execution
    return debug_log


def application_name():
    """
    Returns the currently running application name
    """
    return os.path.split(sys.argv[0])[1]


def hostname():
    """
    Returns the hostname for the machine we're running on
    """
    return gethostname().split(".")[0]


def daemonize(pidfile=None, user=None):
    """Fork the Python app into the background and close the appropriate
    "files" to detach from console. Based off of code by Jürgen Hermann and
    http://code.activestate.com/recipes/66012/

    :param pidfile: Filename to write the pidfile as
    :type pidfile: str
    :param user: Username to run, defaults as current user
    :type user: str
    :returns: bool

    """
    # Flush stdout and stderr
    sys.stdout.flush()
    sys.stderr.flush()

    # Get the user id if we have a user set
    if pwd and user:
        uid = pwd.getpwnam(user).pw_uid
    else:
        uid = -1

    # Fork off from the process that called us
    pid = os.fork()
    if pid > 0:
        sys.exit(0)

    # Second fork to put into daemon mode
    pid = os.fork()
    if pid > 0:
        # Setup a pidfile if we weren't passed one
        pidfile = pidfile or \
                  os.path.normpath('/tmp/%s-%i.pid' % (application_name(),
                                                       pid))

        # Write the pidfile out
        with open(pidfile, 'w') as f:
            f.write('%i\n' % pid)

            # If we have uid or gid change the uid for the file
            if uid > -1:
                os.fchown(f.fileno(), uid, _DEFAULT_GID)

        # Exit the parent process
        sys.exit(0)

    # Detach from parent environment
    os.chdir(os.path.normpath('/'))
    os.umask(0)
    os.setsid()

    # Redirect stdout, stderr, stdin
    si = file('/dev/null', 'r')
    so = file('/dev/null', 'a+')
    se = file('/dev/null', 'a+', 0)
    os.dup2(si.fileno(), sys.stdin.fileno())
    os.dup2(so.fileno(), sys.stdout.fileno())
    os.dup2(se.fileno(), sys.stderr.fileno())

    # Set the running user
    if  user:
        logging.info("Changing the running user to %s", user)

    # If we have a uid and it's not for the running user
    if uid > -1 and uid != os.geteuid():
            try:
                os.seteuid(uid)
                LOGGER.debug("User changed to %s(%i)", user, uid)
            except OSError as e:
                LOGGER.error("Could not set the user: %s", str(e))

    return True


def load_configuration_file(config_file):
    """ Load our YAML configuration file from disk or error out
    if not found or parsable

    :param config_file: Path to the configuration file we want to load
    :type config_file: str
    :returns: dict

    """
    try:
        with file(config_file, 'r') as f:
            config = yaml.load(f)

    except IOError:
        sys.stderr.write('Configuration file not found "%s"\n' % config_file)
        sys.exit(1)

    except yaml.scanner.ScannerError as err:
        sys.stderr.write('Invalid configuration file "%s":\n%s\n' % \
                         (config_file, err))
        sys.exit(1)

    return config


def setup_logger(name, level):
    """Setup an individual logger at a specified level.

    :param name: Logger name
    :type name: str
    :param level: Logging level
    :type level: logging.LEVEL

    """
    logger = logging.getLogger(name)
    logger.setLevel(level)


def setup_loggers(loggers, level):
    """Iterate through our loggers if specified and set their levels.

    :param loggers: list of loggers
    :type loggers: list
    :param level: default logging level if not specified
    :type level: logging.LEVEL

    """
    # It's possible/probable there was nothing there.
    if not loggers:
        return

    # Apply the logging level to the loggers we have specifically set
    for logger in loggers:
        # If it's a list expect a logger_name, level string format
        if isinstance(logger, list):
            level_ = LEVELS.get(logger[1], logging.NOTSET)
            setup_logger(logger[0], level_)

        # Otherwise we just just want a specific logger at the default level
        elif isinstance(logger, str):
            setup_logger(logger, level)


def setup_logging(config, debug=False):
    """
    Setup the logging module to respect our configuration values.
    Expects a dictionary called config with the following parameters

    * directory:   Optional log file output directory
    * filename:    Optional filename, not needed for syslog
    * format:      Format for non-debug mode
    * level:       One of debug, error, warning, info
    * handler:     Optional handler
    * syslog:      If handler == syslog, parameters for syslog
      * address:   Syslog address
      * facility:  Syslog facility

    Passing in debug=True will disable any log output to anything but stdout
    and will set the log level to debug regardless of the config.
    """
    if debug:

        # Override the logging level to use debug mode
        config['level'] = 'debug'

        # If we have specified a file, remove it so logging info goes to stdout
        if 'filename' in config:
            del config['filename']

    # Get the logging value from the dictionary
    logging_level = config['level']

    # Use the configuration option for logging
    config['level'] = LEVELS.get(config['level'], logging.NOTSET)

    # Pass in our logging config
    logging.basicConfig(**config)

    # Get the default logger
    default_logging = logging.getLogger()

    # Setup loggers
    setup_loggers(config.get('loggers'), config['level'])

    # Remove the default stream handler
    stream_handler = None
    for handler in default_logging.handlers:
        if isinstance(handler, logging.StreamHandler):
            stream_handler = handler
            break

    # If we have supported handler
    if 'handler' in config:

        # If we want to syslog
        if config['handler'] == 'syslog':

            facility = config['syslog']['facility']
            import logging.handlers as handlers

            # If we didn't type in the facility name
            if facility in handlers.SysLogHandler.facility_names:

                # Create the syslog handler
                address = config['syslog']['address']
                facility = handlers.SysLogHandler.facility_names[facility]
                syslog = handlers.SysLogHandler(address=address,
                                                facility=facility)
                # Add the handler
                default_logging.addHandler(syslog)

                # Remove the StreamHandler
                if stream_handler and not debug:
                    default_logging.removeHandler(stream_handler)
            else:
                logging.error('%s:Invalid facility, syslog logging aborted',
                              application_name())


def shutdown(signum, frame):
    """Cleanly shutdown the application

    :param signum: Signal passed in
    :type signum: int
    :param frame: The frame
    :type frame: frame or None

    """
    global _SHUTDOWN_HANDLER
    LOGGER.info('SIGTERM received %i:%r', signum, frame)

    # Tell all our children to stop
    if _SHUTDOWN_HANDLER:
        LOGGER.debug('Calling shutdown handler: %r', _SHUTDOWN_HANDLER)
        _SHUTDOWN_HANDLER()
    else:
        LOGGER.info('No signal handler defined')

def setup_signals():
    """Setup the signals we want to be notified on"""
    LOGGER.info('Setting up signals for PID %i', os.getpid())

    # Set our signal handler so we can gracefully shutdown
    signal.signal(signal.SIGTERM, shutdown)

    # Now set all the others
    #for num in [signal.SIGQUIT, signal.SIGUSR1, signal.SIGUSR2, signal.SIGHUP]:
    #    signal.signal(num, _signal_handler)


def _signal_handler(signum, frame):
    """Call the generic sighandler

    :param signum: Signal passed in
    :type signum: int
    :param frame: The frame
    :type frame: uhh
    """
    LOGGER.info("Signal received: %i, %i", signum, frame)
    if signum == signal.SIGHUP:
        if REHASH_HANDLER:
            REHASH_HANDLER()
    elif signum == signal.SIGUSR1:
        show_frames()

def import_namespaced_class(path):
    """
    Pass in a string in the format of foo.Bar, foo.bar.Baz, foo.bar.baz.Qux
    and it will return a handle to the class
    """
    # Split up our string containing the import and class
    parts = path.split('.')

    # Build our strings for the import name and the class name
    import_name = '.'.join(parts[0:-1])
    class_name = parts[-1]

    # get the handle to the class for the given import
    class_handle = getattr(__import__(import_name, fromlist=class_name),
                           class_name)

    # Return the class handle
    return class_handle

def show_frames():
    for threadId, stack in sys._current_frames().items():
        LOGGER.info("# ThreadID: %s", threadId)
        for filename, lineno, name, line in traceback.extract_stack(stack):
            LOGGER.info('  File: "%s", line %d, in %s', filename, lineno, name)
            if line:
                LOGGER.info("    %s", line.strip())

def shutdown_handler(handler):
    global _SHUTDOWN_HANDLER
    _SHUTDOWN_HANDLER = handler
    LOGGER.debug('Shutdown handler set to %r', handler)
