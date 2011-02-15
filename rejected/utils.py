# -*- coding: utf-8 -*-
import grp
import logging
import os
import pwd
import signal
import socket
import sys
import time
import yaml
from functools import wraps

# For log formatting
try:
    import curses
except ImportError:
    curses = None

children = []  # Global list of children to shutdown on shutdown
pidfiles = []  # Global list of pidfiles

logger = logging.getLogger('rejected')
logger.setLevel(logging.WARN)


def application_name():
    """
    Returns the currently running application name
    """
    return os.path.split(sys.argv[0])[1]


def hostname():
    """
    Returns the hostname for the machine we're running on
    """
    return socket.gethostname().split(".")[0]


def daemonize(pidfile=None, user=None, group=None):
    """
    Fork the Python app into the background and close the appropriate
    "files" to detach from console. Based off of code by Jürgen Hermann and
    http://code.activestate.com/recipes/66012/

    Parameters:

    * pidfile: Pass in a file to write the pid, defaults to
               /tmp/current_process_name-pid_number.pid
    * user: User to run as, defaults to current user
    * group: Group to run as, defaults to current group
    """

    # Flush stdout and stderr
    sys.stdout.flush()
    sys.stderr.flush()

    # Fork off from the process that called us
    pid = os.fork()
    if pid > 0:
        sys.exit(0)

    # Second fork to put into daemon mode
    pid = os.fork()
    if pid > 0:
        # exit from second parent, print eventual PID before
        sys.stdout.write('%s: started - PID # %d\n' % (application_name(),
                                                       pid))

        # Setup a pidfile if we weren't passed one
        pidfile = pidfile or \
                  os.path.normpath('/tmp/%s-%i.pid' % (application_name(),
                                                       pid))

        # Write a pidfile out
        with open(pidfile, 'w') as f:
            f.write('%i\n' % pid)

        # Append the pidfile to our global pidfile list
        global pidfiles
        pidfiles.append(pidfile)

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
    logging.debug("Changing the running user to %s:%s" % (user, group))

    if user:
        uid = pwd.getpwnam(user).pw_uid
        # Make sure we're not trying to switch to the same user
        if uid != os.geteuid():
            os.setuid(uid)

    # Set the running group
    if group:
        gid = grp.getgrnam(group).gr_gid
        # Make sure we're not already in the right group
        if gid != os.getegid():
            os.setgid(gid)

    return True


def load_configuration_file(config_file):
    """
    Load our YAML configuration file from disk or error out
    if not found or parsable
    """

    try:
        with file(config_file, 'r') as f:
            config = yaml.load(f)

    except IOError as err:
        sys.stderr.write('Configuration file not found "%s"\n' % config_file)
        sys.exit(1)

    except yaml.scanner.ScannerError as err:
        sys.stderr.write('Invalid configuration file "%s":\n%s\n' % \
                         (config_file, err))
        sys.exit(1)

    return config


def log_method_call(method):
    """
    Logging decorator to send the method and arguments to logger.debug
    """
    @wraps(method)
    def debug_log(*args, **kwargs):

        if logger.getEffectiveLevel() == logging.DEBUG:

            # Get the class name of what was passed to us
            try:
                class_name = args[0].__class__.__name__
            except AttributeError:
                class_name = 'Unknown'
            except IndexError:
                class_name = 'Unknown'

            # Build a list of arguments to send to the logger
            log_args = list()
            for x in xrange(1, len(args)):
                log_args.append(args[x])
            if len(kwargs) > 1:
                log_args.append(kwargs)

            # If we have arguments, log them as well, otherwise just the method
            if log_args:
                logger.debug("%s.%s(%r) Called", class_name, method.__name__,
                             log_args)
            else:
                logger.debug("%s.%s() Called", class_name, method.__name__)

        # Actually execute the method
        return method(*args, **kwargs)

    # Return the debug_log function to the python stack for execution
    return debug_log


class FormatOutput(logging.Formatter):
    """
    Creates a colorized output format for logging that helps provide easier
    context in debugging
    """
    def __init__(self, *args, **kwargs):
        logging.Formatter.__init__(self, *args, **kwargs)
        color = curses.tigetstr("setaf") or curses.tigetstr("setf") or ""
        self._level = {logging.DEBUG: curses.tparm(color, 6),
                       logging.ERROR: curses.tparm(color, 1),
                       logging.INFO: curses.tparm(color, 2),
                       logging.WARNING: curses.tparm(color, 3)}
        self._reset = curses.tigetstr("sgr0")
        self._class = curses.tparm(color, 4)
        self._args = curses.tparm(color, 2)
        elements = ['%(levelname)-8s', '%(asctime)-24s', '#%(process)s']
        self._prefix = '[%s]' % ' '.join(elements)

    def format(self, record):
        #timestamp = datetime.datetime.fromtimestamp(record.created)
        #record.timestamp = timestamp.isoformat(' ')

        message = record.getMessage()
        record.asctime = self.formatTime(record)
        if message[-6:] == 'Called':
            parts = message.split(' ')
            call = parts[0].split('.')
            end_method = call[1].find('(')
            start_content = message.find('(') + 1
            message = '%s%s.%s%s(%s%s%s)' % (self._class,
                                             call[0],
                                             call[1][:end_method],
                                             self._reset,
                                             self._args,
                                             message[start_content:-8],
                                             self._reset)

        output = "%s%s%s %s" % (self._level.get(record.levelno,
                                                self._reset),
                                self._prefix % record.__dict__,
                                self._reset,
                                message)
        if record.exc_info and not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)

        if record.exc_text:
            output = output.rstrip() + "\n    " + record.exc_text
        return output


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
    # Set logging levels dictionary
    logging_levels = {'debug':    logging.DEBUG,
                      'info':     logging.INFO,
                      'warning':  logging.WARNING,
                      'error':    logging.ERROR,
                      'critical': logging.CRITICAL}

    # Get the logging value from the dictionary
    logging_level = config['level']

    if debug:

        # Override the logging level to use debug mode
        config['level'] = logging.DEBUG

        # If we have specified a file, remove it so logging info goes to stdout
        if 'filename' in config:
            del config['filename']

    else:

        # Use the configuration option for logging
        config['level'] = logging_levels.get(config['level'], logging.NOTSET)

    # Pass in our logging config
    logging.basicConfig(**config)
    logging.getLogger('rejected').setLevel(config['level'])

    logging.getLogger('pika').setLevel(config['level'])

    logging.info('Log level set to %s' % logging_level)

    # Get the default logger
    default_logger = logging.getLogger('')

    # Remove the default stream handler
    stream_handler = None
    for handler in default_logger.handlers:
        if isinstance(handler, logging.StreamHandler):
            stream_handler = handler
            break

    # Use colorized output
    if curses and debug and stream_handler and sys.stderr.isatty():
        curses.setupterm()
        if curses.tigetnum("colors") > 0:
            stream_handler.setFormatter(FormatOutput())

    # If we have supported handler
    elif 'handler' in config:

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
                default_logger.addHandler(syslog)

                # Remove the StreamHandler
                if stream_handler:
                    default_logger.removeHandler(stream_handler)
            else:
                logging.error('%s:Invalid facility, syslog logging aborted',
                              application_name())




@log_method_call
def shutdown():
    """
    Cleanly shutdown the application
    """
    # Tell all our children to stop
    for child in children:
        child.stop()

    # Remove our pidfiles
    for pidfile in pidfiles:
        if os.path.isfile(pidfile):
            os.unlink(pidfile)


def setup_signals():
    """
    Setup the signals we want to be notified on
    """
    signal.signal(signal.SIGTERM, _shutdown_signal_handler)
    signal.signal(signal.SIGHUP, _rehash_signal_handler)


def _shutdown_signal_handler(signum, frame):
    """
    Called on SIGTERM to shutdown the application
    """
    logging.info("SIGTERM received, shutting down")
    shutdown()


def _rehash_signal_handler(signum, frame):
    """
    Would be cool to handle this and effect changes in the config
    """
    logging.info("SIGHUP received, rehashing config")

