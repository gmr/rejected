"""
Functions used mainly in startup and shutdown of rejected

"""
import logging
from os import path
from socket import gethostname
import sys
import traceback


def application_name():
    """Returns the currently running application name

    :returns: str

    """
    return path.split(sys.argv[0])[1]


def hostname():
    """Returns the hostname for the machine we're running on

    :returns: str

    """
    return gethostname().split(".")[0]


def import_namespaced_class(namespaced_class):
    """Pass in a string in the format of foo.Bar, foo.bar.Baz, foo.bar.baz.Qux
    and it will return a handle to the class

    :param str namespaced_class: The namespaced class
    :returns: class

    """
    # Split up our string containing the import and class
    parts = namespaced_class.split('.')

    # Build our strings for the import name and the class name
    import_name = '.'.join(parts[0:-1])
    class_name = parts[-1]

    # get the handle to the class for the given import
    class_handle = getattr(__import__(import_name, fromlist=class_name),
                           class_name)

    # Return the class handle
    return class_handle


def show_frames():
    """Log the current framestack to _LOGGER.info, called from SIG_USER1"""
    logger = logging.getLogger(__name__)
    for stack in sys._current_frames().items():
        for filename, lineno, name, line in traceback.extract_stack(stack[1]):
            logger.info('  File: "%s", line %d, in %s', filename, lineno, name)
            if line:
                logger.info("    %s", line.strip())
