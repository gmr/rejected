#!/usr/bin/env python
"""
Rejected AMQP Consumer Framework

A multi-threaded consumer application and how!

Copyright 2009 Insider Guides, Inc.

2009-06-09 Gavin M. Roy <gmr@myyearbook.com>
"""

import amqplib.client_0_8 as amqp
import daemon
import logging
import sys
import optparse
import os
import signal
import threading
import time
import yaml

from monitors import alice

version = '0.1'

def consume():

    return True
        
def shutdown(signum = 0, frame = None):
    logging.info('Graceful shutdown initiated.')
    os._exit(signum)

def main():

    usage = "usage: %prog [options]"
    version_string = "%%prog %s" % version
    description = "rejected.py consumer daemon"
    
    # Create our parser and setup our command line options
    parser = optparse.OptionParser(usage=usage,
                         version=version_string,
                         description=description)

    parser.add_option("-c", "--config", 
                        action="store", type="string", default="rejected.yaml", 
                        help="Specify the configuration file to load.")

    parser.add_option("-b", "--binding", 
                        action="store", dest="binding",
                        help="Binding name to use to when used in conjunction \
                        with the broker and single settings. All other \
                        configuration data will be derived from the\
                        combination of the broker and queue settings.")

    parser.add_option("-C", "--connection",
                        action="store", dest="connection", 
                        help="Specify the broker connection name as defined in the \
                        configuration file. Used in conjunction with the \
                        single and binding command line options. All other \
                        configuration data such as the user credentials and \
                        exchange will be derived from the configuration file.")     

    parser.add_option("-f", "--foreground",
                        action="store_true", dest="foreground", default=False,
                        help="Do not fork and stay in foreground")                                                                                                                                 

    parser.add_option("-s", "--single",
                        action="store_true", dest="single_thread", 
                        default=False,
                        help="Only runs with one thread worker, requires setting \
                        the broker and queue to subscribe to.    All other \
                        configuration data will be derived from the \
                        configuration settings matching the broker and queue.")     

    parser.add_option("-v", "--verbose",
                        action="store_true", dest="verbose", default=False,
                        help="use debug to stdout instead of logging settings")
    
    # Parse our options and arguments                                                                        
    options, args = parser.parse_args()
     
    # Load the Configuration file
    try:
            stream = file(options.config, 'r')
            config = yaml.load(stream)
    except:
            print "\nError: Invalid or missing configuration file \"%s\"\n" % options.config
            sys.exit(1)
    
    # Set logging levels dictionary
    logging_levels = { 
                        'debug':    logging.DEBUG,
                        'info':     logging.INFO,
                        'warning':  logging.WARNING,
                        'error':    logging.ERROR,
                        'critical': logging.CRITICAL
                     }
    
    # Get the logging value from the dictionary
    logging_level = config['Logging']['level']
    config['Logging']['level'] = logging_levels.get( 
                                                    config['Logging']['level'], 
                                                    logging.NOTSET 
                                                   )

    # If the user says verbose overwrite the settings.
    if options.verbose is True:
    
        # Set the debugging level to verbose
        config['Logging']['level'] = logging.DEBUG
        
        # If we have specified a file, remove it so logging info goes to stdout
        if config['Logging'].has_key('filename') == True:
            del config['Logging']['filename']

    else:
        # Build a specific path to our log file
        if config['Logging'].has_key('filename') == True:
            config['Logging']['filename'] = "%s/%s/%s" % ( 
                config['Location']['base'], 
                config['Location']['logs'], 
                config['Logging']['filename'] )
        
    # Pass in our logging config 
    logging.basicConfig(**config['Logging'])
    logging.info('Log level set to %s' % logging_level)

    # Make sure if we specified single thread that we specified connection and binding
    if options.single_thread == True:
        if not options.connection or not options.binding:
            print "\nError: Specify the connection and binding when using single threaded.\n"
            parser.print_help()
            sys.exit(1)

    # Fork our process to detach if not told to stay in foreground
    if options.foreground is False:
        try:
            pid = os.fork()
            if pid > 0:
                logging.info('Parent process ending.')
                sys.exit(0)                        
        except OSError, e:
            sys.stderr.write("Could not fork: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)
        
        # Second fork to put into daemon mode
        try: 
            pid = os.fork() 
            if pid > 0:
                # exit from second parent, print eventual PID before
                print 'rejected.py daemon has started - PID # %d.' % pid
                logging.info('Child forked as PID # %d' % pid)
                sys.exit(0) 
        except OSError, e: 
            sys.stderr.write("Could not fork: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)
        
        # Let the debugging person know we've forked
        logging.debug('After child fork')
        
        # Detach from parent environment
        os.chdir(config['Location']['base']) 
        os.setsid()
        os.umask(0) 

        # Close stdin            
        sys.stdin.close()
        
        # Redirect stdout, stderr
        sys.stdout = open('%s/%s/stdout.log' % ( config['Location']['base'], 
                                                 config['Location']['logs']), 
                                                 'w')
        sys.stderr = open('%s/%s/stderr.log' % ( config['Location']['base'], 
                                                 config['Location']['logs']), 
                                                 'w')
                                                 
    # Set our signal handler so we can gracefully shutdown
    signal.signal(signal.SIGTERM, shutdown)

    while 1:
        try:
            time.sleep(1000)
        except (KeyboardInterrupt, SystemExit):
            shutdown()
        
# Only execute the code if invoked as an application
if __name__ == '__main__':
    main()
