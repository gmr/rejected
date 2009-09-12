#!/usr/bin/env python
"""
Rejected AMQP Consumer Framework

A multi-threaded consumer application and how!

Created by Gavin M. Roy on 2009-09-10.
Copyright (c) 2009 Insider Guides, Inc.. All rights reserved.
"""

import amqplib.client_0_8 as amqp
import logging
import sys
import optparse
import os
import signal
import threading
import time
import yaml

from monitors import alice

# Number of seconds to sleep between polls
mcp_poll_delay = 5

version = '0.1'

class consumerThread( threading.Thread ):
    """ Consumer Class, Handles the actual AMQP work """
    
    def __init__( self, configuration, thread_name, binding_name, connect_name ):

        logging.debug('Initializing a Consumer class in thread "%s"' % thread_name)

        # Rejected full Configuration
        self.config = configuration

        # Variables held for future use
        self.binding_name = binding_name
        self.connect_name = connect_name
        self.running = True
        self.thread_name = thread_name
        self.messages_processed = 0

        # Init the Thread Object itself
        threading.Thread.__init__(self)  

    def connect( self, configuration ):
        """ Connect to an AMQP Broker  """

        logging.debug('Creating a new connection for "%s" in thread "%s"' % ( self.binding_name, self.thread_name ) )

        return amqp.Connection( host ='%s:%s' % ( configuration['host'], configuration['port'] ),
                                userid = configuration['user'], 
                                password = configuration['pass'], 
                                ssl = configuration['ssl'],
                                virtual_host = configuration['vhost'] )

    def getInformation(self):
        """ Grab Information from the Thread """

        return { 
                 'connection': self.connect_name, 
                 'binding': self.binding_name,
                 'queue': self.queue_name,
                 'processed': self.messages_processed
               }

    def run( self ):
        """ Meat of the queue consumer code """

        logging.debug( 'In consumer run for thread "%s"' % self.thread_name )

        # Connect to the AMQP Broker
        self.connection = self.connect( self.config['Connections'][self.connect_name] )
        
        # Create the Channel
        self.channel = self.connection.channel()

        # Create / Connect to the Queue
        self.queue_name = self.config['Bindings'][self.binding_name]['queue']
        queue_auto_delete = self.config['Queues'][self.queue_name ]['auto_delete']
        queue_durable = self.config['Queues'][self.queue_name ]['durable']
        queue_exclusive = self.config['Queues'][self.queue_name ]['exclusive']

        self.channel.queue_declare( queue = self.queue_name , 
                                    durable = queue_durable,
                                    exclusive = queue_exclusive, 
                                    auto_delete = queue_auto_delete )

        # Create / Connect to the Exchange
        exchange = self.config['Bindings'][self.binding_name]['exchange']
        exchange_auto_delete = self.config['Exchanges'][exchange]['auto_delete']
        exchange_durable = self.config['Exchanges'][exchange]['durable']
        exchange_type = self.config['Exchanges'][exchange]['type']
        
        self.channel.exchange_declare( exchange = exchange, 
                                       type = exchange_type, 
                                       durable = exchange_durable,
                                       auto_delete = exchange_auto_delete)

        # Bind to the Queue / Exchange
        self.channel.queue_bind( queue = self.queue_name, 
                                 exchange = exchange,
                                 routing_key = self.binding_name )

        # Let the consuming start
        self.consumer_tag = self.channel.basic_consume( 
                                queue = self.queue_name, 
                                no_ack = self.config['Bindings'][self.binding_name]['no_ack'],
                                callback = self.receive )

        # Set our prefetch QOS
        self.channel.basic_qos( 0, 0, False )

        # Wait for messages
        logging.debug( 'Waiting on messages for "%s"' %  self.thread_name )

        # Loop as long as the thread is running
        while self.running == True:
            try:
                self.channel.wait()
            except AttributeError:
                break
            except IOError:
                break
                
    def receive(self, message):
        """ Receive a message """
        
        if message is not None:
            #logging.debug('Received a message on "%s".' % self.thread_name)
            #print message.body
            self.channel.basic_ack(message.delivery_tag)
            self.messages_processed += 1
        
    def shutdown(self):
        """ Gracefully close the connection """
        
        logging.debug('Shutting down consumer "%s"' % self.thread_name )
        
        # Let the loop know we're not looping
        self.running = False
        try:
            self.connection.close()
        except IOError:
            pass
    
class mcp:
    """ Master Control Process keeps track of threads and threading needs """

    def __init__(self, config, options):
        logging.debug('MCP Created')
        self.alice = alice()
        self.bindings = []
        self.config = config
        pass

    def poll(self):
        logging.debug('MCP Polling')
        for binding in self.bindings:
            new_threads = {}
            for thread in binding['threads']:
                info = binding['threads'][thread].getInformation()
                data = self.alice.getQueueDepth(info['connection'], info['queue'])
                
                # Easier to work with variables
                queue_depth = int(data['depth'])
                min = self.config['Bindings'][info['binding']]['consumers']['min']
                max = self.config['Bindings'][info['binding']]['consumers']['max']
                threshold = self.config['Bindings'][info['binding']]['consumers']['threshold']

                if queue_depth > threshold and binding['threadCount'] < max:
                    
                    logging.info( 'Spawning worker thread for connection "%s" binding "%s": %i messages pending, %i threshhold, %i min, %i max, %i consumers active.' % 
                                    ( info['connection'], 
                                      info['binding'], 
                                      queue_depth, 
                                      threshold,
                                      min,
                                      max,
                                      binding['threadCount'] ) )

                    # Create a unique thread name
                    thread_name = '%s_%s_%i' % ( info['connection'], 
                                                 info['binding'], 
                                                 binding['threadCount'])

                    # Create the new thread making it use self.consume
                    thread = consumerThread( self.config,
                                             thread_name, 
                                             info['binding'], 
                                             info['connection'] );

                    # Add to our dictionary of active threads
                    new_threads[thread_name] = thread
                    
                    # Increment our thread counter
                    binding['threadCount']  += 1

                    # Set the name of the thread for future use
                    thread.setName(thread_name)

                    # Start the thread
                    thread.start()
                    
                    # We only want 1 new thread at a time
                    break

            # Append any new threads
            if len(new_threads) > 0:
                binding['threads'].update(new_threads)

            # Check if our queue depth is below our threshold and we have more than the min amount
            if queue_depth < threshold and binding['threadCount'] > min:

                # Remove a thread
                for thread in binding['threads']:
                    logging.info( 'Removed worker thread for connection "%s" binding "%s": %i messages pending, %i threshhold, %i min, %i max, %i consumers active.' % 
                                    ( info['connection'], 
                                      info['binding'], 
                                      queue_depth, 
                                      threshold,
                                      min,
                                      max,
                                      binding['threadCount'] ) )
                                      
                    binding['threads'][thread].shutdown()
                    binding['threadCount']  -= 1
                    del_thread = True
                    break;

                # Remove it from our thread dictionary
                del binding['threads'][thread] 

    def shutdown(self):
        logging.debug('MCP Shutting Down')
        for binding in self.bindings:
            for thread in binding['threads']:
                binding['threads'][thread].shutdown()

    def start(self):
        logging.debug('MCP Starting Up')

        for binding_name in self.config['Bindings']:

            binding = { 'name': binding_name }
            binding['queue'] = self.config['Bindings'][binding_name]['queue']
            binding['threads'] = {}
            binding['threadCount'] = 0

            # For each connection, kick off the min consumers and start consuming
            for connect_name in self.config['Bindings'][binding_name]['connections']:
                for i in range( 0, self.config['Bindings'][binding_name]['consumers']['min'] ):
                    logging.debug( 'Creating worker thread #%i for connection "%s" binding "%s"' % ( i, connect_name, binding_name ) )

                    # Create a unique thread name
                    thread_name = '%s_%s_%i' % ( connect_name, binding_name, i )

                    # Create the new thread making it use self.consume
                    thread = consumerThread( self.config,
                                             thread_name, 
                                             binding_name, 
                                             connect_name );

                    # Add to our dictionary of active threads
                    binding['threads'][thread_name] = thread
                    
                    binding['threadCount'] += 1

                    # Set the name of the thread for future use
                    thread.setName(thread_name)

                    # Start the thread
                    thread.start()

            # Append this binding to our binding stack
            self.bindings.append(binding)

def shutdown(signum = 0, frame = None):
    global mcp
    
    logging.info('Graceful shutdown initiated.')
    mcp.shutdown()
    logging.debug('Graceful shutdown complete')
    os._exit(signum)

def main():
    global mcp, mcp_poll_delay
    
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

    # Start the Master Control Program ;-)
    mcp = mcp(config, options)
    
    # Kick off our core connections
    mcp.start()
    
    # Loop until someone wants us to stop
    while 1:
        try:
            
            # Sleep is so much more CPU friendly than pass
            time.sleep(mcp_poll_delay)
            
            # Check to see if we need to adjust our threads
            if options.single_thread is not True:
                mcp.poll()
            
        except (KeyboardInterrupt, SystemExit):
            shutdown()
        
# Only execute the code if invoked as an application
if __name__ == '__main__':
    main()
