#!/usr/bin/env python
"""
Rejected AMQP Consumer Framework

A multi-threaded consumer application and how!

Created by Gavin M. Roy on 2009-09-10

@author Gavin M. Roy
@copyright 2009 Insider Guides, Inc.. All rights reserved.
@license BSD
"""

import amqplib.client_0_8 as amqp
import exceptions
import logging
import sys
import optparse
import os
import signal
import threading
import time
import yaml

# Number of seconds to sleep between polls
mcp_poll_delay = 10

version = '0.1'
class ConnectionException( exceptions.Exception ):
    
    def __str__(self):
        return "Connection Failed"

    
class ConsumerThread( threading.Thread ):
    """ Consumer Class, Handles the actual AMQP work """
    
    def __init__( self, configuration, thread_name, binding_name, connect_name ):

        logging.debug( 'Initializing a Consumer class in thread "%s"' % thread_name )

        # Rejected full Configuration
        self.config = configuration
        
        # Binding to make code more readable
        binding = self.config['Bindings'][binding_name]

        # Initialize object wide variables
        self.auto_ack = binding['consumers']['auto_ack']
        self.binding_name = binding_name
        self.connect_name = connect_name
        self.connection = None
        self.errors = 0
        self.interval_count = 0
        self.interval_start = None
        self.locked = False
        self.max_errors = binding['consumers']['max_errors']
        self.messages_processed = 0
        self.requeue_on_error = binding['consumers']['requeue_on_error']
        self.running = True
        self.queue_name = None
        self.thread_name = thread_name
        
        # If we have throttle config use it
        self.throttle = False
        self.throttle_count = 0
        self.throttle_duration = 0
        if binding['consumers'].has_key('throttle'):
            logging.debug( 'Setting message throttle to %i message(s) per second' % 
                            binding['consumers']['throttle'] )
            self.throttle = True
            self.throttle_threshold = binding['consumers']['throttle']

            
        # Init the Thread Object itself
        threading.Thread.__init__(self)  

    def connect( self, configuration ):
        """ Connect to an AMQP Broker  """

        logging.debug( 'Creating a new connection for "%s" in thread "%s"' % 
                        ( self.binding_name, self.thread_name ) )

        try:
            # Try and create our new AMQP connection
            connection = amqp.Connection( host ='%s:%s' % ( configuration['host'], configuration['port'] ),
                                userid = configuration['user'], 
                                password = configuration['pass'], 
                                ssl = configuration['ssl'],
                                virtual_host = configuration['vhost'] )
            return connection

        # amqp lib is only raising a generic exception which is odd since it has a AMQPConnectionException class
        except IOError, e:
            logging.error( 'Connection error #%i: %s' % (e.errno, e.message) )
            raise ConnectionException

    def get_information(self):
        """ Grab Information from the Thread """

        return { 
                 'connection': self.connect_name, 
                 'binding': self.binding_name,
                 'queue': self.queue_name,
                 'processed': self.messages_processed,
                 'throttle_count': self.throttle_count
               }

    def is_locked( self ):
        """ What is the lock status for the MCP? """
        
        return self.locked
        
    def lock( self ):
        """ Lock the thread so the MCP does not destroy it until we're done processing a message """

        self.locked = True

    def process(self, message):
       """ Process a message from Rabbit"""
       
       # If we're throttling
       if self.throttle and self.interval_start is None:
           self.interval_start = time.time()
   
       # Lock while we're processing
       self.lock()
       
       # Process the message, if it returns True, we're all good
       if self.processor.process(message):
           self.messages_processed += 1

           # If we're not auto-acking at the broker level, do so here, but why?
           if not self.auto_ack:
               self.channel.basic_ack( message.delivery_tag )

       # It's returned False, so we should check our our check
       # We don't want to have out-of-control errors
       else:
           # Unlock
           self.unlock()
           
           # Do we need to requeue?  If so, lets send it
           if self.requeue_on_error:
               msg = amqp.Message(message.body)
               msg.properties['delivery_mode'] = 2
               self.channel.basic_publish( msg,
                                           exchange = self.exchange,
                                           routing_key = self.binding_name )
           
           # Keep track of how many errors we've had
           self.errors += 1
           
           # If we've had too many according to the configuration, shutdown
           if self.errors >= self.max_errors:
               logging.error( 'Received %i errors, shutting down thread "%s"' % self.thread_name )
               self.shutdown()
               return
           
       # Unlock the thread, safe to shutdown
       self.unlock()
   
       # If we're throttling
       if self.throttle:
       
           # Get the duration from when we starting this interval to now
           self.throttle_duration += time.time() - self.interval_start
           self.interval_count += 1

           # If the duration is less than 1 second and we've processed up to (or over) our max
           if self.throttle_duration <= 1 and self.interval_count >= self.throttle_threshold:
           
               # Increment our throttle count
               self.throttle_count += 1
               
               # Figure out how much time to sleep
               sleep_time = 1 - self.throttle_duration
               
               logging.debug( 'Throttling to %i message(s) per second on %s, waiting %f seconds.' % 
                              ( self.throttle_threshold, self.thread_name, sleep_time ) )
               
               # Sleep and setup for the next interval
               time.sleep(sleep_time)

               # Reset our counters
               self.interval_count = 0
               self.interval_start = None
               self.throttle_duration = 0
               
           # Else if our duration is more than a second restart our counters
           elif self.throttle_duration >= 1:
               self.interval_count = 0   
               self.interval_start = None
               self.throttle_duration = 0
               
    def run( self ):
        """ Meat of the queue consumer code """

        logging.debug( 'In consumer run for thread "%s"' % self.thread_name )

        # Import our processor class
        import_name = self.config['Bindings'][self.binding_name]['import']
        class_name = self.config['Bindings'][self.binding_name]['processor']
        
        # Try and import the module
        try:
            class_module = getattr(__import__(import_name), class_name)
        
        except ImportError:
            logging.error( 'Could not import the "%s" module.' % import_name )
            self.running = False
            return

        except AttributeError:
            logging.error( 'Did not find the class "%s" in the module "%s".' % ( class_name, import_name ) )
            self.running = False
            return
                
        processor_class = getattr(class_module, class_name)
        logging.info( 'Creating message processor: %s.%s in %s' % 
                      ( import_name, class_name, self.thread_name ) )
        self.processor = processor_class()
            
        # Connect to the AMQP Broker
        try:
            self.connection = self.connect( self.config['Connections'][self.connect_name] )
        except ConnectionException:
            self.running = False
            return
        
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
        self.exchange = self.config['Bindings'][self.binding_name]['exchange']
        exchange_auto_delete = self.config['Exchanges'][self.exchange]['auto_delete']
        exchange_durable = self.config['Exchanges'][self.exchange]['durable']
        exchange_type = self.config['Exchanges'][self.exchange]['type']
        
        self.channel.exchange_declare( exchange = self.exchange, 
                                       type = exchange_type, 
                                       durable = exchange_durable,
                                       auto_delete = exchange_auto_delete)

        # Bind to the Queue / Exchange
        self.channel.queue_bind( queue = self.queue_name, 
                                 exchange = self.exchange,
                                 routing_key = self.binding_name )

        # Wait for messages
        logging.debug( 'Waiting on messages for "%s"' %  self.thread_name )

        # Let AMQP know to send us messages
        self.channel.basic_consume( queue = self.queue_name, 
                                    no_ack = self.auto_ack,
                                    callback = self.process, 
                                    consumer_tag = self.thread_name )

        # Initialize our throttle variable if we need it
        interval_start = None
        
        # Loop as long as the thread is running
        while self.running:
            
            # Wait on messages
            try:
                self.channel.wait()
            except AttributeError, IOError:
                break
                
        logging.debug( 'Exiting ConsumerThread.run() for %s' % self.thread_name )
                    
    def shutdown(self):
        """ Gracefully close the connection """

        if self.running:
            logging.debug( 'Shutting down consumer "%s"' % self.thread_name )
            self.running = False
        
        """ 
        This hangs because channel.wait in the thread is blocking on socket.recv.
        channel.close sends the close message, then enters ultimately into
        socket.recv to get the close_ok response.  Depending on the timing,
        the channel.wait has picked up the close_ok after channel.close (on main
        thread) entered socket.recv.
        
        I was looking at a nonblocking method to deal with this properly:
        http://www.lshift.net/blog/2009/02/18/evserver-part2-rabbit-and-comet
        """
        #self.channel.close()
        if self.connection:
            try:
                self.connection.close()
            except IOError, e:
                # We're already closed
                pass

    def unlock( self ):
        """ Unlock the thread so MCP can shut us down """
        
        self.locked = False
    
class MasterControlProgram:
    """ Master Control Program keeps track of threads and threading needs """

    def __init__(self, config, options):
        
        logging.debug( 'Master Control Program Created' )
        
        # If we have monitoring enabled for elasic resizing
        if config['Monitor']['enabled']:
            from rejected.monitors import Alice
            self.alice = Alice()
        else:
            self.alice = None
            
        self.bindings = []
        self.config = config
        self.last_poll = None
        self.shutdown_pending = False
        self.thread_stats = {}

    def get_information(self):
        """ Return the stats data collected from Poll """
        
        pass
        
    def poll(self):
        """ Check the Alice daemon for queue depths for each binding """
        global mcp_poll_delay
        
        logging.debug( 'Master Control Program Polling' )
        
        # Cache the monitor queue depth checks
        cache_lookup = {}
        
        # default total counts
        total_processed = 0
        total_throttled = 0
        
        # Get our delay since last poll
        if self.last_poll:
            duration_since_last_poll = time.time() - self.last_poll
        else:
            duration_since_last_poll = mcp_poll_delay
        
        # If we're shutting down, no need to do this, can make it take longer
        if self.shutdown_pending:
            return

        # Loop through each binding to ensure all our threads are running
        offset = 0
        for binding in self.bindings:
        
            # Go through the threads to check the queue depths for each server
            for thread_name, thread in binding['threads'].items():
            
                # Make sure the thread is still alive, otherwise remove it and move on
                if not thread.isAlive():
                    logging.error( 'Encountered a dead thread: %s, removing it from the stack' % thread_name )
                    del binding['threads'][thread_name]
                    continue


            # If we don't have any consumer threads, remove the binding
            if not len(binding['threads']):
                logging.error( 'We have no working consumers, removing down this binding.' )
                del self.bindings[offset]
                
            # Increment our list offset
            offset += 1
        
        # If we have removed all of our bindings because they had no working threads, shutdown         
        if not len(self.bindings):
            logging.error( 'We have no working bindings, shutting down.' )
            shutdown()
            return
            
        # If we're monitoring, then run through here
        if self.alice:
            
            # Loop through each binding
            offset = 0
            for binding in self.bindings:
            
                # Go through the threads to check the queue depths for each server
                for thread_name, thread in binding['threads'].items():
                
                    # Get our thread data such as the connection and queue it's using
                    info = thread.get_information()
      
                    # Check our stats info
                    if thread_name in self.thread_stats:
        
                        # Calculate our processed & throttled amount                    
                        processed = info['processed'] - self.thread_stats[thread_name]['processed']  
                        throttled = info['throttle_count'] - self.thread_stats[thread_name]['throttle_count']  
        
                        # Totals for MCP Stats
                        total_processed += processed
                        total_throttled += throttled
            
                        logging.debug( '%s processed %i messages in %f seconds (%f mps) - Throttled %i times' % 
                            ( thread_name, 
                              processed,  
                              duration_since_last_poll, 
                              ( float(processed) / duration_since_last_poll ), 
                              throttled ) )
                    else:
                        # Initialize our thread stats dictionary
                        self.thread_stats[thread_name] = {}
                
                        # Totals for MCP Stats
                        total_processed += info['processed']
                        total_throttled += info['throttle_count']

                    # Set our thread processed # count for next time
                    self.thread_stats[thread_name]['processed'] = info['processed']   
                    self.thread_stats[thread_name]['throttle_count'] = info['throttle_count']   
            
                    # Check the queue depth for the connection and queue
                    cache_name = '%s-%s' % ( info['connection'], info['queue'] )
                    if cache_name in cache_lookup:
                        data = cache_lookup[cache_name]
                    else:
                        # Get the value from Alice
                        data = self.alice.get_queue_depth(info['connection'], info['monitor_port'], info['queue'])
                        cache_lookup[cache_name] = data

                    # Easier to work with variables
                    queue_depth = int(data['depth'])
                    min = self.config['Bindings'][info['binding']]['consumers']['min']
                    max = self.config['Bindings'][info['binding']]['consumers']['max']
                    threshold = self.config['Bindings'][info['binding']]['consumers']['threshold']

                    # If our queue depth exceeds the threshold and we haven't maxed out make a new worker
                    if queue_depth > threshold and len(binding['threads']) < max:
                
                        logging.info( 'Spawning worker thread for connection "%s" binding "%s": %i messages pending, %i threshhold, %i min, %i max, %i consumers active.' % 
                                        ( info['connection'], 
                                          info['binding'], 
                                          queue_depth, 
                                          threshold,
                                          min,
                                          max,
                                          len(binding['threads']) ) )

                        # Create a unique thread name
                        new_thread_name = '%s_%s_%i' % ( info['connection'], 
                                                     info['binding'], 
                                                     len(binding['threads']))

                        # Create the new thread making it use self.consume
                        new_thread = ConsumerThread( self.config,
                                                     new_thread_name, 
                                                     info['binding'], 
                                                     info['connection'] )

                        # Add to our dictionary of active threads
                        binding['threads'][new_thread_name] = new_thread

                        # Start the thread
                        new_thread.start()
                
                        # We only want 1 new thread per poll as to not overwhelm the consumer system
                        break

                # Check if our queue depth is below our threshold and we have more than the min amount
                if queue_depth < threshold and len(binding['threads']) > min:

                    # Remove a thread
                    for thread_name, thread in binding['threads'].items():
                        if not thread.is_locked():
                            logging.info( 'Removed worker thread for connection "%s" binding "%s": %i messages pending, %i threshhold, %i min, %i max, %i consumers active.' % 
                                            ( info['connection'], 
                                              info['binding'], 
                                              queue_depth, 
                                              threshold,
                                              min,
                                              max,
                                              len(binding['threads']) ) )
                    
                            # Shutdown the thread gracefully          
                            thread.shutdown()
                    
                            # Remove it from the start
                            del binding['threads'][thread_name]
                    
                            # We only want to remove one thread per poll
                            break;
        
                logging.info('MCP Poll Results for binding #%i: %i total messages processed in %f seconds (%f mps). %i threads throttled themselves %i times.' %
                               ( offset, 
                                 total_processed, 
                                 duration_since_last_poll, 
                                 ( float(total_processed) / duration_since_last_poll ),
                                 len(binding['threads']), 
                                 total_throttled ) )
                offset += 1
            
            # Get our last poll time
            self.last_poll = time.time()
        
    def shutdown(self):
        """ Graceful shutdown of the MCP means shutting down threads too """
        
        logging.debug( 'Master Control Program Shutting Down' )
        
        # Get the thread count
        threads = self.threadCount()
        
        # Keep track of the fact we're shutting down
        self.shutdown_pending = True
        
        # Loop as long as we have running threads
        while threads:
            
            # Loop through all of the bindings and try and shutdown their threads
            for binding in self.bindings:
                
                # Loop through all the threads in this binding
                for thread_name, thread in binding['threads'].items():

                    # Let the thread know we want to shutdown
                    thread.shutdown()

                    # If the thread is not locked, shut down the thread
                    if not thread.is_locked():
                        del binding['threads'][thread_name]

            # Get our updated thread count and only sleep then loop if it's > 0, 
            threads = self.threadCount()
            
            # If we have any threads left, sleep for a second before trying again
            if threads:
                logging.debug( 'Waiting on %i threads to cleanly shutdown.' % threads )
                time.sleep(1)
                    
    def start(self):
        """ Initialize all of the consumer threads when the MCP comes to life """
        logging.debug( 'Master Control Program Starting Up' )

        # Loop through all of the bindings
        for binding_name in self.config['Bindings']:
            
            # Create the dictionary values for this binding
            binding = { 'name': binding_name }
            binding['queue'] = self.config['Bindings'][binding_name]['queue']
            binding['threads'] = {}

            # For each connection, kick off the min consumers and start consuming
            for connect_name in self.config['Bindings'][binding_name]['connections']:
                for i in xrange( 0, self.config['Bindings'][binding_name]['consumers']['min'] ):
                    logging.debug( 'Creating worker thread #%i for connection "%s" binding "%s"' % ( i, connect_name, binding_name ) )

                    # Create a unique thread name
                    thread_name = '%s_%s_%i' % ( connect_name, binding_name, i )

                    # Create the new thread making it use self.consume
                    thread = ConsumerThread( self.config,
                                             thread_name, 
                                             binding_name, 
                                             connect_name );

                    # Start the thread
                    thread.start()

                    # Check to see if the thread is alive before adding it to our stack
                    if thread.isAlive():

                        # Add to our dictionary of active threads
                        binding['threads'][thread_name] = thread

            # Append this binding to our binding stack
            self.bindings.append(binding)
        
    def threadCount(self):
        """ Return the total number of working threads managed by the MCP """
        
        count = 0
        for binding in self.bindings:
            count += len(binding['threads'])
        return count

def shutdown(signum = 0, frame = None):
    """ Application Wide Graceful Shutdown """
    global mcp
    
    logging.info( 'Graceful shutdown initiated.' )
    mcp.shutdown()
    logging.debug( 'Graceful shutdown complete' )
    os._exit(signum)

def main():
    """ Main Application Handler """
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

    parser.add_option("-d", "--detached",
                        action="store_true", dest="detached", default=False,
                        help="Run in daemon mode")                                                                                                                                 

    parser.add_option("-m", "--monitor",
                        action="store_true", dest="monitor", 
                        default=False,
                        help="Poll Alice for scaling consumer threads.")
                        
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
            stream.close()
            
    except IOError:
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
    config['Logging']['level'] = logging_levels.get( config['Logging']['level'], 
                                                     logging.NOTSET )

    # If the user says verbose overwrite the settings.
    if options.verbose:
    
        # Set the debugging level to verbose
        config['Logging']['level'] = logging.DEBUG
        
        # If we have specified a file, remove it so logging info goes to stdout
        if config['Logging'].has_key('filename'):
            del config['Logging']['filename']

    else:
        # Build a specific path to our log file
        if config['Logging'].has_key('filename'):
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

    # If our config has monitoring disabled but we enable via cli, enable it
    if options.monitor and not config['Monitor']['enabled']:
        config['Monitor']['enabled'] = True

    # Fork our process to detach if not told to stay in foreground
    if options.detached:
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
        logging.debug( 'rejected.py has forked into the background.' )
        
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
    mcp = MasterControlProgram(config, options)
    
    # Kick off our core connections
    mcp.start()
    
    # Loop until someone wants us to stop
    do_poll = options.monitor and not options.single_thread
    while 1:
        
        # Have the Master Control Process poll
        try:
            # Check to see if we need to adjust our threads
            if do_poll:
                mcp.poll()

            # Sleep is so much more CPU friendly than pass
            time.sleep(mcp_poll_delay)

        except (KeyboardInterrupt, SystemExit):
            # The user has sent a kill or ctrl-c
            shutdown()
        
# Only execute the code if invoked as an application
if __name__ == '__main__':
    
    # Get our sub-path going for processor imports
    sys.path.insert(0, 'processors')
    
    # Run the main function
    main()
