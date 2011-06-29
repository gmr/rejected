#!/usr/bin/env python
"""
Rejected AMQP Consumer Framework

A multi-threaded consumer application and how!

Copyright (c) 2009,  Insider Guides, Inc.
All rights reserved.
 
Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
 
Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
Neither the name of the Insider Guides, Inc. nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

__author__  = "Gavin M. Roy"
__email__   = "gmr@myyearbook.com"
__date__    = "2009-09-10"
__version__ = "0.3.0"

import amqplib.client_0_8 as amqp
import exceptions
import logging
import optparse
import os
import signal
import sys
import threading
import time
import traceback
import yaml
import zlib


# Number of seconds to sleep between polls
_MCP_POLL_DELAY = 10
_IS_QUITTING = False
_QOS_PREFETCH_COUNT = 1
_PROCESSOR_BASE = '/opt/processors'

# Process name will be overriden by the config file
process = 'Unknown'
_logger = logging.getLogger('rejected')


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
    
    
class ConnectionException( exceptions.Exception ):
    
    def __str__(self):
        return "Connection Failed"


class ConsumerThread( threading.Thread ):
    """ Consumer Class, Handles the actual AMQP work """
    
    def __init__( self, configuration, routing_key, connect_name ):

        _logger.debug( 'Initializing a Consumer Thread' )

        # Rejected full Configuration
        self.config = configuration
        
        # Binding to make code more readable
        binding = self.config['Bindings'][routing_key]

        # Initialize object wide variables
        self.auto_ack = binding['consumers']['auto_ack']
        self.routing_key = routing_key
        if binding.has_key('compressed'):
            self.compressed = binding['compressed']
        else:
            self.compressed = False
        self.connect_name = connect_name
        self._connection_user_name = None
        self.connection = None
        self.errors = 0
        self.interval_count = 0
        self.interval_start = None
        self._locked = False
        self.max_errors = binding['consumers']['max_errors']
        self.messages_processed = 0
        self.requeue_on_error = binding['consumers']['requeue_on_error']
        self.running = True
        self.queue_name = None

        # If we have throttle config use it
        self.throttle = False
        self.throttle_count = 0
        self.throttle_duration = 0
        if binding['consumers'].has_key('throttle'):
            _logger.debug( 'Setting message throttle to %i message(s) per second' % 
                            binding['consumers']['throttle'] )
            self.throttle = True
            self.throttle_threshold = binding['consumers']['throttle']

        self.total_wait = 0.0
            
        # Init the Thread Object itself
        threading.Thread.__init__(self)  

    def connect(self, configuration):
        """Connect to an AMQP Broker

        :param configuration: Configuration values
        :type configuration: dict
        :raises: ConnectionException

        """
        _logger.debug('Creating a new connection for "%s" in thread "%s"', self.routing_key, self.getName())
        self._connection_user_name = configuration['user']
        try:
            # Try and create our new AMQP connection
            connection = amqp.Connection('%s:%i' % (configuration['host'], configuration['port']),
                                         self._connection_user_name,
                                         configuration['pass'],
                                         virtual_host = configuration['vhost'])
            return connection

        # amqplib is only raising a generic exception which is odd since it has a AMQPConnectionException class
        except IOError as error:
            _logger.error( 'Connection error #%i: %r' , error.errno, error)
            raise ConnectionException

    @property
    def information(self):
        """Grab Information from the Thread"""
        return {'connection': self.connect_name,
                'binding': self.routing_key,
                'queue': self.queue_name,
                'processed': self.messages_processed,
                'throttle_count': self.throttle_count,
                'queue_depth': self.channel.queue_declare(self.queue_name, True, *self.queue_options)[1]}

    @property
    def locked(self):
        """ What is the lock status for the MCP? """
        return self._locked
        
    def lock(self):
        """ Lock the thread so the MCP does not destroy it until we're done processing a message """
        self._locked = True

    def unlock(self):
        self._locked = False

    def _publish(self, _previous_message):
        """Publish a message to the RabbitMQ broker.
        """
        message = amqp.Message(_previous_message.body)
        message.properties = _previous_message.properties
        message.app_id = 'rejected/%s' % __version__
        message.user_id = self._connection_user_name
        self.channel.basic_publish(message, self.exchange, self.routing_key)

    def process(self, message):
        """ Process a message from Rabbit"""
        
        # If we're throttling
        if self.throttle and self.interval_start is None:
           self.interval_start = time.time()
        
        # Lock while we're processing
        self.lock()
        
        # If we're compressed in message body, decompress it
        if self.compressed:
            try:
                message.body = zlib.decompress(message.body)
            except zlib.error:
                _logger.warn('Invalid zlib compressed message.body')
        
        # Process the message, if it returns True, we're all good
        try:
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
                                               routing_key = self.routing_key )
               
               # Keep track of how many errors we've had
               self.errors += 1
               
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            formatted_lines = traceback.format_exc().splitlines()      
            _logger.critical('ConsumerThread: Processor threw an uncaught exception')
            _logger.critical('ConsumerThread: %s:%s' % (type(e), str(e)))
            _logger.critical('ConsumerThread: %s' % formatted_lines[3].strip())
            _logger.critical('ConsumerThread: %s' % formatted_lines[4].strip())
                    
            # Do we need to requeue?  If so, lets send it
            if self.requeue_on_error:
                self._publish(message)

            # Keep track of how many errors we've had
            self.errors += 1
        
            # If we've had too many according to the configuration, shutdown
            if self.errors >= self.max_errors:
                _logger.error( 'Received %i errors, shutting down thread "%s"' % ( self.errors, self.getName() ) )
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
               
               _logger.debug( '%s: Throttling to %i message(s) per second, waiting %.2f seconds.' % 
                              ( self.getName(), self.throttle_threshold, sleep_time ) )
               
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
        global options

        _logger.debug( '%s: Running thread' % self.getName() )

        # Import our processor class
        import_name = self.config['Bindings'][self.routing_key]['import']
        class_name = self.config['Bindings'][self.routing_key]['processor']
        
        # Try and import the module
        processor_class = import_namespaced_class("%s.%s" % (import_name, class_name))
        _logger.info( '%s: Creating message processor: %s.%s' % 
                      ( self.getName(), import_name, class_name ) )
                      
        # If we have a config, pass it in to the constructor                      
        if self.config['Bindings'][self.routing_key].has_key('config'):
            self.processor = processor_class(self.config['Bindings'][self.routing_key]['config'])
        else:
            self.processor = processor_class()
            
        # Assign the port to monitor the queues on
        self.monitor_port = self.config['Connections'][self.connect_name]['monitor_port']

        # Connect to the AMQP Broker
        try:
            self.connection = self.connect( self.config['Connections'][self.connect_name] )
        except ConnectionException:
            self.running = False
            return
        
        # Create the Channel
        self.channel = self.connection.channel()

        # Set the qos prefetch based on a default of _QOS_PREFETCH_COUNT
        self.channel.basic_qos(0, self.config['Bindings'][self.routing_key].get('qos', _QOS_PREFETCH_COUNT), 0)

        # Create / Connect to the Queue
        self.queue_name = self.config['Bindings'][self.routing_key]['queue']

        self.queue_options = (self.config['Queues'][self.queue_name ]['durable'],
                              self.config['Queues'][self.queue_name ]['exclusive'],
                              self.config['Queues'][self.queue_name ]['auto_delete'])
    
        # Create / Connect to the Exchange
        self.exchange = self.config['Bindings'][self.routing_key]['exchange']
        exchange_auto_delete = self.config['Exchanges'][self.exchange]['auto_delete']
        exchange_durable = self.config['Exchanges'][self.exchange]['durable']
        exchange_type = self.config['Exchanges'][self.exchange]['type']

        # Wait for messages
        _logger.debug( '%s: Waiting on messages' %  self.getName() )

        # Let AMQP know to send us messages
        self.channel.basic_consume(queue = self.queue_name,
                                   no_ack = self.auto_ack,
                                   callback = self.process,
                                   consumer_tag = self.getName())

        # Loop as long as the thread is running
        while self.running:
            
            # Wait on messages
            if _IS_QUITTING:
                _logger.info('Not wait()ing because _IS_QUITTING is set!')
                break
            try:
                start = time.time()
                self.channel.wait()
                dur = (time.time() - start) * 1000.0
                self.total_wait += dur
                # _logger.debug('%s: %.3fms in wait()', self.getName(), dur)
            except IOError:
                _logger.error('%s: IOError received' % self.getName() )
            except AttributeError:
                _logger.error('%s: AttributeError received' % self.getName() )
                break
            except TypeError:
                _logger.error('%s: TypeError received' % self.getName() )
                
        _logger.info( '%s: Exiting ConsumerThread.run()' % self.getName() )

    def shutdown(self):
        """ Gracefully close the connection """

        if self.running:
            _logger.debug( 'Already shutting down %s' % self.getName() )
            self.running = False
            return False
        
        """ 
        This hangs because channel.wait in the thread is blocking on socket.recv.
        channel.close sends the close message, then enters ultimately into
        socket.recv to get the close_ok response.  Depending on the timing,
        the channel.wait has picked up the close_ok after channel.close (on main
        thread) entered socket.recv.
        
        I was looking at a nonblocking method to deal with this properly:
        http://www.lshift.net/blog/2009/02/18/evserver-part2-rabbit-and-comet
        """
        if self.connection:
            try:
                _logger.debug('%s: Cancelling the consumer' % self.getName())
                self.channel.basic_cancel(self.getName())
                _logger.debug('%s: Closing the AMQP channel' % self.getName())
                self.channel.close()
                _logger.debug('%s: Closing the AMQP connection' % self.getName())
                self.connection.close()
                _logger.info('%s: AMQP connection closed' % self.getName())
            except IOError as error:
                # Socket is likely already closed
                _logger.debug('%s: Error closing the AMQP connection: %r', self.getName(), error)
            except TypeError as error:
                # Bug
                _logger.debug('%s: Error closing the AMQP connection: %r', self.getName(), error)

        _logger.debug('%s: Shutting down processor' % self.getName())
        try:
            self.processor.shutdown()
        except AttributeError:
            _logger.debug('%s: Processor does not have a shutdown method' % self.getName())

        return True
    
class MasterControlProgram:
    """ Master Control Program keeps track of threads and threading needs """

    def __init__(self, config):
        self.bindings = []
        self.config = config
        self.shutdown_pending = False
        self.thread_stats = {}

        # Carry for calculating messages per second
        self._monitoring_last_poll = time.time()

    def poll(self):
        """ Check the queue depths via passive queue delcare for each binding"""

        _logger.debug( 'MCP: Polling' )

        # If we're shutting down, no need to do this, can make it take longer
        if self.shutdown_pending:
            _logger.debug("Exiting %s.poll() due to pending shutdown", __class__.__name__)
            return

        # Loop through each binding to ensure all our threads are running
        offset = 0
        for binding in self.bindings:
        
            # Go through the threads to check the queue depths for each server
            dead_threads = []
            for x in xrange(0, len(binding['threads'])):
            
                # Make sure the thread is still alive, otherwise remove it and move on
                if not binding['threads'][x].isAlive():
                    _logger.error( 'MCP: Encountered a dead thread, removing.' )
                    dead_threads.append(x)
            
            # Remove dead threads
            for list_offset in dead_threads:
                _logger.error( 'MCP: Removing the dead thread from the stack' )
                binding['threads'].pop(list_offset)

            # If we don't have any consumer threads, remove the binding
            if not len(binding['threads']):
                _logger.error( 'MCP: We have no working consumers, removing down this binding.' )
                del self.bindings[offset]
                
            # Increment our list offset
            offset += 1
        
        # If we have removed all of our bindings because they had no working threads, shutdown         
        if not self.bindings:
            _logger.error( 'MCP: We have no working bindings, shutting down.' )
            shutdown()
            return

        _duration_since_last_poll = time.time() - self._monitoring_last_poll

        # Loop through each binding
        offset = 0
        for binding in self.bindings:

            _logger.debug('Iterating through the bindings')

            # default total counts
            total_processed = 0
            total_throttled = 0
            total_wait = 0.0

            # Go through the threads to check the queue depths for each server
            for thread in binding['threads']:

                _logger.debug('Getting thread information for %s', thread.getName())

                # Get our thread data such as the connection and queue it's using
                info = thread.information

                # Stats are keyed on thread name
                thread_name = thread.getName()
                # To calculate average time waiting
                total_wait += thread.total_wait
                thread.total_wait = 0.0

                # Check our stats info
                if thread_name in self.thread_stats:

                    # Calculate our processed & throttled amount
                    processed = info['processed'] - self.thread_stats[thread_name]['processed']
                    throttled = info['throttle_count'] - self.thread_stats[thread_name]['throttle_count']

                    # Totals for MCP Stats
                    total_processed += processed
                    total_throttled += throttled

                    _logger.debug( '%s processed %i messages and throttled %i messages in %.2f seconds at a rate of %.2f mps; total wait time: %.2fms' %
                        ( thread_name,
                          processed,
                          throttled,
                          _duration_since_last_poll,
                          ( float(processed) / _duration_since_last_poll ),
                          total_wait
                        ))
                else:
                    # Initialize our thread stats dictionary
                    self.thread_stats[thread_name] = {}

                    # Totals for MCP Stats
                    total_processed += info['processed']
                    total_throttled += info['throttle_count']

                # Set our thread processed # count for next time
                self.thread_stats[thread_name]['processed'] = info['processed']
                self.thread_stats[thread_name]['throttle_count'] = info['throttle_count']

                # Easier to work with variables
                queue_depth = int(info['queue_depth'])
                min_threads = self.config['Bindings'][info['binding']]['consumers']['min']
                max_threads = self.config['Bindings'][info['binding']]['consumers']['max']
                threshold = self.config['Bindings'][info['binding']]['consumers']['threshold']


                # If our queue depth exceeds the threshold and we haven't maxed out make a new worker
                if queue_depth > threshold and len(binding['threads']) < max_threads:

                    _logger.info( 'MCP: Spawning worker thread for connection "%s" binding "%s": %i messages pending, %i threshold, %i min, %i max, %i consumers active.' %
                                    ( info['connection'],
                                      info['binding'],
                                      queue_depth,
                                      threshold,
                                      min_threads,
                                      max_threads,
                                      len(binding['threads']) ) )

                    # Create the new thread making it use self.consume
                    new_thread = ConsumerThread( self.config,
                                                 info['binding'],
                                                 info['connection'] )

                    # Add to our dictionary of active threads
                    binding['threads'].append(new_thread)

                    # Start the thread
                    new_thread.start()

                    # We only want 1 new thread per poll as to not overwhelm the consumer system
                    break

                # Check if our queue depth is below our threshold and we have more than the min amount
                if queue_depth < threshold and len(binding['threads']) > min_threads:

                    _logger.info('MCP: Removing worker thread for connection "%s" binding "%s": %i messages pending, %i threshold, %i min, %i max, %i threads active.',
                                 info['connection'],
                                 info['binding'],
                                 queue_depth,
                                 threshold,
                                 min_threads,
                                 max_threads,
                                 len(binding['threads']))

                    # Remove a thread
                    thread =  binding['threads'].pop()

                    while thread.locked:
                        _logger.debug('MCP: Waiting on %s to unlock so we can shut it down', thread.getName())
                        time.sleep(1)

                    # Shutdown the thread gracefully
                    thread.shutdown()

                    # We only want to remove one thread per poll
                    break

            _logger.info('MCP: Binding #%i processed %i total messages in %.2f seconds at a rate of %.2f mps.  Average wait = %.2fms',
                         offset,
                         total_processed,
                         _duration_since_last_poll,
                         ( float(total_processed) / _duration_since_last_poll ),
                         (-1.0 if total_processed == 0 else (total_wait / total_processed)))

            if len(binding['threads']) > 1:
                _logger.info('MCP: Binding #%i has %i threads which throttled themselves %i times.' %
                          ( offset,
                            len(binding['threads']),
                            total_throttled ) )
            else:
                _logger.info('MCP: Binding #%i has 1 thread which throttled itself %i times.' %
                          ( offset, total_throttled ) )

            offset += 1

        self._monitoring_last_poll = time.time()

    def shutdown(self):
        """ Graceful shutdown of the MCP means shutting down threads too """
        
        _logger.debug( 'MCP: Master Control Program Shutting Down' )
        
        # Get the thread count
        threads = self.threadCount()
        
        # Keep track of the fact we're shutting down
        self.shutdown_pending = True
        
        # Loop as long as we have running threads
        while threads:
            
            # Loop through all of the bindings and try and shutdown their threads
            for binding in self.bindings:
                
                # Loop through all the threads in this binding
                for x in xrange(0, len(binding['threads'])):

                    # Let the thread know we want to shutdown
                    thread = binding['threads'].pop()
                    while not thread.shutdown():
                        _logger.debug('MCP: Waiting on %s to shutdown properly' % thread.getName())
                        time.sleep(1)

            # Get our updated thread count and only sleep then loop if it's > 0, 
            threads = self.threadCount()
            
            # If we have any threads left, sleep for a second before trying again
            if threads:
                _logger.debug( 'MCP: Waiting on %i threads to cleanly shutdown.' % threads )
                time.sleep(1)
                    
    def start(self):
        """ Initialize all of the consumer threads when the MCP comes to life """
        _logger.debug( 'MCP: Master Control Program Starting Up' )

        # Loop through all of the bindings
        for routing_key in self.config['Bindings']:
            
            # Create the dictionary values for this binding
            binding = {'name': routing_key,
                       'queue': self.config['Bindings'][routing_key]['queue'],
                       'threads': []}

            # For each connection, kick off the min consumers and start consuming
            for connect_name in self.config['Bindings'][routing_key]['connections']:
                for i in xrange( 0, self.config['Bindings'][routing_key]['consumers']['min'] ):
                    _logger.debug('MCP: Creating worker thread #%i for connection "%s" binding "%s"',
                                  i, connect_name, routing_key)

                    # Create the new thread making it use self.consume
                    thread = ConsumerThread(self.config, routing_key, connect_name)

                    # Start the thread
                    thread.start()

                    # Check to see if the thread is alive before adding it to our stack
                    if thread.isAlive():

                        # Add to our dictionary of active threads
                        binding['threads'].append(thread)

            # Append this binding to our binding stack
            self.bindings.append(binding)
        
    def threadCount(self):
        """ Return the total number of working threads managed by the MCP """
        
        count = 0
        for binding in self.bindings:
            count += len(binding['threads'])
        return count

def show_frames(logger):
    for threadId, stack in sys._current_frames().items():
        logger.info("# ThreadID: %s", threadId)
        for filename, lineno, name, line in traceback.extract_stack(stack):
            logger.info('  File: "%s", line %d, in %s', filename, lineno, name)
            if line:
                logger.info("    %s", line.strip())

def sighandler(signum, frame):
    global mcp, process

    if signum == signal.SIGQUIT:
        logger = logging.getLogger("rejected.framedump")
        logger.setLevel(_logger.info)
        logger.info('Caught SIGQUIT, received at:')
        for filename, lineno, name, line in traceback.extract_stack(frame):
            logger.info('File: "%s", line %d, in %s', filename, lineno, name)
            if line:
                logger.info("  %s", line.strip())
        logger.info("Dumping threads...")
        show_frames(logger)
        return True

    if signum == signal.SIGUSR1:
        level = _logger.info
    elif signum == signal.SIGUSR2:
        level = _logger.debug
    else:
        level = _logger.warn

    logger = logging.getLogger()

    logger.setLevel(_logger.info)
    _logger.info('rejected: *** Got signal %s. Setting level to %s.',
                 signum, level)

    logger.setLevel(level)
    return True

def shutdown(signum = 0, frame = None):
    """ Application Wide Graceful Shutdown """
    global mcp, process
    _IS_QUITTING = True

    _logger.info( 'Graceful shutdown of rejected.py running "%s" initiated.' % process )
    mcp.shutdown()
    _logger.debug( 'Graceful shutdown of rejected.py running "%s" complete' % process )
    os._exit(signum)

def main():
    """ Main Application Handler """
    global mcp, _MCP_POLL_DELAY, options, process
    
    usage = "usage: %prog [options]"
    version_string = "%%prog %s" % __version__
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

    parser.add_option("-d", "--detached",
                      action="store_true", dest="detached", default=False,
                      help="Run in daemon mode")

    parser.add_option("-f", "--foreground",
                      action="store_true", dest="single_thread", default=False,
                      help="use debug to stdout instead of logging settings\
                            and stay as single process in the foreground.")

    parser.add_option("-p", "--processor_base_path", dest="processor_base_path",
                      action="store", type="string", default=_PROCESSOR_BASE,
                      help="Specify the base path for processor packages.")
    
    # Parse our options and arguments                                                                        
    options, args = parser.parse_args()

    # Get our base path going for processor imports
    sys.path.insert(0, options.processor_base_path)

    # Get the config file only for logging options
    parts = options.config.split('/')
    process = parts[len(parts) - 1]
    parts = process.split('.')
    process = parts[0]
    
    # Load the Configuration file
    try:
        stream = file(options.config, 'r')
        config = yaml.load(stream)
        stream.close()
    except IOError:
        sys.stderr.write("\nError: Invalid or missing configuration file \"%s\"\n" % options.config)
        sys.exit(-1)
    
    # Set logging levels dictionary
    logging_levels = {'debug':    logging.DEBUG,
                      'info':     logging.INFO,
                      'warning':  logging.WARN,
                      'error':    logging.ERROR,
                      'critical': logging.CRITICAL}
    
    # Get the logging value from the dictionary
    logging_level = config['Logging']['level']
    config['Logging']['level'] = logging_levels.get(config['Logging']['level'], logging.NOTSET )

    # Set the processor base if specified
    if 'processor_base' in config:
        sys.path.insert(0, config['processor_base'])

    # If the user says verbose overwrite the settings.
    if options.single_thread:
    
        # Set the debugging level to verbose
        config['Logging']['level'] = logging.DEBUG
        
        # If we have specified a file, remove it so logging info goes to stdout
        if config['Logging'].has_key('filename'):
            del config['Logging']['filename']

    else:
        # Build a specific path to our log file
        if config['Logging'].has_key('filename'):
            config['Logging']['filename'] = os.path.join( os.path.dirname(__file__), 
                                                          config['Logging']['directory'], 
                                                          config['Logging']['filename'] )

    # Pass in our logging config
    logging.basicConfig(**config['Logging'])
    _logger.setLevel(config['Logging']['level'])
    _logger.info('Log level set to %s' % logging_level)

    # If we have supported handler
    if config['Logging'].has_key('handler'):
        
        # If we want to syslog
        if config['Logging']['handler'] == 'syslog':

            from logging.handlers import SysLogHandler

            # Create the syslog handler            
            logging_handler = SysLogHandler( address='/dev/log', facility = SysLogHandler.LOG_LOCAL6 )
            
            # Add the handler
            logger = logging.getLogger()
            logger.addHandler(logging_handler)
            logger.debug('Sending message')

    # Fork our process to detach if not told to stay in foreground
    if options.detached:
        try:
            pid = os.fork()
            if pid > 0:
                _logger.info('Parent process ending.')
                sys.exit(0)                        
        except OSError, e:
            sys.stderr.write("Could not fork: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)
        
        # Second fork to put into daemon mode
        try: 
            pid = os.fork() 
            if pid > 0:
                # exit from second parent, print eventual PID before
                sys.stdout.write('rejected.py daemon has started - PID # %d.\n' % pid)
                _logger.info('Child forked as PID # %d' % pid)
                sys.exit(0) 
        except OSError, e: 
            sys.stderr.write("Could not fork: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)
        
        # Let the debugging person know we've forked
        _logger.debug( 'rejected.py has forked into the background.' )
        
        # Detach from parent environment
        os.chdir( os.path.dirname(__file__) ) 
        os.setsid()
        os.umask(0) 

        # Close stdin            
        sys.stdin.close()
        
        # Redirect stdout, stderr
        sys.stdout = open(os.path.join(os.path.dirname(__file__), 
                          config['Logging']['directory'], "stdout.log"), 'w')
        sys.stderr = open(os.path.join(os.path.dirname(__file__), 
                          config['Logging']['directory'], "stderr.log"), 'w')
                                                 
    # Set our signal handler so we can gracefully shutdown
    signal.signal(signal.SIGTERM, shutdown)
    for signum in [signal.SIGQUIT, signal.SIGUSR1, signal.SIGUSR2, signal.SIGHUP]:
        signal.signal(signum, sighandler)

    # Start the Master Control Program ;-)
    mcp = MasterControlProgram(config)
    
    # Kick off our core connections
    mcp.start()
    
    # If our config has monitoring disabled but we enable via cli, enable it
    if 'Monitor' not in config:
        config['Monitor'] = dict()

    # Loop until someone wants us to stop
    do_poll = config['Monitor'].get('enabled', False) and 'interval' in config['Monitor']

    _MCP_POLL_DELAY = config['Monitor'].get('interval', _MCP_POLL_DELAY)
    _logger.debug('%s: MCP_POLL_DELAY set to %i seconds.', __file__, _MCP_POLL_DELAY)

    while True:
        
        # Have the Master Control Process poll
        try:

            # Sleep is so much more CPU friendly than pass
            time.sleep(_MCP_POLL_DELAY)

            # Check to see if we need to adjust our threads
            if do_poll:
                mcp.poll()
                _logger.debug('rejected.py:Thread Count: %i' % threading.active_count())

        except (KeyboardInterrupt, SystemExit):
            # The user has sent a kill or ctrl-c
            shutdown()
        
# Only execute the code if invoked as an application
if __name__ == '__main__':
    
    # Run the main function
    main()
