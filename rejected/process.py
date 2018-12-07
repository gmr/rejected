"""
Consumer process management. Imports consumer code, manages RabbitMQ
connection state and collects stats about the consuming process.

"""
import collections
import importlib
import logging
import math
import multiprocessing
import os
from os import path
try:
    import cProfile as profile
except ImportError:
    import profile
import signal
import time
import warnings

try:
    import sprockets_influxdb as influxdb
except ImportError:
    influxdb = None
from tornado import gen, ioloop, locks
import pika
from pika import exceptions, spec
try:
    import raven
    from raven import breadcrumbs
    from raven.contrib.tornado import AsyncSentryClient
except ImportError:
    breadcrumbs, raven, AsyncSentryClient = None, None, None

from rejected import __version__, data, state, statsd, utils

LOGGER = logging.getLogger(__name__)

Callbacks = collections.namedtuple(
    'callbacks', ['on_ready', 'on_open_error', 'on_closed', 'on_blocked',
                  'on_unblocked', 'on_confirmation', 'on_delivery',
                  'on_return'])
Delivery = collections.namedtuple('Delivery', ['connection', 'message'])


class Connection(state.State):

    HB_INTERVAL = 300
    STATE_CLOSED = 0x08

    def __init__(self, name, config, consumer_name, should_consume,
                 publisher_confirmations, io_loop, callbacks):
        super(Connection, self).__init__()
        self.blocked = False
        self.callbacks = callbacks
        self.channel = None
        self.config = config
        self.should_consume = should_consume
        self.consumer_tag = '{}-{}'.format(consumer_name, os.getpid())
        self.io_loop = io_loop
        self.name = name
        self.publisher_confirm = publisher_confirmations
        self.handle = self.connect()

        # Override STOPPED with CLOSED
        self.STATES[0x08] = 'CLOSED'

    @property
    def is_closed(self):
        return self.is_stopped

    def connect(self):
        self.set_state(self.STATE_CONNECTING)
        return pika.TornadoConnection(
            self._connection_parameters,
            on_open_callback=self.on_open,
            on_open_error_callback=self.on_open_error,
            on_close_callback=self.on_closed,
            stop_ioloop_on_close=False,
            custom_ioloop=self.io_loop)

    def shutdown(self):
        if self.is_shutting_down:
            LOGGER.debug('Connection %s is already shutting down', self.name)
            return

        self.set_state(self.STATE_SHUTTING_DOWN)
        LOGGER.debug('Connection %s is shutting down', self.name)
        if self.is_active:
            LOGGER.debug('Connection %s is sending a Basic.Cancel to RabbitMQ',
                         self.name)
            self.channel.basic_cancel(self.on_consumer_cancelled,
                                      self.consumer_tag)
        else:
            self.channel.close()

    def on_open(self, _handle):
        """Invoked when the connection is opened

        :type _handle: pika.adapters.tornado_connection.TornadoConnection

        """
        LOGGER.debug('Connection %s is open', self.name)
        try:
            self.handle.channel(self.on_channel_open)
        except exceptions.ConnectionClosed:
            LOGGER.warning('Channel open on closed connection')
            self.set_state(self.STATE_CLOSED)
            self.callbacks.on_closed(self.name)
            return
        self.handle.add_on_connection_blocked_callback(self.on_blocked)
        self.handle.add_on_connection_unblocked_callback(self.on_unblocked)

    def on_open_error(self, *args, **kwargs):
        LOGGER.error('Connection %s failure %r %r', self.name, args, kwargs)
        self.set_state(self.STATE_CLOSED)
        self.callbacks.on_open_error(self.name)

    def on_closed(self, _connection, status_code, status_text):
        if self.is_connecting:
            LOGGER.error('Connection %s failure while connecting (%s): %s',
                         self.name, status_code, status_text)
            self.set_state(self.STATE_CLOSED)
            del self.handle
            return self.callbacks.on_open_error(self.name)
        self.set_state(self.STATE_CLOSED)
        LOGGER.info('Connection %s closed (%s) %s',
                    self.name, status_code, status_text)
        self.callbacks.on_closed(self.name)

    def on_blocked(self, frame):
        LOGGER.warning('Connection %s is blocked: %r', frame)
        self.blocked = True
        self.callbacks.on_blocked(self.name)

    def on_unblocked(self, frame):
        LOGGER.warning('Connection %s is unblocked: %r', frame)
        self.blocked = False
        self.callbacks.on_unblocked(self.name)

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened. It
        will change the state to IDLE, add the callbacks and setup the channel
        to start consuming.

        :param pika.channel.Channel channel: The channel object

        """
        LOGGER.debug('Connection %s channel is now open', self.name)
        self.set_state(self.STATE_IDLE)
        self.channel = channel
        self.channel.add_on_close_callback(self.on_channel_closed)
        self.channel.add_on_cancel_callback(self.on_consumer_cancelled)
        self.channel.add_on_return_callback(self.on_return)
        if self.publisher_confirm:
            self.channel.confirm_delivery(self.on_confirmation)
        self.callbacks.on_ready(self.name)

    def on_channel_closed(self, _channel, reply_code, reply_text):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel _channel: The AMQP Channel
        :param int reply_code: The AMQP reply code
        :param str reply_text: The AMQP reply text

        """
        LOGGER.debug('Connection %s channel was closed: (%s) %s',
                     self.name, reply_code, reply_text)
        del self.channel
        if self.is_running:
            self.set_state(self.STATE_CONNECTING)
            try:
                self.handle.channel(self.on_channel_open)
            except exceptions.ConnectionClosed:
                LOGGER.warning('ConnectionClosed raised in on_channel_open')
                self.set_state(self.STATE_CLOSED)
                self.handle.close()
                del self.handle
                self.callbacks.on_open_error(self.name)
        elif self.is_shutting_down:
            LOGGER.debug('Connection %s closing', self.name)
            self.handle.close()

    def consume(self, queue_name, no_ack, prefetch_count):
        self.set_state(self.STATE_ACTIVE)
        self.channel.basic_qos(self.on_qos_set, 0, prefetch_count, False)
        self.channel.basic_consume(consumer_callback=self.on_delivery,
                                   queue=queue_name,
                                   no_ack=no_ack,
                                   consumer_tag=self.consumer_tag)

    def on_qos_set(self, frame):
        """Invoked by pika when the QoS is set

        :param pika.frame.Frame frame: The QoS Frame

        """
        LOGGER.debug("Connection %s QoS was set: %r", self.name, frame)

    def on_consumer_cancelled(self, frame):
        """Invoked by pika when a ``Basic.Cancel`` or ``Basic.CancelOk``
        is received.

        :param pika.frame.Frame frame: The QoS Frame

        """
        LOGGER.info('Connection %s consumer has been cancelled', self.name)
        if not self.is_shutting_down:
            self.set_state(self.STATE_SHUTTING_DOWN)
        self.channel.close()

    def on_confirmation(self, frame):
        """Invoked by pika when RabbitMQ responds to a Basic.Publish RPC
        command, passing in either a Basic.Ack or Basic.Nack frame with
        the delivery tag of the message that was published. The delivery tag
        is an integer counter indicating the message number that was sent
        on the channel via Basic.Publish.

        :param pika.frame.Method frame: Basic.Ack or Basic.Nack frame

        """
        delivered = frame.method.NAME.split('.')[1].lower() == 'ack'
        LOGGER.debug('Connection %s received delivery confirmation '
                     '(Delivered: %s)', self.name, delivered)
        self.callbacks.on_confirmation(
            self.name, delivered, frame.method.delivery_tag)

    def on_delivery(self, channel, method, properties, body):
        self.callbacks.on_delivery(
            self.name, channel, method, properties, body)

    def on_return(self, channel, method, properties, body):
        self.callbacks.on_return(self.name, channel, method, properties, body)

    @property
    def _connection_parameters(self):
        """Return connection parameters for a pika connection.

        :rtype: pika.ConnectionParameters

        """
        return pika.ConnectionParameters(
            self.config.get('host', 'localhost'),
            self.config.get('port', 5672),
            self.config.get('vhost', '/'),
            pika.PlainCredentials(
                self.config.get('user', 'guest'),
                self.config.get('password', self.config.get('pass', 'guest'))),
            ssl=self.config.get('ssl', False),
            frame_max=self.config.get('frame_max', spec.FRAME_MAX_SIZE),
            socket_timeout=self.config.get('socket_timeout', 10),
            heartbeat_interval=self.config.get(
                'heartbeat_interval', self.HB_INTERVAL))


class Process(multiprocessing.Process, state.State):
    """Core process class that manages the consumer object and communicates
    with RabbitMQ.

    """
    AMQP_APP_ID = 'rejected/%s' % __version__

    # Additional State constants
    STATE_PROCESSING = 0x04

    # Counter constants
    ACKED = 'acked'
    CLOSED_ON_COMPLETE = 'closed_on_complete'
    DROPPED = 'dropped'
    ERROR = 'failed'
    FAILURES = 'failures_until_stop'
    NACKED = 'nacked'
    PROCESSED = 'processed'
    REQUEUED = 'requeued'
    REDELIVERED = 'redelivered'
    TIME_SPENT = 'processing_time'
    TIME_WAITED = 'idle_time'

    CONSUMER_EXCEPTION = 'consumer_exception'
    MESSAGE_EXCEPTION = 'message_exception'
    PROCESSING_EXCEPTION = 'processing_exception'
    UNHANDLED_EXCEPTION = 'unhandled_exception'

    QOS_PREFETCH_COUNT = 1
    MAX_ERROR_COUNT = 5
    MAX_ERROR_WINDOW = 60
    MAX_SHUTDOWN_WAIT = 5

    def __init__(self,
                 group=None,
                 target=None,
                 name=None,
                 args=(),
                 kwargs=None):
        if kwargs is None:
            kwargs = {}
        super(Process, self).__init__(group, target, name, args, kwargs)
        self.active_message = None
        self.callbacks = Callbacks(self.on_connection_ready,
                                   self.on_connection_failure,
                                   self.on_connection_closed,
                                   self.on_connection_blocked,
                                   self.on_connection_unblocked,
                                   self.on_confirmation,
                                   self.on_delivery,
                                   self.on_returned)
        self.connections = {}
        self.consumer = None
        self.consumer_lock = None
        self.consumer_version = None
        self.counters = collections.Counter()

        self.delivery_time = None
        self.influxdb = None
        self.ioloop = None
        self.last_failure = 0
        self.last_stats_time = None
        self.measurement = None
        self.message_connection_id = None
        self.pending = collections.deque()
        self.prepend_path = None
        self.previous = None
        self.sentry_client = None
        self.state = self.STATE_INITIALIZING
        self.state_start = time.time()
        self.statsd = None

        # Override ACTIVE with PROCESSING
        self.STATES[0x04] = 'Processing'

    def ack_message(self, message):
        """Acknowledge the message on the broker and log the ack

        :param message: The message to acknowledge
        :type message: rejected.data.Message

        """
        if not self.connections[message.connection].is_running:
            LOGGER.warning('Can not ack message, disconnected from RabbitMQ')
            self.counters[self.CLOSED_ON_COMPLETE] += 1
            return
        LOGGER.debug('Acking %s', message.delivery_tag)
        message.channel.basic_ack(delivery_tag=message.delivery_tag)
        self.counters[self.ACKED] += 1
        self.measurement.set_tag(self.ACKED, True)

    def calc_velocity(self, values):
        """Return the message consuming velocity for the process.

        :param dict values: The dict with velocity data
        :rtype: float

        """
        processed = (values['counts'].get(self.PROCESSED, 0) -
                     values['previous'].get(self.PROCESSED, 0))
        duration = time.time() - self.last_stats_time

        # If there were no messages, do not calculate, use the base
        if not processed or not duration:
            return 0

        # Calculate the velocity as the basis for the calculation
        velocity = float(processed) / float(duration)
        LOGGER.debug('Message processing velocity: %.2f/s', velocity)
        return velocity

    def create_connections(self):
        """Create and start the RabbitMQ connections, assigning the connection
        object to the connections dict.

        """
        self.set_state(self.STATE_CONNECTING)
        for connection in self.consumer_config.get('connections', []):
            name, confirm, consume = connection, False, True
            if isinstance(connection, dict):
                name = connection['name']
                confirm = connection.get('publisher_confirmation', False)
                consume = connection.get('consume', True)

            if name not in self.config['Connections']:
                LOGGER.critical('Connection "%s" for %s not found',
                                name, self.consumer_name)
                continue

            self.connections[name] = Connection(
                name, self.config['Connections'][name], self.consumer_name,
                consume, confirm, self.ioloop, self.callbacks)

    @staticmethod
    def get_config(cfg, number, name, connection):
        """Initialize a new consumer thread, setting defaults and config values

        :param dict cfg: Consumer config section from YAML File
        :param int number: The identification number for the consumer
        :param str name: The name of the consumer
        :param str connection: The name of the connection):
        :rtype: dict

        """
        return {
            'connection': cfg['Connections'][connection],
            'consumer_name': name,
            'process_name': '%s_%i_tag_%i' % (name, os.getpid(), number)
        }

    def get_consumer(self, cfg):
        """Import and create a new instance of the configured message consumer.

        :param dict cfg: The named consumer section of the configuration
        :rtype: instance
        :raises: ImportError

        """
        try:
            handle, version = utils.import_consumer(cfg['consumer'])
        except ImportError as error:
            LOGGER.exception('Error importing the consumer %s: %s',
                             cfg['consumer'], error)
            return

        if version:
            LOGGER.info('Creating consumer %s v%s', cfg['consumer'], version)
            self.consumer_version = version
        else:
            LOGGER.info('Creating consumer %s', cfg['consumer'])

        settings = cfg.get('config', dict())
        settings['_import_module'] = '.'.join(cfg['consumer'].split('.')[0:-1])

        kwargs = {
            'settings': settings,
            'process': self,
            'drop_exchange': cfg.get('drop_exchange'),
            'drop_invalid_messages': cfg.get('drop_invalid_messages'),
            'message_type': cfg.get('message_type'),
            'error_exchange': cfg.get('error_exchange'),
            'error_max_retry': cfg.get('error_max_retry')
        }

        try:
            return handle(**kwargs)
        except Exception as error:
            LOGGER.exception('Error creating the consumer "%s": %s',
                             cfg['consumer'], error)

    @gen.engine
    def invoke_consumer(self, message):
        """Wrap the actual processor processing bits

        :param rejected.data.Message message: The message to process

        """
        # Only allow for a single message to be processed at a time
        with (yield self.consumer_lock.acquire()):
            if self.is_idle:
                self.set_state(self.STATE_PROCESSING)
                self.delivery_time = start_time = time.time()
                self.active_message = message

                self.measurement = data.Measurement()

                if message.method.redelivered:
                    self.counters[self.REDELIVERED] += 1
                    self.measurement.set_tag(self.REDELIVERED, True)

                try:
                    result = yield self.consumer.execute(message,
                                                         self.measurement)
                except Exception as error:
                    LOGGER.exception('Unhandled exception from consumer in '
                                     'process. This should not happen. %s',
                                     error)
                    result = data.MESSAGE_REQUEUE

                LOGGER.debug('Finished processing message: %r', result)
                self.on_processed(message, result, start_time)
            elif self.is_waiting_to_shutdown:
                LOGGER.info(
                    'Requeueing pending message due to pending shutdown')
                self.reject(message, True)
                self.shutdown_connections()
            elif self.is_shutting_down:
                LOGGER.info('Requeueing pending message due to shutdown')
                self.reject(message, True)
                self.on_ready_to_stop()
            else:
                LOGGER.warning('Exiting invoke_consumer without processing, '
                               'this should not happen. State: %s',
                               self.state_description)
        if self.pending:
            self.ioloop.add_callback(
                self.invoke_consumer, self.pending.popleft())

    @property
    def is_processing(self):
        """Returns a bool specifying if the consumer is currently processing

        :rtype: bool

        """
        return self.state in [self.STATE_PROCESSING, self.STATE_STOP_REQUESTED]

    def maybe_submit_measurement(self):
        """Check for configured instrumentation backends and if found, submit
        the message measurement info.

        """
        if self.statsd:
            self.submit_statsd_measurements()
        if self.influxdb:
            self.submit_influxdb_measurement()

    def on_connection_closed(self, name):
        if self.is_running:
            LOGGER.warning('Connection %s was closed, reconnecting', name)
            return self.connections[name].connect()

        ready = all([c.is_closed for c in self.connections.values()])
        if (self.is_shutting_down or self.is_waiting_to_shutdown) and ready:
            self.on_ready_to_stop()

    def on_connection_failure(self, *args, **kwargs):
        LOGGER.warning('Connection failure %r %r', args, kwargs)
        ready = all([c.is_closed for c in self.connections.values()])
        if (self.is_connecting or
                self.is_shutting_down or
                self.is_waiting_to_shutdown) and ready:
            self.on_ready_to_stop()

    def on_connection_ready(self, name):
        LOGGER.debug('Connection %s indicated it is ready', name)
        self.consumer.set_channel(name, self.connections[name].channel)
        if all([c.is_idle for c in self.connections.values()]):
            for key in self.connections.keys():
                if self.connections[key].should_consume:
                    self.connections[key].consume(
                        self.queue_name, self.no_ack, self.qos_prefetch)
            if self.is_connecting:
                self.set_state(self.STATE_IDLE)

    def on_connection_blocked(self, name):
        LOGGER.warning('Connection %s blocked', name)
        if self.is_processing:
            self.consumer.on_blocked(name)

    def on_connection_unblocked(self, name):
        LOGGER.info('Connection %s unblocked', name)
        if self.is_processing:
            self.consumer.on_blocked(name)

    def on_confirmation(self, name, delivered, delivery_tag):
        if self.is_processing:
            self.consumer.on_confirmation(name, delivered, delivery_tag)

    def on_delivery(self, name, channel, method, properties, body):
        """Process a message from Rabbit

        :param str name: The connection name
        :param pika.channel.Channel channel: The message's delivery channel
        :param pika.frames.MethodFrame method: The method frame
        :param pika.spec.BasicProperties properties: The message properties
        :param str body: The message body

        """
        message = data.Message(name, channel, method, properties, body, False)
        if self.is_processing:
            self.pending.append(message)
        else:
            self.invoke_consumer(message)

    def on_returned(self, name, channel, method, properties, body):
        """Send a message to the consumer that was returned by RabbitMQ

        :param str name: The connection name
        :param channel: The channel the message was returned on
        :type channel: pika.channel.Channel channel:
        :param pika.frames.MethodFrame method: The method frame
        :param pika.spec.BasicProperties properties: The message properties
        :param str body: The message body

        """
        message = data.Message(name, channel, method, properties, body, True)
        if self.is_processing:
            self.pending.append(message)
        else:
            self.invoke_consumer(message)

    def on_processed(self, message, result, start_time):
        """Invoked after a message is processed by the consumer and
        implements the logic for how to deal with a message based upon
        the result.

        :param rejected.data.Message message: The message that was processed
        :param int result: The result of the processing of the message
        :param float start_time: When the message was received

        """
        duration = max(start_time, time.time()) - start_time
        self.counters[self.TIME_SPENT] += duration
        self.measurement.add_duration(self.TIME_SPENT, duration)

        if result == data.MESSAGE_DROP:
            LOGGER.debug('Rejecting message due to drop return from consumer')
            self.reject(message, False)
            self.counters[self.DROPPED] += 1

        elif result == data.MESSAGE_EXCEPTION:
            LOGGER.debug('Rejecting message due to MessageException')
            self.reject(message, False)
            self.counters[self.MESSAGE_EXCEPTION] += 1

        elif result == data.PROCESSING_EXCEPTION:
            LOGGER.debug('Rejecting message due to ProcessingException')
            if self.consumer.ACK_PROCESSING_EXCEPTIONS:
                self.ack_message(message)
            else:
                self.reject(message, False)
            self.counters[self.PROCESSING_EXCEPTION] += 1

        elif result == data.CONSUMER_EXCEPTION:
            LOGGER.debug('Re-queueing message due to ConsumerException')
            self.reject(message, True)
            self.on_processing_error()
            self.counters[self.CONSUMER_EXCEPTION] += 1

        elif result == data.UNHANDLED_EXCEPTION:
            LOGGER.debug('Re-queueing message due to UnhandledException')
            self.reject(message, True)
            self.on_processing_error()
            self.counters[self.UNHANDLED_EXCEPTION] += 1

        elif result == data.MESSAGE_REQUEUE:
            LOGGER.debug('Re-queueing message due Consumer request')
            self.reject(message, True)
            self.counters[self.REQUEUED] += 1

        elif result == data.MESSAGE_ACK and not self.no_ack:
            self.ack_message(message)

        self.counters[self.PROCESSED] += 1
        self.measurement.set_tag(self.PROCESSED, True)
        self.maybe_submit_measurement()
        self.reset_state()

    def on_processing_error(self):
        """Called when message processing failure happens due to a
        ConsumerException or an unhandled exception.

        """
        duration = time.time() - self.last_failure
        if duration > self.MAX_ERROR_WINDOW:
            LOGGER.info('Resetting failure window, %i seconds since last',
                        duration)
            self.reset_error_counter()
        self.counters[self.ERROR] += 1
        self.last_failure = time.time()
        if self.too_many_errors:
            LOGGER.critical('Error threshold exceeded (%i), shutting down',
                            self.counters[self.ERROR])
            self.shutdown_connections()

    def on_ready_to_stop(self):
        """Invoked when the consumer is ready to stop."""

        # Set the state to shutting down if it wasn't set as that during loop
        self.set_state(self.STATE_SHUTTING_DOWN)

        # Reset any signal handlers
        signal.signal(signal.SIGABRT, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGPROF, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)

        # Allow the consumer to gracefully stop and then stop the IOLoop
        if self.consumer:
            self.stop_consumer()

        # Clear IOLoop constructs
        self.consumer_lock = None

        # Stop the IOLoop
        if self.ioloop:
            LOGGER.debug('Stopping IOLoop')
            self.ioloop.stop()

        # Note that shutdown is complete and set the state accordingly
        self.set_state(self.STATE_STOPPED)
        LOGGER.info('Shutdown complete')

    def on_sigprof(self, _unused_signum, _unused_frame):
        """Called when SIGPROF is sent to the process, will dump the stats, in
        future versions, queue them for the master process to get data.

        :param int _unused_signum: The signal number
        :param frame _unused_frame: The python frame the signal was received at

        """
        self.stats_queue.put(self.report_stats(), True)
        self.last_stats_time = time.time()
        signal.siginterrupt(signal.SIGPROF, False)

    def on_startup_error(self, error):
        """Invoked when a pre-condition for starting the consumer has failed.
        Log the error and then exit the process.

        """
        LOGGER.critical('Could not start %s: %s', self.consumer_name, error)
        self.set_state(self.STATE_STOPPED)

    def reject(self, message, requeue=True):
        """Reject the message on the broker and log it.

        :param message: The message to reject
        :type message: rejected.Data.message
        :param bool requeue: Specify if the message should be re-queued or not

        """
        if self.no_ack:
            raise RuntimeError('Can not rejected messages when ack is False')

        if not self.connections[message.connection].is_running:
            LOGGER.warning('Can not nack message, disconnected from RabbitMQ')
            self.counters[self.CLOSED_ON_COMPLETE] += 1
            return

        LOGGER.warning('Rejecting message %s %s requeue', message.delivery_tag,
                       'with' if requeue else 'without')
        message.channel.basic_nack(
            delivery_tag=message.delivery_tag, requeue=requeue)
        self.measurement.set_tag(self.NACKED, True)
        self.measurement.set_tag(self.REQUEUED, requeue)

    def report_stats(self):
        """Create the dict of stats data for the MCP stats queue"""
        if not self.previous:
            self.previous = dict()
            for key in self.counters:
                self.previous[key] = 0
        values = {
            'name': self.name,
            'consumer_name': self.consumer_name,
            'counts': dict(self.counters),
            'previous': dict(self.previous)
        }
        self.previous = dict(self.counters)
        return values

    def reset_error_counter(self):
        """Reset the error counter to 0"""
        LOGGER.debug('Resetting the error counter')
        self.counters[self.ERROR] = 0

    def reset_state(self):
        """Reset the runtime state after processing a message to either idle
        or shutting down based upon the current state.

        """
        self.active_message = None
        self.measurement = None
        if self.is_waiting_to_shutdown:
            self.set_state(self.STATE_SHUTTING_DOWN)
            self.shutdown_connections()
        elif self.is_processing:
            self.set_state(self.STATE_IDLE)
        elif self.is_idle or self.is_connecting or self.is_shutting_down:
            pass
        else:
            LOGGER.critical('Unexepected state: %s', self.state_description)
        LOGGER.debug('State reset to %s (%s in pending)',
                     self.state_description, len(self.pending))

    def run(self):
        """Start the consumer"""
        if self.profile_file:
            LOGGER.info('Profiling to %s', self.profile_file)
            profile.runctx('self._run()', globals(), locals(),
                           self.profile_file)
        else:
            self._run()
        LOGGER.debug('Exiting %s (%i, %i)', self.name, os.getpid(),
                     os.getppid())

    def _run(self):
        """Run method that can be profiled"""
        self.set_state(self.STATE_INITIALIZING)
        self.ioloop = ioloop.IOLoop.current()
        self.consumer_lock = locks.Lock()

        self.sentry_client = self.setup_sentry(
            self._kwargs['config'], self.consumer_name)

        try:
            self.setup()
        except (AttributeError, ImportError) as error:
            LOGGER.exception('Setup failure: %s', error)
            return self.on_startup_error(
                'Failed to import the Python module for {}'.format(
                    self.consumer_name))

        if not self.is_stopped:
            try:
                self.ioloop.start()
            except KeyboardInterrupt:
                LOGGER.warning('CTRL-C while waiting for clean shutdown')

    def send_exception_to_sentry(self, exc_info):
        """Send an exception to Sentry if enabled.

        :param tuple exc_info: exception information as returned from
            :func:`sys.exc_info`

        """
        if not self.sentry_client:
            LOGGER.debug('No sentry_client, aborting')
            return

        message = dict(self.active_message)
        try:
            duration = math.ceil(time.time() - self.delivery_time) * 1000
        except TypeError:
            duration = 0
        kwargs = {'extra': {
                      'consumer_name': self.consumer_name,
                      'env': dict(os.environ),
                      'message': message},
                  'time_spent': duration}
        LOGGER.debug('Sending exception to sentry: %r', kwargs)
        self.sentry_client.captureException(exc_info, **kwargs)

    def setup(self):
        """Initialize the consumer, setting up needed attributes and connecting
        to RabbitMQ.

        """
        LOGGER.info('Initializing for %s', self.name)

        if 'consumer' not in self.consumer_config:
            return self.on_startup_error(
                '"consumer" not specified in configuration')

        self.consumer = self.get_consumer(self.consumer_config)

        if not self.consumer:
            return self.on_startup_error(
                'Could not import "{}"'.format(
                    self.consumer_config.get(
                        'consumer', 'unconfigured consumer')))

        self.setup_instrumentation()
        self.reset_error_counter()
        self.setup_sighandlers()
        self.create_connections()

    def setup_influxdb(self, config):
        """Configure the InfluxDB module for measurement submission.

        :param dict config: The InfluxDB configuration stanza

        """
        base_tags = {
            'version': self.consumer_version
        }
        measurement = self.config.get('influxdb_measurement',
                                      os.environ.get('SERVICE'))
        if measurement != self.consumer_name:
            base_tags['consumer'] = self.consumer_name
        for key in {'ENVIRONMENT', 'SERVICE'}:
            if key in os.environ:
                base_tags[key.lower()] = os.environ[key]
        influxdb.install(
            '{}://{}:{}/write'.format(
                config.get('scheme',
                           os.environ.get('INFLUXDB_SCHEME', 'http')),
                config.get('host',
                           os.environ.get('INFLUXDB_HOST', 'localhost')),
                config.get('port', os.environ.get('INFLUXDB_PORT', '8086'))
            ),
            config.get('user', os.environ.get('INFLUXDB_USER')),
            config.get('password', os.environ.get('INFLUXDB_PASSWORD')),
            base_tags=base_tags)
        return config.get('database', 'rejected'), measurement

    def setup_instrumentation(self):
        """Configure instrumentation for submission per message measurements
        to statsd and/or InfluxDB.

        """
        if not self.config.get('stats') and not self.config.get('statsd'):
            return

        if 'stats' not in self.config:
            self.config['stats'] = {}

        # Backwards compatible statsd config support
        if self.config.get('statsd'):
            warnings.warn('Deprecated statsd configuration detected',
                          DeprecationWarning)
            self.config['stats'].setdefault('statsd',
                                            self.config.get('statsd'))

        if self.config['stats'].get('statsd'):
            if self.config['stats']['statsd'].get('enabled', True):
                self.statsd = statsd.Client(self.consumer_name,
                                            self.config['stats']['statsd'])
            LOGGER.debug('statsd measurements configured')

        # InfluxDB support
        if influxdb and self.config['stats'].get('influxdb'):
            if self.config['stats']['influxdb'].get('enabled', True):
                self.influxdb = self.setup_influxdb(
                    self.config['stats']['influxdb'])
            LOGGER.debug('InfluxDB measurements configured: %r', self.influxdb)

    def setup_sentry(self, cfg, consumer_name):
        # Setup the Sentry client if configured and installed
        sentry_dsn = cfg['Consumers'][consumer_name].get('sentry_dsn',
                                                         cfg.get('sentry_dsn'))
        if not raven or not sentry_dsn:
            return
        consumer = cfg['Consumers'][consumer_name]['consumer'].split('.')[0]
        kwargs = {
            'exclude_paths': [],
            'include_paths':
                ['pika', 'rejected', 'raven', 'tornado', consumer],
            'ignore_exceptions': ['rejected.consumer.ConsumerException',
                                  'rejected.consumer.MessageException',
                                  'rejected.consumer.ProcessingException'],
            'processors': ['raven.processors.SanitizePasswordsProcessor']
        }

        if os.environ.get('ENVIRONMENT'):
            kwargs['environment'] = os.environ['ENVIRONMENT']

        if self.consumer_version:
            kwargs['version'] = self.consumer_version

        for logger in {'pika', 'pika.channel', 'pika.connection',
                       'pika.callback', 'pika.heartbeat',
                       'rejected.process', 'rejected.state'}:
            breadcrumbs.ignore_logger(logger)

        return AsyncSentryClient(sentry_dsn, **kwargs)

    def setup_sighandlers(self):
        """Setup the stats and stop signal handlers."""
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)

        signal.signal(signal.SIGPROF, self.on_sigprof)
        signal.signal(signal.SIGABRT, self.stop)

        signal.siginterrupt(signal.SIGPROF, False)
        signal.siginterrupt(signal.SIGABRT, False)
        LOGGER.debug('Signal handlers setup')

    def shutdown_connections(self):
        """This method closes the connections to RabbitMQ."""
        if not self.is_shutting_down:
            self.set_state(self.STATE_SHUTTING_DOWN)
        for name in self.connections:
            if self.connections[name].is_running:
                self.connections[name].shutdown()

    def stop(self, signum=None, _unused=None):
        """Stop the consumer from consuming by calling BasicCancel and setting
        our state.

        :param int signum: The signal received
        :param frame _unused: The stack frame from when the signal was called

        """
        LOGGER.debug('Stop called in state: %s', self.state_description)
        if self.is_stopped:
            LOGGER.warning('Stop requested but consumer is already stopped')
            return
        elif self.is_shutting_down:
            LOGGER.warning('Stop requested, consumer is already shutting down')
            return
        elif self.is_waiting_to_shutdown:
            LOGGER.warning('Stop requested but already waiting to shut down')
            return

        # Stop consuming and close AMQP connections
        self.shutdown_connections()

        # Wait until the consumer has finished processing to shutdown
        if self.is_processing:
            LOGGER.info('Waiting for consumer to finish processing')
            self.set_state(self.STATE_STOP_REQUESTED)
            if signum == signal.SIGTERM:
                signal.siginterrupt(signal.SIGTERM, False)
            return

    def stop_consumer(self):
        """Stop the consumer object and allow it to do a clean shutdown if it
        has the ability to do so.

        """
        try:
            LOGGER.info('Shutting down the consumer')
            self.consumer.shutdown()
        except AttributeError:
            LOGGER.debug('Consumer does not have a shutdown method')

    def submit_influxdb_measurement(self):
        """Submit a measurement for a message to InfluxDB"""
        measurement = influxdb.Measurement(*self.influxdb)
        measurement.set_timestamp(time.time())
        for key, value in self.measurement.counters.items():
            measurement.set_field(key, value)
        for key, value in self.measurement.tags.items():
            measurement.set_tag(key, value)
        for key, value in self.measurement.values.items():
            measurement.set_field(key, value)

        for key, values in self.measurement.durations.items():
            if len(values) == 1:
                measurement.set_field(key, values[0])
            elif len(values) > 1:
                measurement.set_field('{}-average'.format(key),
                                      sum(values) / len(values))
                measurement.set_field('{}-max'.format(key), max(values))
                measurement.set_field('{}-min'.format(key), min(values))
                measurement.set_field('{}-median'.format(key),
                                      utils.percentile(values, 50))
                measurement.set_field('{}-95th'.format(key),
                                      utils.percentile(values, 95))

        influxdb.add_measurement(measurement)
        LOGGER.debug('InfluxDB Measurement: %r', measurement.marshall())

    def submit_statsd_measurements(self):
        """Submit a measurement for a message to statsd as individual items."""
        for key, value in self.measurement.counters.items():
            self.statsd.incr(key, value)
        for key, values in self.measurement.durations.items():
            for value in values:
                self.statsd.add_timing(key, value)
        for key, value in self.measurement.values.items():
            self.statsd.set_gauge(key, value)
        for key, value in self.measurement.tags.items():
            if isinstance(value, bool):
                if value:
                    self.statsd.incr(key)
            elif isinstance(value, str):
                if value:
                    self.statsd.incr('{}.{}'.format(key, value))
            elif isinstance(value, int):
                self.statsd.incr(key, value)
            else:
                LOGGER.warning('The %s value type of %s is unsupported',
                               key, type(value))

    @property
    def active_consumers(self):
        return len([c for c in self.connections.values()
                    if c.should_consume and c.is_active()])

    @property
    def config(self):
        return self._kwargs['config']

    @property
    def consumer_config(self):
        return self.config['Consumers'][self.consumer_name] or {}

    @property
    def consumer_name(self):
        return self._kwargs['consumer_name']

    @property
    def expected_consumers(self):
        return len([c for c in self.connections.values() if c.should_consume])

    @property
    def logging_config(self):
        return self._kwargs['logging_config']

    @property
    def max_error_count(self):
        return int(self.consumer_config.get('max_errors',
                                            self.MAX_ERROR_COUNT))

    @property
    def no_ack(self):
        return not self.consumer_config.get('ack', True)

    @property
    def profile_file(self):
        """Return the full path to write the cProfile data

        :return: str

        """
        if not self._kwargs['profile']:
            return None
        if os.path.exists(self._kwargs['profile']) and \
                os.path.isdir(self._kwargs['profile']):
            return '%s/%s-%s.prof' % (path.normpath(self._kwargs['profile']),
                                      os.getpid(),
                                      self._kwargs['consumer_name'])
        return None

    @property
    def qos_prefetch(self):
        """Return the base, configured QoS prefetch value.

        :rtype: int

        """
        return self.consumer_config.get(
            'qos_prefetch', self.QOS_PREFETCH_COUNT)

    @property
    def queue_name(self):
        return self.consumer_config.get('queue', self.name)

    @property
    def stats_queue(self):
        return self._kwargs['stats_queue']

    @property
    def too_many_errors(self):
        """Return a bool if too many errors have occurred.

        :rtype: bool

        """
        return self.counters[self.ERROR] >= self.max_error_count
