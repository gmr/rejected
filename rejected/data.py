"""
Rejected data objects

"""
import copy
import time
import uuid


class DataObject(object):
    """A class that will return a plain text representation of all of the
    attributes assigned to the object.

    """
    def __repr__(self):
        """Return a string representation of the object and all of its
        attributes.

        :rtype: str

        """
        items = list()
        for key, value in self.__dict__.iteritems():
            if getattr(self.__class__, key, None) != value:
                items.append('%s=%s' % (key, value))
        return "<%s(%s)>" % (self.__class__.__name__, items)


class Message(DataObject):
    """Class for containing all the attributes about a message object creating a
    flatter, move convenient way to access the data while supporting the legacy
    methods that were previously in place in rejected < 2.0

    """

    def __init__(self, channel, method, header, body):
        """Initialize a message setting the attributes from the given channel,
        method, header and body.

        :param pika.channel.Channel channel: The channel the msg was received on
        :param pika.frames.Method method: Pika Method Frame object
        :param pika.frames.Header header: Pika Header Frame object
        :param str body: Pika message body

        """
        DataObject.__init__(self)
        self.channel = channel
        self.method = method
        self.properties = Properties(header)
        self.body = copy.copy(body)

        # Map method properties
        self.consumer_tag = method.consumer_tag
        self.delivery_tag = method.delivery_tag
        self.exchange = method.exchange
        self.redelivered = method.redelivered
        self.routing_key = method.routing_key


class Properties(DataObject):
    """A class that represents all of the field attributes of AMQP's
    Basic.Properties

    """

    def __init__(self, header=None):
        """Create a base object to contain all of the properties we need

        :param pika.spec.BasicProperties header: A header object from Pika

        """
        DataObject.__init__(self)
        if header:
            self.app_id = header.app_id
            self.cluster_id = header.cluster_id
            self.content_type = header.content_type
            self.content_encoding = header.content_encoding
            self.correlation_id = header.correlation_id
            self.delivery_mode = header.delivery_mode
            self.expiration = header.expiration
            self.headers = copy.deepcopy(header.headers) or dict()
            self.priority = header.priority
            self.reply_to = header.reply_to
            self.message_id = header.message_id
            self.timestamp = header.timestamp
            self.type = header.type
            self.user_id = header.user_id
        else:
            self.app_id = None
            self.cluster_id = None
            self.content_type = 'text/text'
            self.content_encoding = None
            self.correlation_id = None
            self.delivery_mode = 1
            self.expiration = None
            self.headers = dict()
            self.message_id = str(uuid.uuid4())
            self.priority = None
            self.reply_to = None
            self.timestamp = int(time.time())
            self.type = None
            self.user_id = None
