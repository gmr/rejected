"""
Base class patterns for interfacing with RabbitMQ and our applications
"""

class rejected_object(object):

    def __repr__(self):
        """
        Return a string representation of the class with its attributes and
        values as a dictionary
        """
        items = dict()
        for key, value in self.__dict__.iteritems():
            items[key] = str(value)
        return "<%s(%s)>" % (self.__class__.__name__, items)
