"""Example Rejected Consumer"""
from rejected import consumer
import logging

logger = logging.getLogger(__name__)


class Consumer(consumer.Consumer):

    def _process(self):
        """Extend this method to control what you do with a message when it's
        received. If the auto-decoding and auto-deserialization isn't for you,
        just access the self._message value, as the deserialization and decoding
        are done on demand, not when a message is received.

        """
        logger.info('Received message %s, a %s message: %r',
                    self.message_id, self.message_type, self.message_body)

        # Since the example messages publish vary in type, redefine the
        # content-type to JSON so it's auto-encoded on the way out
        properties = self.properties
        properties.content_type = 'application/json'
        properties.type = 'Response message'

        # Reply to the message using the message reply_to value in properties
        self.reply({'processed': True}, properties)
