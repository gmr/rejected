"""Generate test messages for the example consumer."""
import random
import time
import uuid

from pika.adapters import BlockingConnection
from pika.connection import ConnectionParameters
from pika import BasicProperties

MESSAGE_COUNT = 100

HTML_VALUE = '<html><head><title>Hi</title></head><body>Hello %i</body></html>'
JSON_VALUE = '{"json_encoded": true, "value": "here", "random": %i}'
XML_VALUE = '<?xml version="1.0"><document><node><item>True</item><other attr' \
            '="foo">Bar</other><value>%i</value></node></document>'
YAML_VALUE = """%%YAML 1.2
---
Application:
  poll_interval: 10.0
  log_stats: True
  name: Example
  value: %i
"""

if __name__ == '__main__':
    connection = BlockingConnection(ConnectionParameters())
    channel = connection.channel()
    channel.exchange_declare(
        exchange='examples', exchange_type='topic', durable=True)
    print('Declared the examples exchange')

    channel.queue_declare(queue='sync_example', durable=True,
                          exclusive=False, auto_delete=False)
    channel.queue_bind(exchange='examples', queue='sync_example',
                       routing_key='example.sync')
    print('Declared and bound sync_example')

    channel.queue_declare(queue='async_example', durable=True,
                          exclusive=False, auto_delete=False)
    channel.queue_bind(exchange='examples', queue='async_example',
                       routing_key='example.async')
    print('Declared and bound async_example')

    for iteration in range(0, MESSAGE_COUNT):
        msg_type = random.randint(1, 4)
        if msg_type == 1:
            body = HTML_VALUE % random.randint(1, 32768)
            content_type = 'text/html'
        elif msg_type == 2:
            body = JSON_VALUE % random.randint(1, 32768)
            content_type = 'application/json'
        elif msg_type == 3:
            body = XML_VALUE % random.randint(1, 32768)
            content_type = 'text/xml'
        elif msg_type == 4:
            body = YAML_VALUE % random.randint(1, 32768)
            content_type = 'text/x-yaml'
        else:
            body = 'Plain text value %i' % random.randint(1, 32768)
            content_type = 'text/text'
        properties = BasicProperties(app_id=__file__,
                                     user_id='guest',
                                     content_type=content_type,
                                     message_id=str(uuid.uuid4()),
                                     timestamp=int(time.time()),
                                     type='example',
                                     delivery_mode=1)
        channel.basic_publish(exchange='examples',
                              routing_key='example.sync',
                              body=body,
                              properties=properties)
    print('Published {} messages for the sync example'.format(MESSAGE_COUNT))

    channel.basic_publish(exchange='examples',
                          routing_key='example.async',
                          body='async seed message',
                          properties=BasicProperties(
                              app_id=__file__,
                              content_type='basic/plain',
                              delivery_mode=1,
                              message_id=str(uuid.uuid4()),
                              timestamp=int(time.time()),
                              type='example',
                              user_id='guest'))
    print('Published one seed message for the async example')

    connection.close()
