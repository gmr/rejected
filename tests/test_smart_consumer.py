import bz2
import csv
import json
import logging
import mock
import pickle
import plistlib
import uuid
import zlib

import bs4
from rejected import consumer, testing
from tornado import gen
import umsgpack
import yaml

LOGGER = logging.getLogger(__name__)


class TestConsumer(consumer.SmartConsumer):
    def process(self):
        self.logger.info('Body: %r', self.body)


class ConsumerTestCase(testing.AsyncTestCase):
    def get_consumer(self):
        return TestConsumer

    @testing.gen_test
    def test_csv(self):
        body = 'foo,bar,baz\n1,2,3\n4,5,6\n'
        expectation = [{
            'foo': '1',
            'bar': '2',
            'baz': '3'
        }, {
            'foo': '4',
            'bar': '5',
            'baz': '6'
        }]
        yield self.process_message(body, 'text/csv')
        self.assertIsInstance(self.consumer.body, csv.DictReader)
        for offset, row in enumerate(self.consumer.body):
            self.assertDictEqual(expectation[offset], row)

    @testing.gen_test
    def test_json(self):
        payload = {'id': str(uuid.uuid4())}
        yield self.process_message(json.dumps(payload), 'application/json')
        self.assertDictEqual(self.consumer.body, payload)

    @testing.gen_test
    def test_msgpack(self):
        payload = {'id': str(uuid.uuid4())}
        yield self.process_message(
            umsgpack.packb(payload), 'application/msgpack')
        self.assertDictEqual(self.consumer.body, payload)

    @testing.gen_test
    def test_opaque_payload(self):
        body = str(uuid.uuid4())
        yield self.process_message(body, 'text/plain')
        self.assertEqual(self.consumer.body, body)

    @testing.gen_test
    def test_pickle(self):
        payload = {'id': str(uuid.uuid4())}
        yield self.process_message(
            pickle.dumps(payload), 'application/x-vnd.python.pickle')
        self.assertDictEqual(self.consumer.body, payload)

    @testing.gen_test
    def test_plist(self):
        payload = {'id': str(uuid.uuid4())}
        if hasattr(plistlib, 'writePlistToString'):
            body = plistlib.writePlistToString(payload)
        elif hasattr(plistlib, 'dumps'):
            body = plistlib.dumps(payload)
        else:
            raise AssertionError('Missing plistlib method')
        yield self.process_message(body, 'application/x-plist')
        self.assertDictEqual(self.consumer.body, payload)

    @testing.gen_test
    def test_ignored_application_content_type(self):
        content_type = 'application/octet-stream'
        with mock.patch.object(self.consumer.logger, 'debug') as debug:
            yield self.process_message(content_type, content_type)
            self.assertEqual(self.consumer.content_type, content_type)
            self.assertNotIn(
                mock.call('Unsupported content-type: %s', content_type),
                debug.mock_calls)

    @testing.gen_test
    def test_ignored_text_content_type(self):
        content_type = 'text/plain'
        with mock.patch.object(self.consumer.logger, 'debug') as debug:
            yield self.process_message(content_type, content_type)
            self.assertEqual(self.consumer.content_type, content_type)
            self.assertNotIn(
                mock.call('Unsupported content-type: %s', content_type),
                debug.mock_calls)

    @testing.gen_test
    def test_invalid_content_type(self):
        value = str(uuid.uuid4())
        yield self.process_message(value, '')
        self.assertEqual(self.consumer.body, value)

    @testing.gen_test
    def test_invalid_json(self):
        with self.assertRaises(consumer.MessageException):
            yield self.process_message('[{"foo", "baz":}', 'application/json')

    @testing.gen_test
    def test_invalid_json_encoding(self):
        with self.assertRaises(consumer.MessageException):
            yield self.process_message(b'\x81',
                                       'application/json; charset=utf-8')

    @testing.gen_test
    def test_valid_json_encoding(self):
        value = b'{"foo": "bar"}'
        yield self.process_message(value, 'application/json; charset=utf-8')
        self.assertEqual(self.consumer.body['foo'], 'bar')

    @testing.gen_test
    def test_invalid_msgpack(self):
        with self.assertRaises(consumer.MessageException):
            yield self.process_message('\x81\x99\x13foo\xc4\x03bar',
                                       'application/msgpack')

    @testing.gen_test
    def test_unsupported_application_content_type(self):
        content_type = 'application/{}'.format(str(uuid.uuid4()))
        with mock.patch.object(self.consumer.logger, 'debug') as debug:
            yield self.process_message(content_type, content_type)
            self.assertEqual(self.consumer.content_type, content_type)
            debug.assert_any_call('Unsupported content-type: %s', content_type)

    @testing.gen_test
    def test_unsupported_content_type(self):
        content_type = 'foo/{}'.format(str(uuid.uuid4()))
        with mock.patch.object(self.consumer.logger, 'debug') as debug:
            yield self.process_message(content_type, content_type)
            self.assertEqual(self.consumer.content_type, content_type)
            self.assertNotIn(
                mock.call('Unsupported content-type: %s', content_type),
                debug.mock_calls)

    @testing.gen_test
    def test_unsupported_text_content_type(self):
        content_type = 'text/{}'.format(str(uuid.uuid4()))
        with mock.patch.object(self.consumer.logger, 'debug') as debug:
            yield self.process_message(content_type, content_type)
            self.assertEqual(self.consumer.content_type, content_type)
            debug.assert_any_call('Unsupported content-type: %s', content_type)

    @testing.gen_test
    def test_xml(self):
        bar = str(uuid.uuid4())
        baz = str(uuid.uuid4())
        body = '<foo><bar>{}</bar><baz>{}</baz></foo>'.format(bar, baz)
        yield self.process_message(body, 'text/xml')
        self.assertIsInstance(self.consumer.body, bs4.BeautifulSoup)
        self.assertEqual(self.consumer.body.foo.bar.string, bar)
        self.assertEqual(self.consumer.body.foo.baz.string, baz)

    @testing.gen_test
    def test_yaml(self):
        payload = {'id': str(uuid.uuid4())}
        yield self.process_message(yaml.dump(payload), 'text/yaml')
        self.assertDictEqual(self.consumer.body, payload)

    @testing.gen_test
    def test_bs4_is_none(self):
        value = str(uuid.uuid4())
        yield self.process_message(value, '')
        self.assertEqual(self.consumer.body, value)

    @testing.gen_test
    def test_bzip2(self):
        payload = {'id': str(uuid.uuid4())}
        yield self.process_message(
            bz2.compress(json.dumps(payload).encode('utf-8')),
            'application/json',
            properties={
                'content_encoding': 'bzip2'
            })
        self.assertDictEqual(self.consumer.body, payload)

    @testing.gen_test
    def test_gzip(self):
        payload = {'id': str(uuid.uuid4())}
        yield self.process_message(
            zlib.compress(json.dumps(payload).encode('utf-8')),
            'application/json',
            properties={
                'content_encoding': 'gzip'
            })
        self.assertDictEqual(self.consumer.body, payload)

    @testing.gen_test
    def test_unsupported_encoding(self):
        encoding = str(uuid.uuid4())
        with mock.patch.object(self.consumer.logger, 'debug') as debug:
            yield self.process_message(
                encoding,
                'text/plain',
                properties={
                    'content_encoding': encoding
                })
            self.assertEqual(self.consumer.content_encoding, encoding)
            debug.assert_any_call('Unsupported content-encoding: %s', encoding)


class TestPublishingConsumer(consumer.SmartConsumer):

    PUBLISHER_CONFIRMATIONS = True

    @gen.coroutine
    def process(self):
        if self.content_type == 'text/csv':
            body = [row for row in self.body]
        elif self.headers.get('json'):
            body = json.loads(self.body)
        elif self.headers.get('decode'):
            body = self.body.decode('utf-8')
            self.logger.debug('Content: %r', body)
        else:
            body = self.body
        try:
            self.publish_message(self.exchange, self.routing_key,
                                 self.properties, body)
        except ValueError as error:
            raise consumer.ProcessingException(str(error))


class PublishingTestCase(testing.AsyncTestCase):
    def get_consumer(self):
        return TestPublishingConsumer

    @testing.gen_test
    def test_opaque_payload(self):
        body = bz2.compress(
            json.dumps({
                'id': str(uuid.uuid4())
            }).encode('utf-8'))
        yield self.process_message(body, '')
        self.assertEqual(self.published_messages[0].body, body)

    @testing.gen_test
    def test_invalid_encoding(self):
        body = bz2.compress(
            json.dumps({
                'id': str(uuid.uuid4())
            }).encode('utf-8'))
        yield self.process_message(
            body, '', properties={
                'content_encoding': 'utf-8'
            })
        self.assertEqual(self.published_messages[0].body, body)

    @testing.gen_test
    def test_bzip2_encode(self):
        body = bz2.compress(b'\x00\x01\x02\x03')
        yield self.process_message(
            body, 'application/octet-stream', properties={
                'content_encoding': 'bzip2'
            })

        self.assertEqual(self.published_messages[0].body, body)

    @testing.gen_test
    def test_bzip2_encode_str(self):
        value = str(uuid.uuid4())
        body = bz2.compress(value.encode('utf-8'))
        yield self.process_message(
            body,
            'text/plain',
            properties={
                'content_encoding': 'bzip2',
                'headers': {
                    'decode': True
                }
            })
        self.assertEqual(self.published_messages[0].body, body)

    @testing.gen_test
    def test_gzip_encode(self):
        body = zlib.compress(b'\x00\x01\x02\x03')
        yield self.process_message(
            body, 'application/octet-stream', properties={
                'content_encoding': 'gzip'
            })
        self.assertEqual(self.published_messages[0].body, body)

    @testing.gen_test
    def test_gzip_encode_str(self):
        value = str(uuid.uuid4())
        body = zlib.compress(value.encode('utf-8'))
        yield self.process_message(
            body,
            'text/plain',
            properties={
                'content_encoding': 'gzip',
                'headers': {
                    'decode': True
                }
            })
        self.assertEqual(self.published_messages[0].body, body)

    @testing.gen_test
    def test_json_serialize(self):
        body = json.dumps({'id': str(uuid.uuid4())})
        yield self.process_message(body, 'application/json')
        self.assertEqual(self.published_messages[0].body, body)

    @testing.gen_test
    def test_msgpack_serialize(self):
        body = umsgpack.packb({'id': str(uuid.uuid4())})
        yield self.process_message(body, 'application/msgpack')
        self.assertEqual(self.published_messages[0].body, body)

    @testing.gen_test
    def test_pickle_serialize(self):
        body = pickle.dumps({'id': str(uuid.uuid4())})
        yield self.process_message(body, 'application/x-pickle')
        self.assertEqual(self.published_messages[0].body, body)

    @testing.gen_test
    def test_plist_serialize(self):
        payload = {'id': str(uuid.uuid4())}
        if hasattr(plistlib, 'writePlistToString'):
            body = plistlib.writePlistToString(payload)
        elif hasattr(plistlib, 'dumps'):
            body = plistlib.dumps(payload)
        else:
            raise AssertionError('Missing plistlib method')
        yield self.process_message(body, 'application/x-plist')
        self.assertEqual(self.published_messages[0].body, body)

    @testing.gen_test
    def test_serialize_invalid_application_type(self):
        with self.assertRaises(consumer.ProcessingException):
            yield self.process_message(
                json.dumps({
                    'id': str(uuid.uuid4())
                }),
                'application/octet-stream',
                properties={
                    'headers': {
                        'json': True
                    }
                })

    @testing.gen_test
    def test_serialize_invalid_content_type(self):
        with self.assertRaises(consumer.ProcessingException):
            yield self.process_message(
                json.dumps({
                    'id': str(uuid.uuid4())
                }),
                'media/octet-stream',
                properties={
                    'headers': {
                        'json': True
                    }
                })

    @testing.gen_test
    def test_serialize_csv(self):
        body = 'aa,bb,cc\r\n3,1,2\r\n4,5,6\r\n'
        yield self.process_message(body, 'text/csv')
        self.assertEqual(self.published_messages[0].body, body)

    @testing.gen_test
    def test_serialize_yaml(self):
        body = yaml.dump({'id': str(uuid.uuid4())})
        yield self.process_message(body, 'text/yaml')
        self.assertEqual(self.published_messages[0].body, body)

    @testing.gen_test
    def test_serialize_invalid_text_type(self):
        with self.assertRaises(consumer.ProcessingException):
            yield self.process_message(
                json.dumps({
                    'id': str(uuid.uuid4())
                }),
                'text/foo',
                properties={
                    'headers': {
                        'json': True
                    }
                })

    @testing.gen_test
    def test_serialize_xml(self):
        body = ('<?xml version="1.0" encoding="utf-8"?>\n<foo>'
                '<bar>{}</bar><baz>{}</baz></foo>').format(
                    str(uuid.uuid4()), str(uuid.uuid4()))
        yield self.process_message(body, 'text/xml')
        self.assertEqual(self.published_messages[0].body, body)
