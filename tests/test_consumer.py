# coding=utf-8
"""Tests for rejected.consumer"""
try:
    import bs4
except ImportError:
    bs4 = None
import bz2
try:
    import couchconfig
except ImportError:
    couchconfig = None
import csv
import datetime
import mock
import pickle
try:
    from psycopg2 import extensions as psycopg2_extensions
    import psycopg2
except ImportError:
    psycopg2 = None
try:
    import redis
except ImportError:
    redis = None
try:
    import unittest2 as unittest
except ImportError:
    import unittest
import zlib

from rejected import consumer


class MockAllProperties(object):
    app_id = "go/1.2.7"
    content_type = "application/x-plist"
    content_encoding = None
    correlation_id = "6e43f307-b82e-4167-9285-345b9a06c424"
    delivery_mode = 1
    expiration = 1340211265
    headers = {"foo": "bar"}
    message_id = "3f777eda-7841-4d04-a7c0-21ab88f4a671"
    priority = 5
    reply_to = "4f757efa-7a41-4504-b7c0-21ab88f4a671"
    timestamp = 1340211260
    type = "click"
    user_id = 'gmr'

class MockJSONProperties(object):
    app_id = "go/1.2.7"
    content_type = "application/json"
    content_encoding = None
    correlation_id = "6e43f307-b82e-4167-9285-345b9a06c424"
    delivery_mode = 1
    expiration = None
    headers = None
    message_id = "3f777eda-7841-4d04-a7c0-21ab88f4a671"
    priority = None
    reply_to = None
    timestamp = 1340211260
    type = "click"
    user_id = None

class MockJSONMessage(object):

    body = '{"foo": "bar", "baz": 10}'
    body_expectation = {'foo': 'bar', 'baz': 10}
    routing_key = 'click'
    redelivered = False
    properties = MockJSONProperties
    def __init__(self):
        self.properties = MockJSONProperties()


class LocalConsumer(consumer.Consumer):

    _CONFIG_DOC = {'pgsql': {
        'host': 'localhost',
        'port': 6000,
        'user': 'www',
        'dbname': 'production'
    },
                   'redis': {
                       'host': 'localhost',
                       'port': 6879,
                       'db': 0
                   },
                   'whitelist': False}

    _DROP_INVALID_MESSAGES = True
    _MESSAGE_TYPE = None

    def _get_configuration_obj(self, config_dict):
        if not couchconfig:
            return None
        config = mock.Mock(couchconfig.Configuration, autospec=True)
        config.get_document = mock.Mock(return_value=self._CONFIG_DOC)
        return config

    def _get_postgresql_cursor(self, host, port, dbname, user,
                               password=None):
        if not psycopg2:
            return None
        return mock.Mock(psycopg2_extensions.cursor, autospec=True)

    def _get_redis_client(self, host, port, db):
        if not redis:
            return None
        kwargs = {'host': host, 'port': port, 'db': db}
        return mock.Mock(spec=redis.Redis, **kwargs)

class TestJSONConsumer(unittest.TestCase):

    _CONFIG = {'service': 'go.messaging.mtmedev.com',
               'config_host': 'config',
               'config_domain': 'mtmedev.com',
               'config_ttl': 300}

    _CSV = '"foo","bar"\r\n1,2\r\n3,4\r\n5,6\r\n'
    _CSV_EXPECTATION = [{"foo": "1", "bar": "2"},
            {"foo": "3", "bar": "4"},
            {"foo": "5", "bar": "6"}]

    _HTML = '<html><head><title>hi</title></head><body>there</body></html>'

    _PLIST = """\
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Name</key>
    <string>John Doe</string>
    <key>Phones</key>
    <array>
        <string>408-974-0000</string>
        <string>503-333-5555</string>
    </array>
</dict>
</plist>
"""
    _PLIST_EXPECTATION = {'Phones': ['408-974-0000', '503-333-5555'],
                          'Name': 'John Doe'}

    _YAML = """\
%YAML 1.2
---
Logging:
  format: "%(levelname) -10s %(asctime)s %(process)-6d %(processName) -15s %(name) -35s %(funcName) -30s: %(message)s"
  level: warning
  handler: syslog
  syslog:
    address: /dev/log
    facility: local6
"""
    _YAML_EXPECTATION = {'Logging': {'format':'%(levelname) -10s %(asctime)s '
                                              '%(process)-6d %(processName) '
                                              '-15s %(name) -35s %(funcName) '
                                              '-30s: %(message)s',
                                     'level': 'warning',
                                     'handler': 'syslog',
                                     'syslog': {'address': '/dev/log',
                                                'facility': 'local6'}}}

    def setUp(self):
        self._obj = LocalConsumer(self._CONFIG)
        self._obj._message = MockJSONMessage()

    def tearDown(self):
        del self._obj

    def test_initialize_called(self):
        @mock.patch.object(consumer.Consumer, '_initialize')
        def validate_method_called(mock_method=None):
            obj = consumer.Consumer({})
            mock_method.assertCalled()
        validate_method_called()

    def test_underscore_process_raises_exception(self):
        self.assertRaises(NotImplementedError, self._obj._process)

    def test_bz2_decompress(self):
        value = ('BZh91AY&SY\xe7\xdex,\x00\x00E\x9f\x80\x10\x07\x7f\xf0\x00'
                 '\xa0@\x00\xbe\xe7\xde\n \x00u\x11\x1a\x87\xa4\xf2@\x00\x03'
                 '\xd4i\xa0j\xa7\xfa)\xa4\xc6M4\xd0\x04`\x03\xab\xff\x01\x8f'
                 '\x07\x82\x17\xb9\xfe\x04\x8d\x14\x90m2WM\xb1,\x11\xb2d\x8d'
                 '\x80\xb6v\xb1%\xfb2!\x81"N$8\x13\xdc\xd63\xcc\xe0\xb0\xddV'
                 '\x06{\x12\xc4\x0c\xe2\x82\x1e\xc1\xa2o\x078\x07\xa9U\\A\xdc'
                 '\x97\x9e\xd3\x94W\xb9d\x83 \xf2\x18\x1a(\xf1\xde\x8b\xb9"'
                 '\x9c(Hs\xef<\x16\x00')
        expectation = ('{"birth_date":"1989-10-18","gender":"m","latitude":27'
                       '.945900,"longitude":-82.788400,"geolocation_source":"'
                       'GeoIP","ip_address":"97.78.13.106"}')
        self.assertEqual(self._obj._decode_bz2(value), expectation)

    def test_gzip_decompress(self):
        value = ('x\x9c-\xccA\x0e\xc2 \x10\x05\xd0\xbb\xccZ\x08\xd4*\x8c\x170'
                 '\xee\xbc\x01\xc12A\x92\xca\x18\xa0+\xe3\xdd\x9d&.\xff\xcb'
                 '\xff\xff\x03\x8f\xd2\xc63\xa48\x08.`\xd1\xa3\xb2FY\x0f\x07'
                 '\xc8T\x135\xd1\x97\x845\x8e2\xb6$\xa5\xc9i\x9cOh\x8c \xd7'
                 "\xfcW\xe5'\xed\xbc\x9fw\xce\xc4+/2\xe0\x1a:om\xd9\xaf\xaf"
                 '\xc4\xb7\xbb\x1c\x95w\x88)5\xea]\x10\x9d\x8c\xb4=jk\xce\xf0'
                 '\xfd\x01\x93))\x83')
        expectation = ('{"birth_date":"1989-10-18","gender":"m","latitude":27'
                       '.945900,"longitude":-82.788400,"geolocation_source":"'
                       'GeoIP","ip_address":"97.78.13.106"}')
        self.assertEqual(self._obj._decode_gzip(value), expectation)

    def test_get_configuration(self):
        if not couchconfig:
            return self.skipTest('Missing couchconfig package')
        @mock.patch('couchconfig.Configuration', autospec=True, **self._CONFIG)
        def test_object_creation(mock_method=None):
            obj = consumer.Consumer(self._CONFIG)
            self.assertEqual(obj._config._spec_class,
                             couchconfig.config.Configuration)
        test_object_creation()

    def test_get_service(self):
        value = 'foo.bar.baz'
        expectation = 'baz_bar_foo'
        self.assertEqual(self._obj._get_service(value), expectation)

    def test_get_postgresql_cursor_connect(self):
        if not psycopg2:
            return self.skipTest('Missing psycopg2 package')
        with mock.patch('psycopg2.connect') as mock_method:
            settings = self._obj._config.get_document('settings')
            self._obj._get_postgresql_cursor(**settings['pgsql'])
            mock_method.assertCalledWith(**settings['pgsql'])

    def test_get_postgresql_cursor_value(self):
        if not psycopg2:
            return self.skipTest('Missing psycopg2 package')
        @mock.patch('pgsql_wrapper.PgSQL', autospec=True)
        def test_object_creation(mock_class=None):
            obj = consumer.Consumer(self._CONFIG)
            settings = self._obj._config.get_document('settings')
            value = obj._get_postgresql_cursor(**settings['pgsql'])
            self.assertEqual(value._mock_name, 'cursor')
        test_object_creation()

    def test_get_postgresql_cursor_failure(self):
        if not psycopg2:
            return self.skipTest('Missing psycopg2 package')
        @mock.patch('pgsql_wrapper.PgSQL', autospec=True,
                    side_effect=psycopg2.OperationalError)
        def test_object_creation(mock_class=None):
            obj = consumer.Consumer(self._CONFIG)
            settings = self._obj._config.get_document('settings')
            self.assertRaises(IOError,
                              obj._get_postgresql_cursor,
                              **settings['pgsql'])
        test_object_creation()

    def test_get_redis(self):
        if not redis:
            return self.skipTest('Missing redis package')
        @mock.patch('redis.Redis', autospec=True)
        def test_object_creation(mock_method=None):
            obj = consumer.Consumer(self._CONFIG)
            settings = self._obj._config.get_document('settings')
            value = obj._get_redis_client(**settings['redis'])
            self.assertIsInstance(value, redis.client.Redis)
        test_object_creation()

    def test_load_csv_value_return_type(self):
        self.assertIsInstance(self._obj._load_csv_value(self._CSV),
                              csv.DictReader)

    def test_load_csv_value(self):
        result = list(self._obj._load_csv_value(self._CSV))
        self.assertListEqual(result, self._CSV_EXPECTATION)

    def test_load_html_value_return_type(self):
        if not bs4:
            return self.skipTest('Missing BeautifulSoup package')
        self.assertIsInstance(self._obj._load_html_value(self._HTML),
                              bs4.BeautifulSoup)

    def test_load_html_value(self):
        if not bs4:
            return self.skipTest('Missing BeautifulSoup package')
        expectation = 'hi'
        result = self._obj._load_html_value(self._HTML)
        self.assertEqual(result.title.string, expectation)

    def test_load_json_value_return_type(self):
        self.assertIsInstance(self._obj._load_json_value(MockJSONMessage.body),
                              dict)

    def test_load_json_value(self):
        self.assertEqual(self._obj._load_json_value(MockJSONMessage.body),
                         MockJSONMessage.body_expectation)

    def test_load_pickle_value_return_type(self):
        value = pickle.dumps(MockJSONMessage.body_expectation)
        self.assertIsInstance(self._obj._load_pickle_value(value),
                              dict)

    def test_load_pickle_value(self):
        value = pickle.dumps(MockJSONMessage.body_expectation)
        self.assertEqual(self._obj._load_pickle_value(value),
                         MockJSONMessage.body_expectation)

    def test_load_plist_value_return_type(self):
        self.assertIsInstance(self._obj._load_plist_value(self._PLIST),
                              dict)

    def test_load_plist_value(self):
        self.assertEqual(self._obj._load_plist_value(self._PLIST),
                         self._PLIST_EXPECTATION)


    def test_load_xml_value_return_type(self):
        if not bs4:
            return self.skipTest('Missing BeautifulSoup package')
        self.assertIsInstance(self._obj._load_xml_value(self._PLIST),
                              bs4.BeautifulSoup)

    def test_load_xml_value(self):
        if not bs4:
            return self.skipTest('Missing BeautifulSoup package')
        value = self._obj._load_xml_value(self._PLIST)
        self.assertEqual(value.find('key').string, 'Name')

    def test_message_app_id(self):
        self.assertEqual(self._obj.message_app_id, MockJSONProperties.app_id)

    def test_message_app_id_is_null(self):
        with mock.patch.object(LocalConsumer, '_process'):
            message = MockJSONMessage()
            message.properties = MockAllProperties()
            message.properties.app_id = None
            self._obj.process(message)
        self.assertIsNone(self._obj.message_app_id)

    def test_message_body(self):
        with mock.patch.object(LocalConsumer, '_process'):
            self._obj.process(MockJSONMessage())
        self.assertEqual(self._obj.message_body,
                         MockJSONMessage.body_expectation)

    def test_message_correlation_id(self):
        self.assertEqual(self._obj.message_correlation_id,
                         MockJSONProperties.correlation_id)

    def test_message_correlation_id_is_null(self):
        with mock.patch.object(LocalConsumer, '_process'):
            message = MockJSONMessage()
            message.properties = MockAllProperties()
            message.properties.correlation_id = None
            self._obj.process(message)
        self.assertIsNone(self._obj.message_correlation_id)

    def _datetime(self, value):
        return datetime.datetime.fromtimestamp(float(value))

    def test_message_expiration(self):
        expectation = self._datetime(MockAllProperties.expiration)
        with mock.patch.object(LocalConsumer, '_process'):
            message = MockJSONMessage()
            message.properties = MockAllProperties()
            self._obj.process(message)
        self.assertEqual(self._obj.message_expiration,
                         expectation)

    def test_message_expiration_is_null(self):
        with mock.patch.object(LocalConsumer, '_process'):
            message = MockJSONMessage()
            message.properties = MockAllProperties()
            message.properties.expiration = None
            self._obj.process(message)
        self.assertIsNone(self._obj.message_expiration)

    def test_message_headers(self):
        with mock.patch.object(LocalConsumer, '_process'):
            message = MockJSONMessage()
            message.properties = MockAllProperties()
            self._obj.process(message)
        self.assertEqual(self._obj.message_headers,
                         MockAllProperties.headers)

    def test_message_headers_is_null(self):
        self.assertIsNone(self._obj.message_headers)

    def test_message_id(self):
        self.assertEqual(self._obj.message_id, MockJSONProperties.message_id)

    def test_message_id_is_null(self):
        with mock.patch.object(LocalConsumer, '_process'):
            message = MockJSONMessage()
            message.properties = MockAllProperties()
            message.properties.message_id = None
            self._obj.process(message)
        self.assertIsNone(self._obj.message_id)

    def test_message_priority(self):
        with mock.patch.object(LocalConsumer, '_process'):
            message = MockJSONMessage()
            message.properties = MockAllProperties()
            self._obj.process(message)
        self.assertEqual(self._obj.message_priority,
                         MockAllProperties.priority)

    def test_message_priority_is_null(self):
        self.assertIsNone(self._obj.message_priority)

    def test_message_reply_to(self):
        with mock.patch.object(LocalConsumer, '_process'):
            message = MockJSONMessage()
            message.properties = MockAllProperties()
            self._obj.process(message)
        self.assertEqual(self._obj.message_reply_to,
                         MockAllProperties.reply_to)

    def test_message_reply_to_is_null(self):
        self.assertIsNone(self._obj.message_reply_to)

    def test_message_time(self):
        expectation = self._datetime(MockJSONProperties.timestamp)
        self.assertEqual(self._obj.message_time, expectation)

    def test_message_time_is_null(self):
        with mock.patch.object(LocalConsumer, '_process'):
            message = MockJSONMessage()
            message.properties = MockAllProperties()
            message.properties.timestamp = None
            self._obj.process(message)
        self.assertIsNone(self._obj.message_time)

    def test_message_type(self):
        self.assertEqual(self._obj.message_type,
                         MockJSONProperties.type)

    def test_message_type_is_null(self):
        with mock.patch.object(LocalConsumer, '_process'):
            message = MockJSONMessage()
            message.properties = MockAllProperties()
            message.properties.type = None
            self._obj.process(message)
        self.assertIsNone(self._obj.message_type)

    def test_message_user_id(self):
        with mock.patch.object(LocalConsumer, '_process'):
            message = MockJSONMessage()
            message.properties = MockAllProperties()
            self._obj.process(message)
        self.assertEqual(self._obj.message_user_id, MockAllProperties.user_id)

    def test_message_user_id_is_null(self):
        self.assertIsNone(self._obj.message_user_id)

    def test_process_with_message_type_validation_pass(self):
        self._obj._MESSAGE_TYPE = MockJSONProperties.type
        with mock.patch.object(LocalConsumer, '_process'):
            message = MockJSONMessage()
            self._obj.process(message)
            assert True, "ConsumerException was not raised"

    def test_process_with_message_type_validation_fail_drop(self):
        self._obj._MESSAGE_TYPE = 'BAD_TYPE_LOL'
        self._obj._DROP_INVALID_MESSAGES = True
        with mock.patch.object(LocalConsumer, '_process'):
            message = MockJSONMessage()
            self._obj.process(message)
            assert True, 'ConsumerException not raised'

    def test_process_with_message_type_validation_fail_dont_drop(self):
        self._obj._MESSAGE_TYPE = 'BAD_TYPE_LOL'
        self._obj._DROP_INVALID_MESSAGES = False
        with mock.patch.object(LocalConsumer, '_process'):
            message = MockJSONMessage()
            self.assertRaises(consumer.ConsumerException,
                              self._obj.process, message)

    def test_process_with_processing_exception(self):
        self._obj._MESSAGE_TYPE = None
        with mock.patch.object(LocalConsumer, '_process') as mock_method:
            mock_method.side_effect = consumer.ConsumerException
            message = MockJSONMessage()
            self.assertRaises(consumer.ConsumerException,
                              self._obj.process, message)

    def test_message_body_compressed_with_bzip2(self):
        with mock.patch.object(LocalConsumer, '_process'):
            message = MockJSONMessage()
            message.body = bz2.compress(self._PLIST)
            message.properties.content_type = 'application/x-plist'
            message.properties.content_encoding = 'bzip2'
            self._obj.process(message)
            self.assertEqual(self._obj.message_body,
                             self._PLIST_EXPECTATION)

    def test_message_body_compressed_with_gzip(self):
        with mock.patch.object(LocalConsumer, '_process'):
            message = MockJSONMessage()
            message.body = zlib.compress(self._PLIST)
            message.properties.content_type = 'application/x-plist'
            message.properties.content_encoding = 'gzip'
            self._obj.process(message)
            self.assertEqual(self._obj.message_body,
                             self._PLIST_EXPECTATION)

    def test_message_body_with_utf8_encoding(self):
        with mock.patch.object(LocalConsumer, '_process'):
            message = MockJSONMessage()
            message.properties.content_encoding = 'utf-8'
            self._obj.process(message)
            body = self._obj.message_body  # Get this to change encoding
            self.assertIsNone(self._obj.message_content_encoding)

    def test_message_body_as_json(self):
        with mock.patch.object(LocalConsumer, '_process'):
            self._obj.process(MockJSONMessage())
            self.assertEqual(self._obj.message_body,
                             MockJSONMessage.body_expectation)

    def test_message_body_as_pickle(self):
        with mock.patch.object(LocalConsumer, '_process'):
            message = MockJSONMessage()
            message.body = pickle.dumps(self._PLIST_EXPECTATION)
            message.properties.content_type = 'application/pickle'
            self._obj.process(message)
            self.assertEqual(self._obj.message_body,
                             self._PLIST_EXPECTATION)

    def test_message_body_as_csv(self):
        with mock.patch.object(LocalConsumer, '_process'):
            message = MockJSONMessage()
            message.body = self._CSV
            message.properties.content_type = 'text/csv'
            self._obj.process(message)
            self.assertEqual(list(self._obj.message_body),
                             self._CSV_EXPECTATION)

    def test_message_body_as_html(self):
        if not bs4:
            return self.skipTest('Missing BeautifulSoup package')
        with mock.patch.object(LocalConsumer, '_process'):
            message = MockJSONMessage()
            message.body = self._HTML
            message.properties.content_type = 'text/html'
            self._obj.process(message)
            self.assertEqual(self._obj.message_body.title.string, 'hi')

    def test_message_body_as_plist(self):
        with mock.patch.object(LocalConsumer, '_process'):
            message = MockJSONMessage()
            message.body = self._PLIST
            message.properties.content_type = 'application/x-plist'
            self._obj.process(message)
            self.assertEqual(self._obj.message_body,
                             self._PLIST_EXPECTATION)

    def test_message_body_as_xml(self):
        if not bs4:
            return self.skipTest('Missing BeautifulSoup package')
        with mock.patch.object(LocalConsumer, '_process'):
            message = MockJSONMessage()
            message.body = self._PLIST
            message.properties.content_type = 'text/xml'
            self._obj.process(message)
            self.assertEqual(self._obj.message_body.find('key').string, 'Name')


    def test_message_body_as_yaml(self):
        with mock.patch.object(LocalConsumer, '_process'):
            message = MockJSONMessage()
            message.body = self._YAML
            message.properties.content_type = 'text/yaml'
            self._obj.process(message)
            self.assertEqual(self._obj.message_body, self._YAML_EXPECTATION)

    def test_message_body_no_decoding_deserializing(self):
        with mock.patch.object(LocalConsumer, '_process'):
            message = MockJSONMessage()
            message.body = self._PLIST
            message.properties.content_type = 'text/text'
            self._obj.process(message)
            self.assertEqual(self._obj.message_body, self._PLIST)

    def test_message_body_no_content_type(self):
        with mock.patch.object(LocalConsumer, '_process'):
            message = MockJSONMessage()
            message.body = self._PLIST
            message.properties.content_type = None
            self._obj.process(message)
            self.assertEqual(self._obj.message_body, self._PLIST)

    def test_message_body_called_twice(self):
        with mock.patch.object(LocalConsumer, '_process'):
            message = MockJSONMessage()
            message.body = self._PLIST
            message.properties.content_type = None
            self._obj.process(message)
            unused_body_for_first_call = self._obj.message_body
            self.assertEqual(self._obj.message_body, self._PLIST)
