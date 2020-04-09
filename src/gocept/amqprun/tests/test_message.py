# -*- coding: utf-8 -*-
from datetime import datetime
from gocept.amqprun.message import Properties, Message
import gocept.testing.assertion
import pytest
import six
import time
import unittest
import zope.interface.verify


class TestMessage(unittest.TestCase):

    def test_message_should_provide_IMessage(self):
        message = Message({}, b'body')
        zope.interface.verify.verifyObject(
            gocept.amqprun.interfaces.IMessage,
            message)
        self.assertEqual(b'body', message.body)
        self.assertEqual('amq.topic', message.exchange)

    def test_message_can_be_unicode_if_encoding_is_set(self):
        message = Message({'content_encoding': 'utf-8'}, u'bödÿ')
        self.assertEqual(u'bödÿ', message.body)

    def test_header_should_be_converted_to_Properties(self):
        now = int(time.time())
        message = Message(
            dict(foo='bar', content_type='text/xml', app_id='myapp'), None,
            delivery_tag=1)
        header = message.header
        self.assertTrue(isinstance(header, Properties))
        self.assertEqual(dict(foo='bar'), header.application_headers)
        self.assertTrue(header.timestamp >= now)
        self.assertEqual('text/xml', header.content_type)
        self.assertEqual(2, header.delivery_mode)  # persistent
        self.assertEqual('myapp', header.app_id)

    def test_converted_header_should_contain_message_id(self):
        message = Message({}, b'')
        self.assertRegexpMatches(
            message.header.message_id,
            r'<(\d+\.)+gocept\.amqprun@.*>$')

    def test_given_message_id_should_not_be_overwritten(self):
        message = Message(dict(message_id='myid'), b'')
        self.assertEqual('myid', message.header.message_id)

    def test_message_with_unicode_body_needs_content_encoding(self):
        with pytest.raises(ValueError):
            Message({}, u'Test')
        message = Message({'content_encoding': 'UTF-8'}, u'Test')

        assert message

    @pytest.mark.skipif(
        six.PY3, reason='There is no special handling on Python 3')
    def test_message_with_empty_unicode_body_gets_empty_string_body(self):
        message = Message({}, u'')

        assert isinstance(message.body, str)

    def test_message_additional_application_headers_get_merged(self):
        headers = {
            'test_header_1': 'test',
            'application_headers': {
                'test_header_2': 'test'
            }}

        message = Message(headers, b'')

        assert message.header.application_headers == {
            'test_header_1': 'test',
            'test_header_2': 'test'
        }

        assert 'test_header_1' not in message.header.as_dict()


class GenerateFilename(unittest.TestCase,
                       gocept.testing.assertion.Exceptions):

    def create_message(self, body=b'testbody'):
        message = Message({}, body, routing_key='routing')
        message.header.message_id = 'myid'
        message.header.timestamp = time.mktime(
            datetime(2011, 7, 14, 14, 15).timetuple())
        return message

    def test_filename_uses_more_than_millisecond_precision(self):
        filename = self.create_message().generate_filename('${unique}')
        digits = filename.split('.')[-1]
        self.assertGreater(len(digits), 2)

    def test_no_timestamp_uses_now_as_date_placeholder(self):
        self.assertEqual(
            datetime(2011, 7, 14, 14, 15).strftime('%Y-%m-%d'),
            self.create_message().generate_filename('${date}'))

    def test_filename_substitutes_pattern(self):
        filename = self.create_message().generate_filename(
            '${routing_key}_${date}_${msgid}_${unique}')
        parts = filename.split('_')
        self.assertEqual(['routing', '2011-07-14', 'myid'], parts[:-1])
        self.assertNothingRaised(lambda: float(parts[3]))
