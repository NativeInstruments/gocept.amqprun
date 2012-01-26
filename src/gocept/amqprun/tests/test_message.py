# Copyright (c) 2012 gocept gmbh & co. kg
# See also LICENSE.txt

from datetime import datetime
import gocept.testing.assertion
import time
import unittest
import zope.interface.verify


class TestMessage(unittest.TestCase):

    def test_message_should_provide_IMessage(self):
        from gocept.amqprun.message import Message
        import gocept.amqprun.interfaces
        message = Message({}, 2, 3)
        zope.interface.verify.verifyObject(
            gocept.amqprun.interfaces.IMessage,
            message)
        self.assertEqual(2, message.body)
        self.assertEqual('amq.topic', message.exchange)

    def test_header_should_be_converted_to_BasicProperties(self):
        from gocept.amqprun.message import Message
        import pika.spec
        now = int(time.time())
        message = Message(
            dict(foo='bar', content_type='text/xml', app_id='myapp'), None,
            delivery_tag=1)
        header = message.header
        self.assertTrue(isinstance(header, pika.spec.BasicProperties))
        self.assertEqual(dict(foo='bar'), header.headers)
        self.assertTrue(header.timestamp >= now)
        self.assertEqual('text/xml', header.content_type)
        self.assertEqual(2, header.delivery_mode) # persistent
        self.assertEqual('myapp', header.app_id)

    def test_converted_header_should_contain_message_id(self):
        from gocept.amqprun.message import Message
        message = Message({}, '')
        self.assertRegexpMatches(
            message.header.message_id,
            r'<(\d+\.)+gocept\.amqprun@.*>$')

    def test_given_message_id_should_not_be_overwritten(self):
        from gocept.amqprun.message import Message
        message = Message(dict(message_id='myid'), '')
        self.assertEqual('myid', message.header.message_id)


class GenerateFilename(unittest.TestCase,
                       gocept.testing.assertion.Exceptions):

    def create_message(self, body='testbody'):
        from gocept.amqprun.message import Message
        message = Message({}, body, routing_key='routing')
        message.header.message_id = 'myid'
        message.header.timestamp = time.mktime(
            datetime(2011, 7, 14, 14, 15).timetuple())
        return message

    def test_filename_uses_more_than_millisecond_precision(self):
        filename = self.create_message().generate_filename('${unique}')
        digits = filename.split('.')[-1]
        self.assertGreater(digits, 2)

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
