import unittest
import zope.interface.verify
import time


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
