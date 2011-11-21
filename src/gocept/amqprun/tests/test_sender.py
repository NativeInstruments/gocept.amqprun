# Copyright (c) 2011 gocept gmbh & co. kg
# See also LICENSE.txt

import gocept.amqprun.testing
import transaction


class MessageSenderTest(
        gocept.amqprun.testing.LoopTestCase,
        gocept.amqprun.testing.QueueTestCase):

    def setUp(self):
        super(MessageSenderTest, self).setUp()
        transaction.abort()
        self.expect_response_on('test.key')
        self.sender = self.create_server()
        self.start_thread(self.sender)

    def send(self, body='message 1'):
        from gocept.amqprun.message import Message
        self.sender.send(Message({}, body, routing_key='test.key'))

    def test_message_is_sent_on_commit(self):
        self.send()
        transaction.commit()
        response = self.wait_for_response(5)
        self.assertEqual('message 1', response.body)

    def test_message_is_not_sent_before_commit(self):
        self.send()
        with self.assertRaises(AssertionError) as err:
            self.wait_for_response(2)
        self.assertEqual('No message received', str(err.exception))

    def test_message_is_not_sent_on_abort(self):
        self.send()
        transaction.abort()
        with self.assertRaises(AssertionError) as err:
            self.wait_for_response(2)
        self.assertEqual('No message received', str(err.exception))

    def test_message_can_be_sent_after_abort(self):
        self.send('message 1')
        transaction.abort()
        self.send('message 2')
        transaction.commit()
        response = self.wait_for_response(5)
        self.assertEqual('message 2', response.body)

    def test_message_can_be_sent_after_commit(self):
        self.send('message 1')
        transaction.commit()
        response = self.wait_for_response(5)
        self.assertEqual('message 1', response.body)
        self.send('message 2')
        transaction.commit()
        response = self.wait_for_response(5)
        self.assertEqual('message 2', response.body)
