# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import amqplib.client_0_8 as amqp
import threading
import time
import unittest


class MessageReaderTest(unittest.TestCase):

    queue_name = 'test'

    def setUp(self):
        self.connection = amqp.Connection(host='localhost')
        self.channel = self.connection.channel()
        self.channel.queue_purge(self.queue_name)

    def tearDown(self):
        self.channel.queue_purge(self.queue_name)
        self.channel.close()
        self.connection.close()

    def test_loop_can_be_stopped_from_outside(self):
        import gocept.amqprun.server
        reader = gocept.amqprun.server.MessageReader()
        t = threading.Thread(target=reader.start)
        t.start()
        time.sleep(0.1)
        reader.stop()
        t.join()

    def test_messages_arrive_in_task_queue(self):
        import gocept.amqprun.server
        reader = gocept.amqprun.server.MessageReader()
        t = threading.Thread(target=reader.start)
        t.start()

        self.send_message('foo')
        self.assertEqual(1, reader.tasks.qsize())
        message = reader.tasks.get()
        self.assertEqual('foo', message.body)

        self.send_message('bar')
        self.assertEqual(1, reader.tasks.qsize())
        message = reader.tasks.get()
        self.assertEqual('bar', message.body)

        reader.stop()
        t.join()

    def send_message(self, body):
        self.channel.basic_publish(amqp.Message(body), 'amq.fanout')
        time.sleep(0.1)
