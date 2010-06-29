# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import amqplib.client_0_8 as amqp
import threading
import time
import unittest


class MessageReaderTest(unittest.TestCase):

    hostname = 'localhost'
    queue_name = 'test'

    def setUp(self):
        self.connection = amqp.Connection(host=self.hostname)
        self.channel = self.connection.channel()
        self.channel.queue_purge(self.queue_name)

    def tearDown(self):
        self.reader.stop()
        self.thread.join()

        self.channel.queue_purge(self.queue_name)
        self.channel.close()
        self.connection.close()

    def _create_reader(self):
        import gocept.amqprun.server
        self.reader = gocept.amqprun.server.MessageReader(self.hostname)
        self.thread = threading.Thread(target=self.reader.start)
        self.thread.start()

    def test_loop_can_be_stopped_from_outside(self):
        # this test simply should not hang indefinitely
        self._create_reader()
        time.sleep(0.1)

    def test_messages_arrive_in_task_queue(self):
        self._create_reader()

        self.send_message('foo')
        self.assertEqual(1, self.reader.tasks.qsize())
        message = self.reader.tasks.get()
        self.assertEqual('foo', message.body)

        self.send_message('bar')
        self.assertEqual(1, self.reader.tasks.qsize())
        message = self.reader.tasks.get()
        self.assertEqual('bar', message.body)

    def send_message(self, body):
        self.channel.basic_publish(amqp.Message(body), 'amq.fanout')
        time.sleep(0.1)
