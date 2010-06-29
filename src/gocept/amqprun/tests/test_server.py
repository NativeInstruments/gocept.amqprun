# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import amqplib.client_0_8 as amqp
import threading
import time
import unittest


class ServerTest(unittest.TestCase):

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
        loop = gocept.amqprun.server.Mainloop()
        t = threading.Thread(target=loop.start)
        t.start()
        time.sleep(0.1)
        loop.stop()
        t.join()

    def test_messages_arrive_in_task_queue(self):
        import gocept.amqprun.server
        loop = gocept.amqprun.server.Mainloop()
        t = threading.Thread(target=loop.start)
        t.start()
        self.send_message('foo')
        time.sleep(0.1)

        self.assertEqual(1, loop.tasks.qsize())
        message = loop.tasks.get()
        self.assertEqual('foo', message.body)

        loop.stop()
        t.join()

    def send_message(self, body):
        self.channel.basic_publish(amqp.Message(body), 'amq.fanout')
