# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import Queue
import time
import unittest


class WorkerTest(unittest.TestCase):

    def setUp(self):
        self.queue = Queue.Queue()

    def tearDown(self):
        if hasattr(self, 'worker'):
            self.worker.stop()

    def _create_worker(self, handler):
        import gocept.amqprun.worker
        self.worker = gocept.amqprun.worker.Worker(self.queue, handler)
        self.worker.start()

    def test_worker_can_be_stopped_from_outside(self):
        self._create_worker(lambda x: None)

    def test_worker_gets_messages_from_queue(self):
        self._create_worker(lambda x: None)

        self.queue.put('foo')
        time.sleep(0.1)
        self.assertEqual(0, self.queue.qsize())

        self.queue.put('bar')
        time.sleep(0.1)
        self.assertEqual(0, self.queue.qsize())

    def test_handle_message(self):
        messages = []
        self._create_worker(lambda x: messages.append(x))
        self.queue.put('foo')
        time.sleep(0.1)
        self.assertEqual(1, len(messages))
        self.assertEqual('foo', messages[0])
