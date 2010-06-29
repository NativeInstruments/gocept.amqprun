# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import Queue
import mock
import time
import unittest


class WorkerTest(unittest.TestCase):

    def setUp(self):
        self.queue = Queue.Queue()
        self.datamanager = mock.Mock()
        self.dm_factory = mock.Mock(return_value=self.datamanager)

    def tearDown(self):
        if hasattr(self, 'worker'):
            self.worker.stop()

    def _create_worker(self, handler):
        import gocept.amqprun.worker
        self.worker = gocept.amqprun.worker.Worker(
            self.queue, handler, self.dm_factory)
        self.worker.start()

    def test_worker_can_be_stopped_from_outside(self):
        # this test simply should not hang indefinitely
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

    def test_transaction_should_commit(self):
        self._create_worker(lambda x: None)

        self.queue.put('foo')
        time.sleep(0.1)
        self.assertEqual(0, self.queue.qsize())
        self.assertTrue(self.datamanager.tpc_begin.called)
        self.assertTrue(self.datamanager.commit.called)

    def test_on_exception_transaction_should_abort(self):
        def provoke_error():
            raise RuntimeError('provoked error')

        self._create_worker(provoke_error)

        self.queue.put('foo')
        time.sleep(0.1)
        self.assertEqual(0, self.queue.qsize())
        self.assertTrue(self.datamanager.begin.called)
        self.assertFalse(self.datamanager.commit.called)
        self.assertTrue(self.datamanager.abort.called)
