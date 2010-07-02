# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import Queue
import mock
import time
import transaction.interfaces
import unittest


class WorkerTest(unittest.TestCase):

    def setUp(self):
        self.queue = Queue.Queue()
        self.datamanager = mock.Mock(spec=list(
            transaction.interfaces.IDataManager))
        self.dm_factory = mock.Mock(return_value=self.datamanager)

    def tearDown(self):
        if hasattr(self, 'worker'):
            self.worker.stop()

    def _create_worker(self):
        import gocept.amqprun.worker
        self.worker = gocept.amqprun.worker.Worker(
            self.queue, self.dm_factory)
        self.worker.start()

    def test_worker_can_be_stopped_from_outside(self):
        # this test simply should not hang indefinitely
        self._create_worker()

    def test_worker_gets_messages_from_queue(self):
        self._create_worker()

        self.queue.put('foo')
        time.sleep(0.1)
        self.assertEqual(0, self.queue.qsize())

        self.queue.put('bar')
        time.sleep(0.1)
        self.assertEqual(0, self.queue.qsize())

    def test_handle_message(self):
        import gocept.amqprun.handler
        messages = []
        self._create_worker()

        handler = gocept.amqprun.handler.FactoredHandler(
            lambda x: messages.append(x), mock.sentinel.message)

        self.queue.put(handler)
        time.sleep(0.1)
        self.assertEqual(1, len(messages))
        self.assertEqual(mock.sentinel.message, messages[0])

    def test_transaction_should_commit(self):
        import gocept.amqprun.handler
        self._create_worker()
        handler = gocept.amqprun.handler.FactoredHandler(
            lambda x: None, mock.sentinel.message)
        self.queue.put(handler)
        time.sleep(0.1)
        self.assertEqual(0, self.queue.qsize())
        self.assertTrue(self.datamanager.tpc_begin.called)
        self.assertTrue(self.datamanager.commit.called)
        self.assertTrue(self.datamanager.tpc_vote.called)
        self.assertTrue(self.datamanager.tpc_finish.called)

    def test_on_exception_transaction_should_abort(self):
        import gocept.amqprun.handler
        def provoke_error():
            raise RuntimeError('provoked error')

        self._create_worker()

        handler = gocept.amqprun.handler.FactoredHandler(
            provoke_error, mock.sentinel.message)

        self.queue.put('foo')
        time.sleep(0.1)
        self.assertEqual(0, self.queue.qsize())
        self.assertFalse(self.datamanager.tpc_begin.called)
        self.assertFalse(self.datamanager.commit.called)
        self.assertTrue(self.datamanager.abort.called)
        self.assertFalse(self.datamanager.tpc_abort.called)
