# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import Queue
import mock
import time
import transaction
import unittest


class WorkerTest(unittest.TestCase):

    def setUp(self):
        import gocept.amqprun.worker
        self.queue = Queue.Queue()
        self.session_factory = mock.Mock()
        self._timeout = gocept.amqprun.worker.Worker.timeout
        gocept.amqprun.worker.Worker.timeout = 0.05

    def tearDown(self):
        import gocept.amqprun.worker
        if hasattr(self, 'worker'):
            self.worker.stop()
        gocept.amqprun.worker.Worker.timeout = self._timeout

    def _create_worker(self):
        import gocept.amqprun.worker
        self.worker = gocept.amqprun.worker.Worker(
            self.queue, self.session_factory)
        self.worker.start()
        # wait for thread to start
        for i in range(10):
            time.sleep(0.025)
            if self.worker.running:
                break

    def test_worker_can_be_stopped_from_outside(self):
        # this test simply should not hang indefinitely
        self._create_worker()

    def test_worker_should_be_daemon_thread(self):
        self._create_worker()
        self.assertTrue(self.worker.daemon)

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
        self.assertFalse(self.session_factory.called)

        message = mock.Mock()
        handler = gocept.amqprun.handler.FactoredHandler(
            lambda msg: messages.append(msg), message)

        self.queue.put(handler)
        time.sleep(0.1)
        self.assertEqual(1, len(messages))
        self.assertEqual(message, messages[0])
        self.assertTrue(self.session_factory.called)

    def test_messages_returned_by_handler_should_be_sent(self):
        import gocept.amqprun.handler
        self._create_worker()
        self.assertFalse(self.session_factory.called)

        message1 = mock.Mock()
        message2 = mock.Mock()

        def func(msg):
            return [message1, message2]

        handler = gocept.amqprun.handler.FactoredHandler(
            func, mock.Mock())

        self.queue.put(handler)
        time.sleep(0.1)
        session = self.session_factory()
        self.assertEqual(2, session.send.call_count)
        session.send.assert_called_with(message2)

    @mock.patch('transaction.commit')
    def test_transaction_should_commit(self, transaction_commit):
        import gocept.amqprun.handler
        self._create_worker()
        handler = gocept.amqprun.handler.FactoredHandler(
            lambda x: None, mock.Mock())
        self.queue.put(handler)
        time.sleep(0.1)
        self.assertEqual(0, self.queue.qsize())
        self.assertTrue(transaction_commit.called)

    @mock.patch('transaction.commit')
    @mock.patch('transaction.abort')
    def test_on_exception_transaction_should_abort(self, mock1, mock2):
        import gocept.amqprun.handler
        self._create_worker()
        provoke_error = mock.Mock(side_effect=RuntimeError('provoked error'))
        handler = gocept.amqprun.handler.FactoredHandler(
            provoke_error, mock.Mock())
        self.queue.put(handler)
        time.sleep(0.1)
        self.assertEqual(0, self.queue.qsize())
        self.assertTrue(provoke_error.called)
        self.assertFalse(transaction.commit.called)
        self.assertTrue(transaction.abort.called)

    @mock.patch('transaction.commit')
    @mock.patch('transaction.abort')
    def test_error_on_commit_should_abort_transaction(self, mock1, mock2):
        import gocept.amqprun.handler
        self._create_worker()
        handler = gocept.amqprun.handler.FactoredHandler(
            lambda x: None, mock.Mock())
        transaction.commit.side_effect = RuntimeError('commit error')
        self.queue.put(handler)
        time.sleep(0.1)
        self.assertEqual(0, self.queue.qsize())
        self.assertTrue(transaction.commit.called)
        self.assertTrue(transaction.abort.called)

    @mock.patch('transaction.abort')
    def test_error_on_abort_should_not_crash_thread(self, mock1):
        import gocept.amqprun.handler
        self._create_worker()
        provoke_error = mock.Mock(side_effect=RuntimeError('provoked error'))
        handler = gocept.amqprun.handler.FactoredHandler(
            provoke_error, mock.Mock())
        transaction.abort.side_effect = RuntimeError('abort error')
        self.queue.put(handler)
        time.sleep(0.1)
        self.assertEqual(0, self.queue.qsize())
        self.assertTrue(transaction.abort.called)
        self.assertTrue(self.worker.is_alive())
