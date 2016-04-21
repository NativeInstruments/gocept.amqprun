import Queue
import gocept.amqprun.handler
import gocept.amqprun.interfaces
import mock
import time
import transaction
import unittest
import zope.interface


class WorkerTest(unittest.TestCase):

    def setUp(self):
        import gocept.amqprun.worker
        self.queue = Queue.Queue()
        self.session = mock.Mock()
        self._timeout = gocept.amqprun.worker.Worker.timeout
        gocept.amqprun.worker.Worker.timeout = 0.05

    def tearDown(self):
        import gocept.amqprun.worker
        if hasattr(self, 'worker'):
            self.worker.stop()
        gocept.amqprun.worker.Worker.timeout = self._timeout

    def _create_worker(self):
        import gocept.amqprun.worker
        self.worker = gocept.amqprun.worker.Worker(self.queue)
        self.worker.start()
        # wait for thread to start
        for i in range(10):
            time.sleep(0.025)
            if self.worker.running:
                break

    def _create_task(self, handler):
        ANY = None
        return (
            self.session, gocept.amqprun.handler.Handler(ANY, ANY, handler))

    def test_worker_can_be_stopped_from_outside(self):
        # this test simply should not hang indefinitely
        self._create_worker()

    def test_worker_should_be_daemon_thread(self):
        self._create_worker()
        self.assertTrue(self.worker.daemon)

    def test_worker_gets_messages_from_queue(self):
        self._create_worker()

        self.queue.put(self._create_task(lambda msg: True))
        time.sleep(0.1)
        self.assertEqual(0, self.queue.qsize())

        self.queue.put(self._create_task(lambda msg: True))
        time.sleep(0.1)
        self.assertEqual(0, self.queue.qsize())

    def test_handle_message(self):
        messages = []
        self._create_worker()
        session, handler = self._create_task(
            lambda msg: messages.append(msg))

        self.queue.put((session, handler))
        time.sleep(0.1)
        self.assertEqual(1, len(messages))
        self.assertEqual(session.received_message, messages[0])

    def test_messages_returned_by_handler_should_be_sent(self):
        self._create_worker()

        message1 = mock.Mock()
        message2 = mock.Mock()

        self.queue.put(self._create_task(lambda msg: [message1, message2]))
        time.sleep(0.1)
        self.assertEqual(2, self.session.send.call_count)
        self.session.send.assert_called_with(message2)

    @mock.patch('transaction.commit')
    def test_transaction_should_commit(self, transaction_commit):
        self._create_worker()
        self.queue.put(self._create_task(lambda x: None))
        time.sleep(0.1)
        self.assertEqual(0, self.queue.qsize())
        self.assertTrue(transaction_commit.called)
        assert self.session.channel.release.called

    @mock.patch('transaction.commit')
    @mock.patch('transaction.abort')
    def test_on_exception_transaction_should_abort(self, mock1, mock2):
        self._create_worker()
        provoke_error = mock.Mock(side_effect=RuntimeError('provoked error'))
        self.queue.put(self._create_task(provoke_error))
        time.sleep(0.1)
        self.assertEqual(0, self.queue.qsize())
        self.assertTrue(provoke_error.called)
        self.assertFalse(transaction.commit.called)
        self.assertTrue(transaction.abort.called)
        assert self.session.channel.release.called

    @mock.patch('transaction.commit')
    @mock.patch('transaction.abort')
    def test_on_exception_with_response_transaction_should_abort_then_commit(
            self, abort, commit):
        calls = []
        abort.side_effect = lambda: calls.append('abort')
        self.commit_called = False

        def commit_effect():
            if not self.commit_called:
                self.commit_called = True
                raise RuntimeError('provoked error')
            calls.append('commit')

        commit.side_effect = commit_effect

        self._create_worker()
        response = mock.Mock()
        response.responses = []
        response.exception.return_value = []
        zope.interface.alsoProvides(
            response, gocept.amqprun.interfaces.IResponse)
        self.queue.put(self._create_task(lambda x: response))
        time.sleep(0.1)
        self.assertEqual(0, self.queue.qsize())
        self.assertTrue(response.exception.called)
        self.assertEqual(['abort', 'commit'], calls)
        assert self.session.channel.release.called

    @mock.patch('transaction.commit')
    @mock.patch('transaction.abort')
    def test_error_on_commit_should_abort_transaction(self, mock1, mock2):
        self._create_worker()
        transaction.commit.side_effect = RuntimeError('commit error')
        self.queue.put(self._create_task(lambda x: None))
        time.sleep(0.1)
        self.assertEqual(0, self.queue.qsize())
        self.assertTrue(transaction.commit.called)
        self.assertTrue(transaction.abort.called)
        assert self.session.channel.release.called

    @mock.patch('transaction.abort')
    def test_error_on_abort_should_not_crash_thread(self, mock1):
        self._create_worker()
        provoke_error = mock.Mock(side_effect=RuntimeError('provoked error'))
        transaction.abort.side_effect = RuntimeError('abort error')
        self.queue.put(self._create_task(provoke_error))
        time.sleep(0.1)
        self.assertEqual(0, self.queue.qsize())
        self.assertTrue(transaction.abort.called)
        self.assertTrue(self.worker.is_alive())
        assert self.session.channel.release.called

    def test_handler_with_principal_should_create_interaction(self):
        import zope.security.management

        def store_principal(message):
            interaction = zope.security.management.getInteraction()
            self.principal = interaction.participations[0].principal.id

        self._create_worker()
        task = self._create_task(store_principal)
        task[1].principal = 'userid'
        self.queue.put(task)
        time.sleep(0.1)
        self.assertEqual(0, self.queue.qsize())
        self.assertEqual('userid', self.principal)
        self.assertFalse(zope.security.management.queryInteraction())


class WorkerITests(gocept.amqprun.testing.MainTestCase):
    """Integration testing ..worker.Worker."""

    def setUp(self):
        super(WorkerITests, self).setUp()
        self.make_config(__name__, 'integration')
        self.start_server_in_subprocess()

    def test_worker__Worker__run__1(self):
        """It kills the server on a `CounterBelowZero` exception.

        This exception is provoked by the handler for this routing key by
        decrementing the channel reference counter.

        """
        self.send_message('peng!', routing_key='test.counterbelowzero')
        self.wait_for_subprocess_exit()
