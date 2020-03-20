import gocept.amqprun.handler
import gocept.amqprun.interfaces
import gocept.amqprun.worker
import mock
import unittest
import zope.interface


class WorkerTest(unittest.TestCase):

    def setUp(self):
        self.session = mock.Mock()

    def _create_worker(self, handler_function=lambda msg: [], principal=None):
        handler = gocept.amqprun.handler.Handler(
            'test.queue', 'test.route', handler_function, principal=principal)
        self.worker = gocept.amqprun.worker.Worker(self.session, handler)

    def test_worker_can_be_stopped_from_outside(self):
        # this test simply should not hang indefinitely
        self._create_worker()

    def test_handle_message(self):
        messages = []
        self._create_worker(lambda msg: messages.append(msg))
        self.worker()
        self.assertEqual(1, len(messages))
        self.assertEqual(self.session.received_message, messages[0])

    def test_messages_returned_by_handler_should_be_sent(self):
        message1 = mock.Mock()
        message2 = mock.Mock()
        self._create_worker(lambda msg: [message1, message2])
        self.worker()

        self.assertEqual(2, self.session.send.call_count)
        self.session.send.assert_called_with(message2)

    def test_transaction_should_commit(self):
        self._create_worker(lambda x: None)
        with mock.patch('transaction.commit') as commit:
            self.worker()

        self.assertTrue(commit.called)

    def test_on_exception_transaction_should_abort(self):
        provoke_error = mock.Mock(side_effect=RuntimeError('provoked error'))
        self._create_worker(provoke_error)
        with mock.patch('transaction.commit') as commit, \
                mock.patch('transaction.abort') as abort:
            self.worker()
        self.assertTrue(provoke_error.called)
        self.assertFalse(commit.called)
        self.assertTrue(abort.called)

    def test_on_exception_with_response_transaction_should_abort_then_commit(
            self):
        calls = []
        self.commit_called = False

        def commit_effect():
            if not self.commit_called:
                self.commit_called = True
                raise RuntimeError('provoked error')
            calls.append('commit')

        response = mock.Mock()
        response.responses = []
        response.exception.return_value = []
        zope.interface.alsoProvides(
            response, gocept.amqprun.interfaces.IResponse)
        self._create_worker(lambda x: response)

        with mock.patch('transaction.commit') as commit, \
                mock.patch('transaction.abort') as abort:
            abort.side_effect = lambda: calls.append('abort')
            commit.side_effect = commit_effect
            self.worker()

        self.assertTrue(response.exception.called)
        self.assertEqual(['abort', 'commit'], calls)

    def test_error_on_commit_should_abort_transaction(self):
        self._create_worker()
        with mock.patch(
                'transaction.commit',
                side_effect=RuntimeError('commit error')) as commit, \
                mock.patch('transaction.abort') as abort:
            self.worker()
        self.assertTrue(commit.called)
        self.assertTrue(abort.called)

    def test_error_on_abort_should_not_crash_thread(self):
        provoke_error = mock.Mock(side_effect=RuntimeError('provoked error'))
        self._create_worker(handler_function=provoke_error)
        with mock.patch('transaction.abort',
                        side_effect=RuntimeError('abort error')) as abort:
            self.worker()

        self.assertTrue(abort.called)

    def test_handler_with_principal_should_create_interaction(self):
        import zope.security.management

        def store_principal(message):
            interaction = zope.security.management.getInteraction()
            self.principal = interaction.participations[0].principal.id

        self._create_worker(store_principal, principal='userid')
        self.worker()
        self.assertEqual('userid', self.principal)
        self.assertFalse(zope.security.management.queryInteraction())
