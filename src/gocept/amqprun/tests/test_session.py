# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import mock
import threading
import time
import unittest
import zope.component
import zope.component.testing
import zope.interface.verify


class DataManagerTest(unittest.TestCase):

    def setUp(self):
        import gocept.amqprun.interfaces
        zope.component.testing.setUp()
        self.channel_manager = mock.Mock(
            spec=list(gocept.amqprun.interfaces.IChannelManager))
        zope.component.provideAdapter(
            mock.Mock(return_value=self.channel_manager),
            adapts=(mock.Mock,),
            provides=gocept.amqprun.interfaces.IChannelManager)
        self.reader = mock.Mock()
        self.reader.connection = self.connection = mock.Mock()
        self.connection.lock = threading.Lock()
        self.reader.channel = self.channel = mock.Mock()

    def tearDown(self):
        zope.component.testing.tearDown()

    def get_message(self):
        import gocept.amqprun.message
        return gocept.amqprun.message.Message({}, '', 'mytag')

    def get_dm(self):
        import gocept.amqprun.session
        self.session = gocept.amqprun.session.Session(
            self.reader, self.get_message())
        return gocept.amqprun.session.AMQPDataManager(
            self.reader, self.session, self.get_message())

    def test_interface(self):
        import transaction.interfaces
        zope.interface.verify.verifyObject(
            transaction.interfaces.IDataManager, self.get_dm())

    def test_tpc_begin_should_acquire_connection_lock(self):
        dm = self.get_dm()
        dm.tpc_begin(None)
        self.assertFalse(self.connection.lock.acquire(False))

    def test_tpc_begin_should_call_tx_select(self):
        dm = self.get_dm()
        dm.tpc_begin(None)
        self.assertTrue(self.channel.tx_select.called)

    def test_tpc_commit_should_ack_message(self):
        dm = self.get_dm()
        dm.commit(None)
        self.assertTrue(self.channel.basic_ack.called)
        self.assertEquals(('mytag',), self.channel.basic_ack.call_args[0])

    def test_tpc_vote_should_commit_transaction(self):
        dm = self.get_dm()
        dm.tpc_vote(None)
        self.assertTrue(self.channel.tx_commit.called)

    def test_tpc_finish_should_release_connection_lock(self):
        dm = self.get_dm()
        self.connection.lock.acquire()
        dm.tpc_finish(None)
        self.assertTrue(self.connection.lock.acquire(False))

    def test_tpc_abort_should_release_connection_lock(self):
        dm = self.get_dm()
        self.connection.lock.acquire()
        dm.tpc_abort(None)
        self.assertTrue(self.connection.lock.acquire(False))

    def test_tpc_abort_should_tx_rollback(self):
        dm = self.get_dm()
        self.connection.lock.acquire()
        dm.tpc_abort(None)
        self.assertTrue(self.channel.tx_rollback.called)

    def test_tpc_abort_should_not_reject_message(self):
        dm = self.get_dm()
        self.connection.lock.acquire()
        dm.tpc_abort(None)
        self.assertFalse(self.channel.basic_reject.called)

    def test_abort_should_not_reject_message(self):
        dm = self.get_dm()
        dm.abort(None)
        self.assertFalse(self.channel.basic_reject.called)

    def test_abort_should_acquire_and_release_lock(self):
        dm = self.get_dm()
        self.connection.lock.acquire()
        self.assertFalse(self.channel.basic_reject.called)
        t = threading.Thread(target=dm.abort, args=(None,))
        t.start()
        time.sleep(0.1)  # Let the thread start up
        self.assertFalse(self.channel.basic_reject.called)
        self.connection.lock.release()
        time.sleep(0.1)
        self.assertFalse(self.channel.basic_reject.called)
        self.assertTrue(self.connection.lock.acquire(False))

    def test_commit_should_send_queued_messages(self):
        dm = self.get_dm()
        m1 = mock.Mock()
        m1.header.headers = None
        m2 = mock.Mock()
        m2.header.headers = None
        self.session.send(m1)
        self.session.send(m2)
        dm.commit(None)
        self.assertEqual(2, self.channel.basic_publish.call_count)
        self.channel.basic_publish.assert_called_with(
            m2.exchange, m2.routing_key, m2.body, m2.header)

    def test_messages_should_be_sent_with_correlation_id(self):
        dm = self.get_dm()
        dm.message.header.message_id = 'message id'
        msg = mock.Mock()
        msg.header.correlation_id = None
        msg.header.headers = None
        self.session.send(msg)
        dm.commit(None)
        self.assertEqual('message id', msg.header.correlation_id)

    def test_existing_correlation_id_should_not_be_overwritten(self):
        dm = self.get_dm()
        dm.message.header.message_id = 'message id'
        msg = mock.Mock()
        msg.header.correlation_id = mock.sentinel.correlation_id
        msg.header.headers = None
        self.session.send(msg)
        dm.commit(None)
        self.assertEqual(mock.sentinel.correlation_id,
                         msg.header.correlation_id)

    def test_messages_should_be_sent_with_references_header(self):
        dm = self.get_dm()
        dm.message.header.message_id = 'message id'
        msg = mock.Mock()
        msg.header.headers = None
        self.session.send(msg)
        dm.commit(None)
        ((_, _, _, header), _) = self.channel.basic_publish.call_args
        self.assertIn('references', header.headers)
        self.assertEqual('message id', header.headers['references'])

    def test_references_header_contains_parents_references(self):
        dm = self.get_dm()
        dm.message.header.message_id = 'message id'
        dm.message.header.headers = {'references': 'parent id'}
        msg = mock.Mock()
        msg.header.headers = None
        self.session.send(msg)
        dm.commit(None)
        ((_, _, _, header), _) = self.channel.basic_publish.call_args
        self.assertIn('references', header.headers)
        self.assertEqual('parent id\nmessage id', header.headers['references'])

    def test_existing_references_header_should_not_be_overwritten(self):
        dm = self.get_dm()
        dm.message.header.message_id = 'message id'
        msg = mock.Mock()
        msg.header.headers = {'references': 'custom id'}
        self.session.send(msg)
        dm.commit(None)
        ((_, _, _, header), _) = self.channel.basic_publish.call_args
        self.assertEqual('custom id', header.headers['references'])

    def test_no_references_should_be_created_when_parent_lacks_message_id(self):
        dm = self.get_dm()
        dm.message.header.message_id = None
        msg = mock.Mock()
        msg.header.headers = {}
        self.session.send(msg)
        dm.commit(None)
        ((_, _, _, header), _) = self.channel.basic_publish.call_args
        self.assertNotIn('references', header.headers)

    def test_abort_should_discard_queued_messages(self):
        dm = self.get_dm()
        m1 = mock.Mock()
        self.session.send(m1)
        dm.abort(None)
        self.assertEqual([], self.session.messages)

    def test_tpc_abort_should_discard_queued_messages(self):
        dm = self.get_dm()
        m1 = mock.Mock()
        self.session.send(m1)
        self.connection.lock.acquire()
        dm.tpc_abort(None)
        self.assertEqual([], self.session.messages)
        self.assertTrue(self.connection.lock.acquire(False))

    def test_abort_should_not_do_anything_after_tpc_begin(self):
        dm = self.get_dm()
        dm.tpc_begin(None)
        dm.abort(None)
        self.assertFalse(self.channel.basic_reject.called)

    def test_init_should_acquire_channel(self):
        self.get_dm()
        self.assertTrue(self.channel_manager.acquire.called)

    def test_tpc_finish_should_release_channel(self):
        dm = self.get_dm()
        self.connection.lock.acquire()
        dm.tpc_finish(None)
        self.assertTrue(self.channel_manager.release.called)

    def test_tpc_abort_should_release_channel(self):
        dm = self.get_dm()
        self.connection.lock.acquire()
        dm.tpc_abort(None)
        self.assertTrue(self.channel_manager.release.called)

    def test_abort_should_release_channel(self):
        dm = self.get_dm()
        dm.abort(None)
        self.assertTrue(self.channel_manager.release.called)


class SessionTest(unittest.TestCase):

    def setUp(self):
        self.patcher = mock.patch('gocept.amqprun.session.AMQPDataManager')
        self.patcher.__enter__()

    def tearDown(self):
        self.patcher.__exit__()

    def create_session(self):
        from gocept.amqprun.session import Session
        return Session(mock.Mock())

    def test_send_should_queue_messages(self):
        message = mock.sentinel.message
        session = self.create_session()
        session.send(message)
        self.assertEqual([message], session.messages)

    def test_session_provides_interface(self):
        import gocept.amqprun.interfaces
        zope.interface.verify.verifyObject(
            gocept.amqprun.interfaces.ISession, self.create_session())

    @mock.patch('transaction.get')
    def test_joins_transaction_on_first_send(self, transaction_get):
        session = self.create_session()
        session.send('asdf')
        self.assertTrue(transaction_get().join.called)

    @mock.patch('transaction.get')
    def test_joins_only_once_on_send(self, transaction_get):
        session = self.create_session()
        session.send('asdf')
        session.send('bsdf')
        self.assertEqual(1, transaction_get().join.call_count)
