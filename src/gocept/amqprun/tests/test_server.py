# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import amqplib.client_0_8 as amqp
import mock
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
        for i in range(100):
            if self.reader.running:
                break
            time.sleep(0.025)
        else:
            self.fail('Reader did not start up.')


    def test_loop_can_be_stopped_from_outside(self):
        # this test simply should not hang indefinitely
        self._create_reader()

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

    def test_create_datamanager_returns_dm_with_lock_and_message(self):
        self._create_reader()
        message = mock.Mock()
        dm1 = self.reader.create_datamanager(message)
        dm2 = self.reader.create_datamanager(message)
        self.assertTrue(dm1.connection_lock is dm2.connection_lock)

    def send_message(self, body):
        self.channel.basic_publish(amqp.Message(body), 'amq.fanout')
        time.sleep(0.1)


class DataManagerTest(unittest.TestCase):

    def setUp(self):
        self.connection_lock = threading.Lock()
        self.channel = mock.Mock()

    def get_message(self):
        import gocept.amqprun.server
        method = mock.Mock()
        method.delivery_tag = 'mytag'
        return gocept.amqprun.server.Message(
            self.channel, method, {}, '')

    def get_dm(self):
        import gocept.amqprun.server
        return gocept.amqprun.server.AMQPDataManager(self.connection_lock,
                                                     self.get_message())

    def test_interface(self):
        import transaction.interfaces
        import zope.interface.verify
        zope.interface.verify.verifyObject(
            transaction.interfaces.IDataManager, self.get_dm())

    def test_tpc_begin_should_acquire_connection_lock(self):
        dm = self.get_dm()
        dm.tpc_begin(None)
        self.assertFalse(self.connection_lock.acquire(False))

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
        self.connection_lock.acquire()
        dm.tpc_finish(None)
        self.assertTrue(self.connection_lock.acquire(False))

    def test_tpc_abort_should_release_connection_lock(self):
        dm = self.get_dm()
        self.connection_lock.acquire()
        dm.tpc_abort(None)
        self.assertTrue(self.connection_lock.acquire(False))

    def test_tpc_abort_should_tx_rollback(self):
        dm = self.get_dm()
        self.connection_lock.acquire()
        dm.tpc_abort(None)
        self.assertTrue(self.channel.tx_rollback.called)

    def test_tpc_abort_should_reject_message(self):
        dm = self.get_dm()
        self.connection_lock.acquire()
        dm.tpc_abort(None)
        self.assertTrue(self.channel.tx_reject.called)

    def test_abort_should_reject_message(self):
        dm = self.get_dm()
        dm.abort(None)
        self.assertTrue(self.channel.tx_reject.called)

    def test_abort_should_acquire_and_release_lock(self):
        dm = self.get_dm()
        self.connection_lock.acquire()
        self.assertFalse(self.channel.tx_reject.called)
        t = threading.Thread(target=dm.abort, args=(None,))
        t.start()
        time.sleep(0.1)  # Let the thread start up
        self.assertFalse(self.channel.tx_reject.called)
        # After releasing the lock, tx_reject gets called
        self.connection_lock.release()
        time.sleep(0.1)
        self.assertTrue(self.channel.tx_reject.called)
        self.assertTrue(self.connection_lock.acquire(False))
