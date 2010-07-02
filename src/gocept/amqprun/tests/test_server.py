# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import amqplib.client_0_8 as amqp
import mock
import threading
import time
import unittest
import zope.component
import zope.component.testing


class MessageReaderTest(unittest.TestCase):

    hostname = 'localhost'

    def setUp(self):
        self._queue_prefix = 'test.%f.' % time.time()
        self._queues = []
        zope.component.testing.setUp()
        self.connection = amqp.Connection(host=self.hostname)
        self.channel = self.connection.channel()

    def tearDown(self):
        self.reader.stop()
        self.thread.join()

        for queue_name in self._queues:
            self.channel.queue_delete(queue_name)
        self.channel.close()
        self.connection.close()
        zope.component.testing.tearDown()

    def get_queue_name(self, suffix):
        queue_name = self._queue_prefix + suffix
        self._queues.append(queue_name)
        return queue_name

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

    def test_messages_wo_handler_declaration_should_not_arrive_in_tasks(self):
        self._create_reader()
        self.send_message('foo')
        self.assertEqual(0, self.reader.tasks.qsize())

    def test_messages_with_handler_should_arrive_in_task_queue(self):
        import gocept.amqprun.handler
        # Provide a handler
        handle_message = mock.Mock()
        decl = gocept.amqprun.handler.HandlerDeclaration(
            self.get_queue_name('test.case.1'),
            'test.messageformat.1', handle_message)
        zope.component.provideUtility(decl, name='queue')

        # Start up the reader
        self._create_reader()

        # Without routing key, the message is not delivered
        self.assertEqual(0, self.reader.tasks.qsize())
        self.send_message('foo')
        self.assertEqual(0, self.reader.tasks.qsize())
        # With routing key, the message is delivered
        self.send_message('foo', routing_key='test.messageformat.1')
        self.assertEqual(1, self.reader.tasks.qsize())
        handler = self.reader.tasks.get()
        handler()
        self.assertTrue(handle_message.called)
        message = handle_message.call_args[0][0]
        self.assertEquals('foo', message.body)

    def test_different_handlers_should_be_handled_separately(self):
        import gocept.amqprun.handler
        # Provide two handlers
        handle_message_1 = mock.Mock()
        handle_message_2 = mock.Mock()
        decl_1 = gocept.amqprun.handler.HandlerDeclaration(
            self.get_queue_name('test.case.2'),
            'test.messageformat.2', handle_message_1)
        decl_2 = gocept.amqprun.handler.HandlerDeclaration(
            self.get_queue_name('test.case.3'),
            'test.messageformat.3', handle_message_2)
        zope.component.provideUtility(decl_1, name='1')
        zope.component.provideUtility(decl_2, name='2')

        # Start up the reader
        self._create_reader()

        self.assertEqual(0, self.reader.tasks.qsize())
        # With routing key, the message is delivered to the correct handler
        self.send_message('foo', routing_key='test.messageformat.2')
        self.assertEqual(1, self.reader.tasks.qsize())
        handler = self.reader.tasks.get()
        handler()
        self.assertFalse(handle_message_2.called)
        self.assertTrue(handle_message_1.called)


    def test_create_datamanager_returns_dm_with_lock_and_message(self):
        self._create_reader()
        message = mock.Mock()
        dm1 = self.reader.create_datamanager(message)
        dm2 = self.reader.create_datamanager(message)
        self.assertTrue(dm1.connection_lock is dm2.connection_lock)

    def send_message(self, body, routing_key=''):
        self.channel.basic_publish(amqp.Message(body), 'amq.topic',
                                   routing_key=routing_key)
        time.sleep(0.05)


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
