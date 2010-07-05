# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import gocept.amqprun.testing
import mock
import threading
import time
import unittest
import zope.component
import zope.interface.verify


class MessageReaderTest(gocept.amqprun.testing.QueueTestCase):

    def test_loop_can_be_stopped_from_outside(self):
        # this test simply should not hang indefinitely
        self.create_reader()

    def test_messages_wo_handler_declaration_should_not_arrive_in_tasks(self):
        self.create_reader()
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
        self.create_reader()

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
        self.create_reader()

        self.assertEqual(0, self.reader.tasks.qsize())
        # With routing key, the message is delivered to the correct handler
        self.send_message('foo', routing_key='test.messageformat.2')
        self.assertEqual(1, self.reader.tasks.qsize())
        handler = self.reader.tasks.get()
        handler()
        self.assertFalse(handle_message_2.called)
        self.assertTrue(handle_message_1.called)

    @mock.patch('transaction.get')
    def test_create_session_returns_session_and_joins_transaction(
        self, transaction_get):
        self.create_reader()
        handler = mock.Mock()
        session = self.reader.create_session(handler)
        self.assertTrue(transaction_get().join.called)
        dm = transaction_get().join.call_args[0][0]
        self.assertEqual(session, dm.session)
        self.assertEqual(self.reader.connection.lock, dm.connection_lock)


class DataManagerTest(unittest.TestCase):

    def setUp(self):
        self.reader = mock.Mock()
        self.reader.connection = self.connection = mock.Mock()
        self.connection.lock = threading.Lock()
        self.reader.channel = self.channel = mock.Mock()

    def get_message(self):
        import gocept.amqprun.server
        return gocept.amqprun.server.Message({}, '', 'mytag')

    def get_dm(self):
        import gocept.amqprun.server
        self.session = gocept.amqprun.server.Session()
        return gocept.amqprun.server.AMQPDataManager(self.reader,
                                                     self.get_message(),
                                                     self.session)

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

    #def test_tpc_abort_should_reject_message(self):
    #    dm = self.get_dm()
    #    self.connection.lock.acquire()
    #    dm.tpc_abort(None)
    #    self.assertTrue(self.channel.basic_reject.called)

    #def test_abort_should_reject_message(self):
    #    dm = self.get_dm()
    #    dm.abort(None)
    #    self.assertTrue(self.channel.basic_reject.called)

    def test_abort_should_acquire_and_release_lock(self):
        dm = self.get_dm()
        self.connection.lock.acquire()
        #self.assertFalse(self.channel.basic_reject.called)
        t = threading.Thread(target=dm.abort, args=(None,))
        t.start()
        time.sleep(0.1)  # Let the thread start up
        #self.assertFalse(self.channel.basic_reject.called)
        # After releasing the lock, basic_reject gets called
        self.connection.lock.release()
        time.sleep(0.1)
        #self.assertTrue(self.channel.basic_reject.called)
        self.assertTrue(self.connection.lock.acquire(False))

    def test_commit_should_send_queued_messages(self):
        dm = self.get_dm()
        m1 = mock.Mock()
        m2 = mock.Mock()
        self.session.send(m1)
        self.session.send(m2)
        dm.commit(None)
        self.assertEqual(2, self.channel.basic_publish.call_count)
        self.channel.basic_publish.assert_called_with(
            m2.exchange, m2.routing_key, m2.body, m2.header)

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


class TestMainWithQueue(gocept.amqprun.testing.MainTestCase):

    def test_message_should_be_processed(self):
        config = self.make_config(__name__, 'integration')
        self._queues.append('test.queue')
        self.create_reader()

        from gocept.amqprun.tests.integration import messages_received
        self.assertEquals([], messages_received)
        self.send_message('blarf', routing_key='test.routing')
        for i in range(100):
            if messages_received:
                break
        else:
            self.fail('Message was not received')
        self.assertEquals(1, len(messages_received))

    def test_message_with_error_should_reject(self):
        config = self.make_config(__name__, 'integration')
        self._queues.append('test.error')
        self.create_reader()

        self.reader = gocept.amqprun.server.main_reader

        from gocept.amqprun.tests.integration import messages_received
        self.assertEquals([], messages_received)
        self.send_message('blarf', routing_key='test.routing')
        for i in range(100):
            if messages_received:
                break
        else:
            self.fail('Message was not received')
        self.assertEquals(1, len(messages_received))


    @mock.patch('gocept.amqprun.server.MessageReader')
    @mock.patch('gocept.amqprun.worker.Worker')
    def test_basic_configuration_should_load_zcml(self, worker, reader):
        import gocept.amqprun.interfaces
        import gocept.amqprun.server
        config = self.make_config(__name__, 'basic')
        gocept.amqprun.server.main(config)
        self.assertEquals(1, reader.call_count)
        self.assertEquals(2, worker.call_count)
        utilities = list(zope.component.getUtilitiesFor(
            gocept.amqprun.interfaces.IHandlerDeclaration))
        self.assertEquals(1, len(utilities))
        self.assertEquals('basic', utilities[0][0])

    @mock.patch('gocept.amqprun.server.MessageReader')
    @mock.patch('gocept.amqprun.worker.Worker')
    def test_settings_should_be_available_through_utility(self, _1, _2):
        import gocept.amqprun.interfaces
        import gocept.amqprun.server
        config = self.make_config(__name__, 'settings')
        gocept.amqprun.server.main(config)
        settings = zope.component.getUtility(
            gocept.amqprun.interfaces.ISettings)
        self.assertEquals('foo', settings.get('test.setting.1'))
        self.assertEquals('bar', settings.get('test.setting.2'))


class TestMessage(unittest.TestCase):

    def test_message_should_provide_IMessage(self):
        from gocept.amqprun.server import Message
        import gocept.amqprun.interfaces
        message = Message({}, 2, 3)
        zope.interface.verify.verifyObject(
            gocept.amqprun.interfaces.IMessage,
            message)
        self.assertEqual(2, message.body)

    def test_header_should_be_converted_to_BasicProperties(self):
        from gocept.amqprun.server import Message
        import pika.spec
        now = int(time.time())
        message = Message(
            dict(foo='bar', content_type='text/xml', app_id='myapp'), None,
            delivery_tag=1)
        header = message.header
        self.assertTrue(isinstance(header, pika.spec.BasicProperties))
        self.assertEqual(dict(foo='bar'), header.headers)
        self.assertTrue(header.timestamp >= now)
        self.assertEqual('text/xml', header.content_type)
        self.assertEqual(2, header.delivery_mode) # persistent
        self.assertEqual('myapp', header.app_id)

    def test_routing_key_and_delivery_tag_should_be_exclusive(self):
        from gocept.amqprun.server import Message
        self.assertRaises(
            zope.interface.Invalid,
            Message, {}, None, delivery_tag=1, routing_key=2)


class SessionTest(unittest.TestCase):

    def test_send_should_queue_messages(self):
        from gocept.amqprun.server import Session
        message = mock.sentinel.message
        session = Session()
        session.send(message)
        self.assertEqual([message], session.messages)

    def test_session_provides_interface(self):
        from gocept.amqprun.server import Session
        import gocept.amqprun.interfaces
        zope.interface.verify.verifyObject(
            gocept.amqprun.interfaces.ISession, Session())
