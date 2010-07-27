# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import collections
import gocept.amqprun.testing
import mock
import os
import threading
import time
import unittest
import zope.component
import zope.component.testing
import zope.interface.verify


class MessageReaderTest(
    gocept.amqprun.testing.LoopTestCase,
    gocept.amqprun.testing.QueueTestCase):

    def create_reader(self):
        import gocept.amqprun.server
        self.reader = gocept.amqprun.server.MessageReader(self.layer.hostname)
        self.start_thread(self.reader)

    def test_loop_can_be_stopped_from_outside(self):
        # this test simply should not hang indefinitely
        self.create_reader()

    @mock.patch('gocept.amqprun.server.Connection')
    def test_loop_should_exit_on_keyboardinterrupt(self, Connection):
        Connection().lock = threading.Lock()
        Connection().drain_events.side_effect = KeyboardInterrupt
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

    def test_unicode_queue_names_should_work(self):
        import gocept.amqprun.handler
        decl = gocept.amqprun.handler.HandlerDeclaration(
            self.get_queue_name(u'test.case.2'),
            'test.messageformat.2', lambda x: None)
        zope.component.provideUtility(decl, name='decl')
        self.create_reader()

    def test_unicode_routing_keys_should_work_for_handler(self):
        import gocept.amqprun.handler
        decl = gocept.amqprun.handler.HandlerDeclaration(
            self.get_queue_name('test.case.2'),
            u'test.messageformat.2', lambda x: None)
        zope.component.provideUtility(decl, name='decl')
        self.create_reader()

    def test_unicode_routing_keys_should_work_for_message(self):
        import gocept.amqprun.server
        import transaction
        self.create_reader()
        handler = mock.Mock()
        session = self.reader.create_session(handler)
        session.send(gocept.amqprun.server.Message(
            {}, 'body', routing_key=u'test.routing'))
        # Patch out basic_ack because we haven't actually received a message
        with mock.patch('pika.channel.Channel.basic_ack'):
            transaction.commit()

    def test_unicode_body_should_work_for_message(self):
        import gocept.amqprun.server
        import transaction
        self.create_reader()
        handler = mock.Mock()
        session = self.reader.create_session(handler)
        session.send(gocept.amqprun.server.Message(
            {}, u'body', routing_key='test.routing'))
        # Patch out basic_ack because we haven't actually received a message
        with mock.patch('pika.channel.Channel.basic_ack'):
            transaction.commit()

    def test_unicode_header_should_work_for_message(self):
        import gocept.amqprun.server
        import transaction
        self.create_reader()
        handler = mock.Mock()
        session = self.reader.create_session(handler)
        session.send(gocept.amqprun.server.Message(
            {u'content_type': u'text/plain'},
            'body', routing_key='test.routing'))
        # Patch out basic_ack because we haven't actually received a message
        with mock.patch('pika.channel.Channel.basic_ack'):
            transaction.commit()


class TestChannelSwitch(unittest.TestCase):

    def setUp(self):
        import gocept.amqprun.channel
        import gocept.amqprun.interfaces
        zope.component.testing.setUp()
        zope.component.provideAdapter(
            gocept.amqprun.channel.Manager,
            adapts=(mock.Mock,))

    def tearDown(self):
        zope.component.testing.tearDown()

    def get_reader(self):
        from gocept.amqprun.server import MessageReader
        import pika.channel
        reader = MessageReader(mock.sentinel.hostname)
        reader.connection = mock.Mock()
        reader.connection.lock = threading.Lock()
        reader.channel = mock.Mock(pika.channel.Channel)
        reader.channel.callbacks = collections.OrderedDict()
        new_channel = mock.Mock(pika.channel.Channel)
        new_channel.callbacks = {}
        reader.connection.channel.return_value = new_channel
        return reader

    def test_switch_channel_should_cancel_consume_on_old_channel(self):
        reader = self.get_reader()
        channel = reader.channel
        channel.callbacks[mock.sentinel.ct1] = mock.sentinel.callback1
        channel.callbacks[mock.sentinel.ct2] = mock.sentinel.callback2
        reader.switch_channel()
        self.assertEqual(2, channel.basic_cancel.call_count)
        channel.basic_cancel.assert_called_with(mock.sentinel.ct2)

    def test_switch_channel_should_empty_tasks(self):
        reader = self.get_reader()
        reader.tasks.put(mock.sentinel.task1)
        reader.tasks.put(mock.sentinel.task2)
        reader.switch_channel()
        self.assertEqual(0, reader.tasks.qsize())

    def test_switch_channel_should_open_new_channel(self):
        reader = self.get_reader()
        old_channel = reader.channel
        reader.switch_channel()
        self.assertTrue(reader.connection.channel.called)
        new_channel = reader.connection.channel()
        self.assertEqual(new_channel, reader.channel)

    def test_switch_channel_calls_consume_on_new_channel(self):
        from gocept.amqprun.handler import HandlerDeclaration
        reader = self.get_reader()
        decl = HandlerDeclaration(
            'queue_name', 'routing_key', lambda x: None)
        zope.component.provideUtility(decl, name='handler')
        reader.switch_channel()
        self.assertTrue(reader.channel.basic_consume.called)

    def test_switch_channel_should_return_false_when_alrady_switching(self):
        reader = self.get_reader()
        self.assertTrue(reader.switch_channel())  # Switched
        # Switch is still in progress as the old channel hasn't been closed.
        self.assertFalse(reader.switch_channel())
        # After switch has been completed, switching becomes possible again
        reader._switching_channels = False
        self.assertTrue(reader.switch_channel())

    def test_switch_channel_should_acquire_and_release_connection_lock(self):
        reader = self.get_reader()
        reader.connection.lock.acquire()
        t = threading.Thread(target=reader.switch_channel)
        t.start()
        time.sleep(0.1)
        self.assertFalse(reader._switching_channels)
        reader.connection.lock.release()
        time.sleep(0.1)
        self.assertTrue(reader._switching_channels)
        self.assertTrue(reader.connection.lock.acquire(False))

    def test_close_old_channel_should_acquire_and_release_lock(self):
        reader = self.get_reader()
        reader.switch_channel()
        reader.connection.lock.acquire()
        t = threading.Thread(target=reader.close_old_channel)
        t.start()
        time.sleep(0.1)
        self.assertTrue(reader._switching_channels)
        reader.connection.lock.release()
        time.sleep(0.1)
        self.assertFalse(reader._switching_channels)
        self.assertTrue(reader.connection.lock.acquire(False))

    def test_run_calls_switch_channel_once_in_a_while(self):
        import gocept.amqprun.interfaces
        reader = self.get_reader()
        gocept.amqprun.interfaces.IChannelManager(reader.channel).acquire()
        self.assertEquals(360, reader.CHANNEL_LIFE_TIME)
        reader.CHANNEL_LIFE_TIME = 0.4
        reader._channel_opened = time.time()
        # Channels are not switched immediately after startup
        reader.run_once()
        self.assertFalse(reader._switching_channels)
        # Channels are switched only after the set channel life time.
        time.sleep(0.25)
        reader.run_once()
        self.assertFalse(reader._switching_channels)
        # Channels are switched after the set channel life time.
        time.sleep(0.25)
        reader.run_once()
        self.assertTrue(reader._switching_channels)

    def test_old_channel_should_be_closed(self):
        reader = self.get_reader()
        old_channel = reader.channel
        reader.switch_channel()
        self.assertEqual(old_channel, reader._old_channel)
        reader.run_once()
        self.assertTrue(old_channel.close.called)
        self.assertIsNone(reader._old_channel)
        self.assertFalse(reader._switching_channels)

    def test_old_channel_should_remain_if_closing_is_not_possible(self):
        import gocept.amqprun.interfaces
        reader = self.get_reader()
        old_channel = reader.channel
        gocept.amqprun.interfaces.IChannelManager(old_channel).acquire()
        reader.switch_channel()
        self.assertEqual(old_channel, reader._old_channel)
        reader.run_once()
        self.assertFalse(old_channel.close.called)
        self.assertIsNotNone(reader._old_channel)
        # After releasing the channel is closed
        gocept.amqprun.interfaces.IChannelManager(old_channel).release()
        reader.run_once()
        self.assertTrue(old_channel.close.called)
        self.assertIsNone(reader._old_channel)


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

    # XXX reject is not implemented by RabbitMQ
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

    def test_init_should_acquire_channel(self):
        dm = self.get_dm()
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


class TestMainWithQueue(gocept.amqprun.testing.MainTestCase):

    def test_message_should_be_processed(self):
        self.make_config(__name__, 'integration')
        self._queues.append('test.queue')
        self._queues.append('test.queue.error')
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

    def test_technical_errors_should_not_crash(self):
        self.make_config(__name__, 'integration')
        self._queues.append('test.queue')
        self._queues.append('test.queue.error')
        self.create_reader()

        self.reader = gocept.amqprun.server.main_reader

        from gocept.amqprun.tests.integration import messages_received
        self.assertEquals([], messages_received)
        self.send_message('blarf', routing_key='test.error')
        for i in range(100):
            if messages_received:
                break
        else:
            self.fail('Message was not received')
        self.assertEquals(1, len(messages_received))

    def test_rejected_messages_should_be_received_again_later(self):
        self.make_config(__name__, 'integration')
        self._queues.append('test.queue')
        self._queues.append('test.queue.error')
        self.create_reader()
        self.reader = gocept.amqprun.server.main_reader
        self.reader.CHANNEL_LIFE_TIME = 1

        from gocept.amqprun.tests.integration import messages_received
        self.assertEqual([], messages_received)
        self.send_message('blarf', routing_key='test.error')
        for i in range(200):
            time.sleep(0.025)
            os.write(self.reader.connection.notifier_w, 'W')
            if len(messages_received) == 2:
                break
        else:
            self.fail('Message was not received again')

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
        self.assertEqual('amq.topic', message.exchange)

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
