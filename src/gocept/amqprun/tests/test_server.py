# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import collections
import gocept.amqprun.testing
import os
import mock
import pika.spec
import random
import signal
import subprocess
import sys
import threading
import tempfile
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

        class Parameters(object):
            hostname = self.layer.hostname
            port = None
            virtual_host = '/'
            username = None
            password = None
            heartbeat_interval = 0
        self.reader = gocept.amqprun.server.MessageReader(Parameters)
        self.start_thread(self.reader)

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

    def test_unicode_queue_names_should_work(self):
        import gocept.amqprun.interfaces
        class Decl(object):
            zope.interface.classProvides(
                gocept.amqprun.interfaces.IHandlerDeclaration)
            queue_name = self.get_queue_name(u'test.case.2')
            routing_key = 'test.messageformat.2'
            arguments = {}
        zope.component.provideUtility(Decl, name='decl')
        self.create_reader()

    def test_unicode_routing_keys_should_work_for_handler(self):
        import gocept.amqprun.interfaces
        class Decl(object):
            zope.interface.classProvides(
                gocept.amqprun.interfaces.IHandlerDeclaration)
            queue_name = self.get_queue_name('test.case.2')
            routing_key = u'test.messageformat.2'
            arguments = {}
        zope.component.provideUtility(Decl, name='decl')
        self.create_reader()

    def test_unicode_arguments_should_work_for_handler(self):
        import gocept.amqprun.interfaces
        class Decl(object):
            zope.interface.classProvides(
                gocept.amqprun.interfaces.IHandlerDeclaration)
            queue_name = self.get_queue_name('test.case.2')
            routing_key = 'test.messageformat.2'
            arguments = {u'x-ha-policy': u'all'}
        zope.component.provideUtility(Decl, name='decl')
        self.create_reader()

    def test_unicode_routing_keys_should_work_for_message(self):
        import gocept.amqprun.message
        import transaction
        self.create_reader()
        handler = mock.Mock()
        handler.message.header.message_id = None
        handler.message.header.headers = None
        session = self.reader.create_session(handler)
        session.send(gocept.amqprun.message.Message(
            {}, 'body', routing_key=u'test.routing'))
        # Patch out basic_ack because we haven't actually received a message
        with mock.patch('pika.channel.Channel.basic_ack'):
            transaction.commit()

    def test_unicode_body_should_work_for_message(self):
        import gocept.amqprun.message
        import transaction
        self.create_reader()
        handler = mock.Mock()
        handler.message.header.message_id = None
        handler.message.header.headers = None
        session = self.reader.create_session(handler)
        session.send(gocept.amqprun.message.Message(
            {}, u'body', routing_key='test.routing'))
        # Patch out basic_ack because we haven't actually received a message
        with mock.patch('pika.channel.Channel.basic_ack'):
            transaction.commit()

    def test_unicode_header_should_work_for_message(self):
        import gocept.amqprun.message
        import transaction
        self.create_reader()
        handler = mock.Mock()
        handler.message.header.message_id = None
        session = self.reader.create_session(handler)
        session.send(gocept.amqprun.message.Message(
            {u'content_type': u'text/plain'},
            'body', routing_key='test.routing'))
        # Patch out basic_ack because we haven't actually received a message
        with mock.patch('pika.channel.Channel.basic_ack'):
            transaction.commit()

    def test_multiple_routing_keys_should_recieve_messages_for_all(self):
        import gocept.amqprun.handler
        handle_message = mock.Mock()
        decl = gocept.amqprun.handler.HandlerDeclaration(
            self.get_queue_name('test.routing'),
            ['route.1', 'route.2'], handle_message)
        zope.component.provideUtility(decl, name='decl')
        self.create_reader()

        self.assertEqual(0, self.reader.tasks.qsize())
        self.send_message('foo', routing_key='route.1')
        self.send_message('bar', routing_key='route.2')
        self.assertEqual(2, self.reader.tasks.qsize())


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
        new_channel.handler = mock.Mock()
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
        reader.connection.lock = mock.Mock()
        reader.switch_channel()
        self.assertTrue(reader.connection.lock.acquire.called)
        self.assertTrue(reader.connection.lock.release.called)
        self.assertTrue(reader._switching_channels)

    def test_close_old_channel_should_acquire_and_release_lock(self):
        reader = self.get_reader()
        reader.connection.lock = mock.Mock()
        reader.switch_channel()
        reader.close_old_channel()
        self.assertTrue(reader.connection.lock.acquire.called)
        self.assertTrue(reader.connection.lock.release.called)
        self.assertFalse(reader._switching_channels)

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

    def test_active_connection_lock_should_defer_switch_channel(self):
        reader = self.get_reader()
        reader.connection.lock.acquire()
        self.assertFalse(reader.switch_channel())

    def test_active_connection_lock_should_defer_close_old_channel(self):
        reader = self.get_reader()
        reader.connection.lock.acquire()
        reader._old_channel = mock.sentinel.channel
        # The assertion is that the call doesn't hang :/
        reader.close_old_channel()


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


class ConnectionTest(unittest.TestCase):

    def test_parameters_should_be_converted_to_pika(self):
        from gocept.amqprun.server import Connection
        with mock.patch('pika.AsyncoreConnection.__init__') as init:
            conn = Connection(mock.sentinel)
            conn.finish_init()
            params = init.call_args[0][1]
            self.assertEqual(mock.sentinel.hostname, params.host)
            self.assertEqual(mock.sentinel.port, params.port)
            self.assertEqual(mock.sentinel.virtual_host, params.virtual_host)
            self.assertEqual(
                mock.sentinel.heartbeat_interval, params.heartbeat)
            self.assertEqual(
                mock.sentinel.username, params.credentials.username)
            self.assertEqual(
                mock.sentinel.password, params.credentials.password)

    def test_no_username_and_password_should_yield_default_credentials(self):
        from gocept.amqprun.server import Connection
        with mock.patch('pika.AsyncoreConnection.__init__') as init:
            parameters = mock.Mock()
            parameters.username = None
            parameters.password = None
            conn = Connection(parameters)
            conn.finish_init()
            params = init.call_args[0][1]
            self.assertIsNone(params.credentials)


class DyingRabbitTest(
    gocept.amqprun.testing.LoopTestCase,
    gocept.amqprun.testing.QueueTestCase):

    pid = None

    def tearDown(self):
        super(DyingRabbitTest, self).tearDown()
        if self.pid:
            os.kill(self.pid, signal.SIGINT)

    def create_reader(self, _port=None):
        import gocept.amqprun.server

        class Parameters(object):
            hostname = self.layer.hostname
            port = _port
            virtual_host = '/'
            username = None
            password = None
            heartbeat_interval = 0
        self.reader = gocept.amqprun.server.MessageReader(Parameters)
        self.start_thread(self.reader)

    def test_socket_close_should_not_stop_main_loop_and_open_connection(self):
        # This hopefully simulates local errors
        self.create_reader()
        self.assertTrue(self.reader.connection.is_alive())
        time.sleep(0.5)
        self.reader.connection.dispatcher.socket.close()
        self.reader.connection.notify()
        time.sleep(3)

        self.assertTrue(self.thread.is_alive())
        self.assertTrue(self.reader.connection.is_alive())
        self.assertEqual(self.reader.connection.channels[1],
                         self.reader.channel.handler)
        # Do something with the channel
        self.reader.channel.tx_select()

    def test_remote_close_should_reopen_connection(self):
        port = random.randint(30000, 40000)
        pid = self.tcpwatch(port)
        self.create_reader(port)
        old_channel = self.reader.channel
        self.assertTrue(self.reader.connection.is_alive())

        os.kill(pid, signal.SIGINT)
        self.pid = self.tcpwatch(port)
        time.sleep(1)

        self.assertTrue(self.thread.is_alive())
        self.assertTrue(self.reader.connection.is_alive())
        self.assertEqual(self.reader.connection.channels[1],
                         self.reader.channel.handler)
        self.assertNotEqual(old_channel, self.reader.channel)
        # Do something with the channel
        self.reader.channel.tx_select()

    def tcpwatch(self, port):
        script = tempfile.NamedTemporaryFile(suffix='.py')
        script.write("""
import sys
sys.path[:] = %(path)r
import tcpwatch
tcpwatch.main(['-L', '%(src)s:%(dest)s', '-s'])
        """ % dict(path=sys.path, src=port, dest=pika.spec.PORT))
        script.flush()
        process = subprocess.Popen(
            [sys.executable, script.name],
            stdout=open('/dev/null', 'w'), stderr=subprocess.STDOUT)
        time.sleep(0.5)
        return process.pid


class ConsumerTest(unittest.TestCase):

    def test_routing_key_should_be_transferred_to_message(self):
        from gocept.amqprun.server import Consumer
        handler = mock.Mock()
        tasks = mock.Mock()
        consumer = Consumer(handler, tasks)
        method = mock.Mock()
        method.routing_key = 'route'
        consumer(mock.sentinel.channel, method, {}, '')
        message = handler.call_args[0][0]
        self.assertEqual('route', message.routing_key)
