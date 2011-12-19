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


class MessageReaderTest(
    gocept.amqprun.testing.LoopTestCase,
    gocept.amqprun.testing.QueueTestCase):

    def create_server(self):
        self.reader = super(MessageReaderTest, self).create_server()
        self.start_thread(self.reader)

    def test_loop_can_be_stopped_from_outside(self):
        # this test simply should not hang indefinitely
        self.create_server()

    def test_messages_wo_handler_declaration_should_not_arrive_in_tasks(self):
        self.create_server()
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
        self.create_server()

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
        self.create_server()

        self.assertEqual(0, self.reader.tasks.qsize())
        # With routing key, the message is delivered to the correct handler
        self.send_message('foo', routing_key='test.messageformat.2')
        self.assertEqual(1, self.reader.tasks.qsize())
        handler = self.reader.tasks.get()
        handler()
        self.assertFalse(handle_message_2.called)
        self.assertTrue(handle_message_1.called)

    def test_unicode_queue_names_should_work(self):
        import gocept.amqprun.interfaces
        class Decl(object):
            zope.interface.classProvides(
                gocept.amqprun.interfaces.IHandlerDeclaration)
            queue_name = self.get_queue_name(u'test.case.2')
            routing_key = 'test.messageformat.2'
            arguments = {}
        zope.component.provideUtility(Decl, name='decl')
        self.create_server()

    def test_unicode_routing_keys_should_work_for_handler(self):
        import gocept.amqprun.interfaces
        class Decl(object):
            zope.interface.classProvides(
                gocept.amqprun.interfaces.IHandlerDeclaration)
            queue_name = self.get_queue_name('test.case.2')
            routing_key = u'test.messageformat.2'
            arguments = {}
        zope.component.provideUtility(Decl, name='decl')
        self.create_server()

    def test_unicode_arguments_should_work_for_handler(self):
        import gocept.amqprun.interfaces
        class Decl(object):
            zope.interface.classProvides(
                gocept.amqprun.interfaces.IHandlerDeclaration)
            queue_name = self.get_queue_name('test.case.2')
            routing_key = 'test.messageformat.2'
            arguments = {u'x-ha-policy': u'all'}
        zope.component.provideUtility(Decl, name='decl')
        self.create_server()

    def test_unicode_routing_keys_should_work_for_message(self):
        import gocept.amqprun.message
        import transaction
        self.create_server()
        handler = mock.Mock()
        handler.message.header.message_id = None
        handler.message.header.headers = None
        session = self.reader.get_session(handler.message)
        session.send(gocept.amqprun.message.Message(
            {}, 'body', routing_key=u'test.routing'))
        # Patch out basic_ack because we haven't actually received a message
        with mock.patch('pika.channel.Channel.basic_ack'):
            transaction.commit()

    def test_unicode_body_should_work_for_message(self):
        import gocept.amqprun.message
        import transaction
        self.create_server()
        handler = mock.Mock()
        handler.message.header.message_id = None
        handler.message.header.headers = None
        session = self.reader.get_session(handler.message)
        session.send(gocept.amqprun.message.Message(
            {}, u'body', routing_key='test.routing'))
        # Patch out basic_ack because we haven't actually received a message
        with mock.patch('pika.channel.Channel.basic_ack'):
            transaction.commit()

    def test_unicode_header_should_work_for_message(self):
        import gocept.amqprun.message
        import transaction
        self.create_server()
        handler = mock.Mock()
        handler.message.header.message_id = None
        session = self.reader.get_session(handler.message)
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
        self.create_server()

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
        from gocept.amqprun.server import Server
        import pika.channel
        reader = Server(mock.sentinel.hostname)
        reader.connection = mock.Mock()
        reader.connection.lock = threading.Lock()
        # XXX somehow the ZCA in test_channel_should_be_adaptable_to_manager
        # manages to corrupt the Channel class with __providedBy__ and stuff
        # which we can't get rid off otherwise, but which in turn leads
        # the ZCA here to assume that the channel-mock already provides
        # IChannelManager. (That's actually two bugs in ZCA in one go...)
        channel_spec = [x for x in dir(pika.channel.Channel)
                        if not x.startswith('__')]
        reader.channel = mock.Mock(channel_spec)
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


class DyingRabbitTest(
    gocept.amqprun.testing.LoopTestCase,
    gocept.amqprun.testing.QueueTestCase):

    pid = None

    def tearDown(self):
        super(DyingRabbitTest, self).tearDown()
        if self.pid:
            os.kill(self.pid, signal.SIGINT)

    def create_server(self, port=None):
        self.reader = super(DyingRabbitTest, self).create_server(port=port)
        self.start_thread(self.reader)

    def test_socket_close_should_not_stop_main_loop_and_open_connection(self):
        # This hopefully simulates local errors
        self.create_server()
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
        self.create_server(port)
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
