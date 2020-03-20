# Copyright (c) 2010-2012, 2020 gocept gmbh & co. kg
# See also LICENSE.txt

from gocept.amqprun.server import Parameters
import amqp
import collections
import gocept.amqprun.handler
import gocept.amqprun.interfaces
import gocept.amqprun.testing
import kombu
import mock
import pytest
import socket
import time
import unittest
import zope.component
import zope.interface


class ParametersTest(unittest.TestCase):
    """Testing ..server.Parameters."""

    def test_bails_on_unknown_keyword_argument(self):
        with self.assertRaises(TypeError) as err:
            Parameters(virtualhost='vhost3')
        self.assertEqual(
            "Parameters() got an unexpected keyword argument 'virtualhost'",
            str(err.exception))

    def test_server__Parameters__1(self):
        """It converts a strings to an integer as port number if necessary."""
        params = Parameters(port='1234')
        self.assertEqual(1234, params['port'])


class MessageStoringHandler:
    """A handler which only stores the message."""

    message = None

    def __call__(self, message):
        self.message = message


class ServerTests(gocept.amqprun.testing.QueueTestCase):
    """Testing ..server.Server"""

    def test_server__Server__connect__1(self):
        with mock.patch('kombu.connection.Connection.ensure_connection') as fn:
            self.create_server().connect()

        assert fn.called


class ServerCredentialTests(gocept.amqprun.testing.MainTestCase):
    """Testing ..server.Server"""

    def test_server__Server__connect__2(self):
        """It fails to connect when an incorrect username is given."""
        self.make_config(__name__, 'basic', {'amqp_username': 'wrong'})
        with pytest.raises(amqp.AccessRefused):
            self.start_server()


class MessageReaderTest(gocept.amqprun.testing.QueueTestCase):

    def start_server(self, **kw):
        self.server = self.create_server(**kw)
        self.server.connect()
        return self.server

    def test_different_handlers_should_be_handled_separately(self):
        # Provide two handlers
        handle_message_1 = mock.Mock()
        handle_message_2 = mock.Mock()
        handler_1 = gocept.amqprun.handler.Handler(
            self.get_queue_name('test.case.2'),
            'test.messageformat.2', handle_message_1)
        handler_2 = gocept.amqprun.handler.Handler(
            self.get_queue_name('test.case.3'),
            'test.messageformat.3', handle_message_2)
        zope.component.provideUtility(handler_1, name='1')
        zope.component.provideUtility(handler_2, name='2')
        self.start_server()
        # With routing key, the message is delivered to the correct handler
        self.send_message('foo', routing_key='test.messageformat.2')
        self.server.run_once()
        self.assertFalse(handle_message_2.called)
        self.assertTrue(handle_message_1.called)

    def test_handlers_disabled_should_not_handle_messages(self):
        # Provide a handler
        handle_message = mock.Mock()
        handler = gocept.amqprun.handler.Handler(
            self.get_queue_name('test.case.1'),
            'test.messageformat.1', handle_message)
        zope.component.provideUtility(handler, name='queue')
        self.start_server(setup_handlers=False)

        self.send_message('foo', routing_key='test.messageformat.1')
        assert not handle_message.called

    def test_unicode_queue_names_should_work(self):
        import gocept.amqprun.interfaces

        class Handler(object):
            zope.interface.classProvides(
                gocept.amqprun.interfaces.IHandler)
            queue_name = self.get_queue_name(u'test.case.2')
            routing_key = 'test.messageformat.2'
            arguments = {}
        zope.component.provideUtility(Handler, name='handler')
        self.start_server()

    def test_unicode_routing_keys_should_work_for_handler(self):
        import gocept.amqprun.interfaces

        class Handler(object):
            zope.interface.classProvides(
                gocept.amqprun.interfaces.IHandler)
            queue_name = self.get_queue_name('test.case.2')
            routing_key = u'test.messageformat.2'
            arguments = {}
        zope.component.provideUtility(Handler, name='handler')
        self.start_server()

    def test_unicode_arguments_should_work_for_handler(self):
        import gocept.amqprun.interfaces

        class Handler(object):
            zope.interface.classProvides(
                gocept.amqprun.interfaces.IHandler)
            queue_name = self.get_queue_name('test.case.2')
            routing_key = 'test.messageformat.2'
            arguments = {u'x-ha-policy': u'all'}
        zope.component.provideUtility(Handler, name='handler')
        self.start_server()

    def test_unicode_routing_keys_should_work_for_message(self):
        import gocept.amqprun.message
        import transaction
        self.start_server()
        self.server.send(gocept.amqprun.message.Message(
            {}, 'body', routing_key=u'test.routing'))
        transaction.commit()

    def test_unicode_body_should_work_for_message(self):
        import gocept.amqprun.message
        import transaction
        self.start_server()
        self.server.send(gocept.amqprun.message.Message(
            {'content_encoding': 'UTF-8'},
            u'body',
            routing_key='test.routing'))
        transaction.commit()

    def test_unicode_header_should_work_for_message(self):
        import gocept.amqprun.message
        import transaction
        self.start_server()
        self.server.send(gocept.amqprun.message.Message(
            {u'content_type': u'text/plain'},
            'body', routing_key='test.routing'))
        transaction.commit()

    def test_multiple_routing_keys_should_recieve_messages_for_all(self):
        handle_message = mock.Mock()
        handler = gocept.amqprun.handler.Handler(
            self.get_queue_name('test.routing'),
            ['route.1', 'route.2'], handle_message)
        zope.component.provideUtility(handler, name='handler')
        self.start_server()
        self.send_message('foo', routing_key='route.1')
        self.send_message('bar', routing_key='route.2')
        self.server.run_once()
        self.server.run_once()

        assert handle_message.call_count == 2
        message_1 = handle_message.call_args_list[0][0][0]
        message_2 = handle_message.call_args_list[1][0][0]
        assert message_1.routing_key == 'route.1'
        assert message_2.routing_key == 'route.2'

    def test_server_handles_messages(self):
        handle_message = MessageStoringHandler()
        handler = gocept.amqprun.handler.Handler(
            self.get_queue_name('test.case.1'),
            'test.messageformat.1', handle_message)
        zope.component.provideUtility(handler, name='queue')
        self.start_server()
        self.send_message('foo', routing_key='test.messageformat.1')
        self.server.run_once()
        assert handle_message.message.body == 'foo'
        assert handle_message.message.routing_key == 'test.messageformat.1'


class TestChannelSwitch(unittest.TestCase):

    def create_server(self):
        from gocept.amqprun.server import Server
        server = Server({})
        server.connection = mock.Mock()
        server.channel = mock.Mock(spec=amqp.Channel, channel_id=1)
        server.channel.callbacks = collections.OrderedDict()
        new_channel = mock.Mock(spec=amqp.Channel, channel_id=2)
        new_channel.callbacks = {}
        new_channel.basic_get.return_value = None
        new_channel.handler = mock.Mock()
        server.connection.channel.return_value = new_channel
        return server

    def test_switch_channel_should_open_new_channel(self):
        server = self.create_server()
        server.switch_channel()
        self.assertTrue(server.connection.channel.called)
        new_channel = server.connection.channel()
        self.assertEqual(new_channel, server.channel)

    def test_switch_channel_calls_get_on_new_channel(self):
        from gocept.amqprun.handler import Handler
        server = self.create_server()
        handler = Handler('queue_name', 'routing_key', lambda x: None)
        zope.component.provideUtility(handler, name='handler')
        old_channel = server.channel
        server.switch_channel()
        assert old_channel != server.channel
        server.run_once()
        self.assertTrue(server.channel.basic_get.called)

    def test_run_calls_switch_channel_once_in_a_while(self):
        server = self.create_server()
        self.assertEquals(360, server.CHANNEL_LIFE_TIME)
        server.CHANNEL_LIFE_TIME = 0.4
        server._channel_opened = time.time()
        # Channels are not switched immediately after startup
        with mock.patch.object(server, 'switch_channel') as switch_channel:
            server.run_once()
            assert not switch_channel.called
            # Channels are switched only after the set channel life time.
            time.sleep(0.25)
            server.run_once()
            assert not switch_channel.called
            # Channels are switched after the set channel life time.
            time.sleep(0.25)
            server.run_once()
            assert switch_channel.called

    def test_old_channel_should_be_closed(self):
        server = self.create_server()
        old_channel = server.channel
        server.switch_channel()
        assert old_channel.close.called

    def test_active_connection_lock_should_defer_switch_channel(self):
        server = self.create_server()
        server.connection.lock.acquire()
        self.assertFalse(server.switch_channel())


class DyingRabbitTest(gocept.amqprun.testing.QueueTestCase):

    def start_server(self):
        self.server = self.create_server()
        self.server.connect()

    def test_socket_close_should_not_stop_main_loop_and_reopen_channel(self):
        handler = gocept.amqprun.handler.Handler(
            'test.queuename',
            'test.routingkey', mock.Mock())
        zope.component.provideUtility(handler, name='1')
        self.start_server()

        with mock.patch.object(
                self.server.channel, 'basic_get', side_effect=socket.error):
            self.server.run_once()
        assert self.server.channel is None

        self.server.run_once()
        assert self.server.channel is not None

    def test_remote_close_should_not_break_switch_channel(self):
        self.start_server()
        with mock.patch.object(
                self.server.channel, 'close', side_effect=socket.error):
            self.server.switch_channel()
        assert self.server.channel is None
        self.server.run_once()
        assert self.server.channel is not None


class ConsumerTest(unittest.TestCase):

    def test_routing_key_should_be_transferred_to_message(self):
        from gocept.amqprun.server import Consumer
        handler = mock.Mock()
        consumer = Consumer(handler)
        delivery_info = {
            'delivery_tag': 'delivery_tag',
            'routing_key': 'route'}
        message = kombu.Message(
            delivery_info=delivery_info,
            channel=mock.Mock(channel_id=1))
        consumer(message)
        assert handler.call_args[0][0].routing_key == 'route'


class EndToEndTest(gocept.amqprun.testing.MainTestCase):
    """Testing .server.Server when started in a seperate subprocess."""

    def setUp(self):
        super(EndToEndTest, self).setUp()
        self.make_config(__name__, 'server')
        self.expect_message_on('test.echoed')
        self.start_server_in_subprocess()
        self.wait_for_queue_declared('test.queue')

    def tearDown(self):
        self.stop_server_in_subprocess()
        super(EndToEndTest, self).tearDown()

    def wait_for_queue_declared(self, queue_name, timeout=5):
        wait = 0
        while wait < timeout:
            try:
                self.channel.basic_get(queue_name, no_ack=False)
            except amqp.NotFound as e:
                print(e)
                time.sleep(1)
                wait += 1
            else:
                print('found!')
                return
        raise RuntimeError

    def test_server_receives_messages_in_subprocess(self):
        self.send_message('test echo!', 'test.echo')
        message = self.wait_for_message()
        assert message.body == 'test echo!'
