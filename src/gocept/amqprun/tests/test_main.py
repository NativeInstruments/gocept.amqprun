# coding: utf8
import gocept.amqprun.interfaces
import gocept.amqprun.message
import gocept.amqprun.session
import gocept.amqprun.testing
import gocept.amqprun.tests.integration
import logging
import mock
import zope.component


class ReceiveMessages(gocept.amqprun.testing.MainTestCase):

    def setUp(self):
        super(ReceiveMessages, self).setUp()
        self.messages_received = []
        gocept.amqprun.tests.integration.messages_received = (
            self.messages_received)
        self.make_config(__name__, 'integration')
        self._queues.append('test.queue')
        self._queues.append('test.queue.error')
        self.start_server()

    def tearDown(self):
        gocept.amqprun.tests.integration.messages_received = None
        super(ReceiveMessages, self).tearDown()

    def test_message_should_be_processed(self):
        self.assertEquals([], self.messages_received)
        self.send_message('blarf', routing_key='test.routing')
        self.server.run_once()
        self.assertEquals(1, len(self.messages_received))

    def test_technical_errors_should_not_crash(self):
        self.assertEquals([], self.messages_received)
        self.send_message('blarf', routing_key='test.error')
        self.server.run_once()
        self.assertEquals(1, len(self.messages_received))

    def test_exception_handling_with_iresponse_acks_original_message(self):
        self.assertEquals([], self.messages_received)
        self.expect_message_on('test.iresponse-error')

        def ack(session):
            self.ack_called = True
            original_ack(session)
        self.ack_called = False
        original_ack = gocept.amqprun.session.Session.ack_received_message

        def flush(session):
            if not self.flush_called:
                self.flush_called = True
                raise RuntimeError('provoked error')
            original_flush(session)
        self.flush_called = False
        original_flush = gocept.amqprun.session.Session.flush

        with mock.patch.multiple('gocept.amqprun.session.Session',
                                 ack_received_message=ack, flush=flush):
            self.send_message('blarf', routing_key='test.iresponse')
            self.server.run_once()
            self.wait_for_message()
            self.assertTrue(self.ack_called)
            self.assertEquals(1, len(self.messages_received))
            self.assertTrue(self.messages_received[0]._exception)


class ConfigLoadingTest(gocept.amqprun.testing.MainTestCase):

    def setUp(self):
        super(ConfigLoadingTest, self).setUp()
        self.patchers = []
        patcher = mock.patch('gocept.amqprun.server.Server')
        self.server = patcher.start()
        self.patchers.append(patcher)
        patcher = mock.patch('gocept.amqprun.worker.Worker')
        self.worker = patcher.start()
        self.patchers.append(patcher)
        patcher = mock.patch('sys.exit')
        patcher.start()
        self.patchers.append(patcher)

    def tearDown(self):
        for patcher in self.patchers:
            patcher.stop()
        super(ConfigLoadingTest, self).tearDown()

    def test_basic_configuration_should_load_zcml(self):
        import gocept.amqprun.interfaces
        import gocept.amqprun.main
        config = self.make_config(__name__, 'basic')
        gocept.amqprun.main.main(config)
        self.assertEquals(1, self.server.call_count)
        utilities = list(zope.component.getUtilitiesFor(
            gocept.amqprun.interfaces.IHandler))
        self.assertEquals(1, len(utilities))
        self.assertEquals('basic', utilities[0][0])

    def test_configuration_should_load_logging(self):
        import gocept.amqprun.interfaces
        import gocept.amqprun.main
        config = self.make_config(
            __name__, 'logging', mapping={
                'logger_info_name': 'foo',
                'logger_debug_name': 'foo.bar'
            })
        gocept.amqprun.main.main(config)
        self.assertEquals(1, self.server.call_count)
        self.assertEqual(logging.CRITICAL, logging.getLogger().level)
        self.assertEqual(logging.INFO, logging.getLogger('foo').level)
        self.assertEqual(logging.DEBUG, logging.getLogger('foo.bar').level)

    def test_settings_should_be_available_through_utility(self):
        import gocept.amqprun.interfaces
        import gocept.amqprun.main
        config = self.make_config(__name__, 'settings')
        gocept.amqprun.main.main(config)
        settings = zope.component.getUtility(
            gocept.amqprun.interfaces.ISettings)
        self.assertEquals('foo', settings.get('test.setting.1'))
        self.assertEquals('bar', settings.get('test.setting.2'))

    def test_settings_should_be_unicode(self):
        import gocept.amqprun.interfaces
        import gocept.amqprun.main
        config = self.make_config(__name__, 'settings')
        gocept.amqprun.main.main(config)
        settings = zope.component.getUtility(
            gocept.amqprun.interfaces.ISettings)
        self.assertIsInstance(settings.get('test.setting.1'), unicode)
        self.assertEquals(u'Ümläuten', settings.get('test.setting.unicode'))

    def test_settings_should_allow_upper_case(self):
        import gocept.amqprun.interfaces
        import gocept.amqprun.main
        config = self.make_config(__name__, 'settings')
        gocept.amqprun.main.main(config)
        settings = zope.component.getUtility(
            gocept.amqprun.interfaces.ISettings)
        self.assertEquals('qux', settings.get('test.SETTING.__default__'))

    def test_main_should_send_configfinished_event(self):
        from gocept.amqprun.interfaces import ISender, IConfigFinished
        from gocept.amqprun.main import main
        from zope.component import provideHandler, queryUtility
        config = self.make_config(__name__, 'basic')
        self.handler_called = False

        def handler(event):
            self.handler_called = True
            self.assertTrue(queryUtility(ISender))

        provideHandler(handler, (IConfigFinished,))
        main(config)
        self.assertTrue(self.handler_called)

    def test_main_should_register_ISender(self):
        import gocept.amqprun.interfaces
        import gocept.amqprun.main
        import zope.component
        config = self.make_config(__name__, 'basic')
        gocept.amqprun.main.main(config)
        self.assertEqual(
            self.server(), zope.component.getUtility(
                gocept.amqprun.interfaces.ISender))
