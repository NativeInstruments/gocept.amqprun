# coding: utf8
# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import gocept.amqprun.testing
import logging
import mock
import os
import signal
import subprocess
import sys
import tempfile
import time
import zope.component


class TestMainWithQueue(gocept.amqprun.testing.MainTestCase):

    def setUp(self):
        import gocept.amqprun.tests.integration
        super(TestMainWithQueue, self).setUp()
        self.messages_received = []
        gocept.amqprun.tests.integration.messages_received = (
            self.messages_received)
        self.make_config(__name__, 'integration')
        self._queues.append('test.queue')
        self._queues.append('test.queue.error')
        self.start_server()

    def tearDown(self):
        import gocept.amqprun.tests.integration
        gocept.amqprun.tests.integration.messages_received = None
        super(TestMainWithQueue, self).tearDown()

    def test_message_should_be_processed(self):
        self.assertEquals([], self.messages_received)
        self.send_message('blarf', routing_key='test.routing')
        for i in range(100):
            if self.messages_received:
                break
        else:
            self.fail('Message was not received')
        self.assertEquals(1, len(self.messages_received))

    def test_existing_messages_in_queue_should_not_crash_startup(self):
        # Stop the reader so it doesn't consume messages
        self.loop.stop()
        self.thread.join()
        # Send a message while the reader is not active
        message_count = 50
        for i in range(message_count):
            self.send_message('blarf', routing_key='test.routing')
        self.start_server()

        for i in range(50):
            if len(self.messages_received) >= message_count:
                break
            time.sleep(0.25)
        else:
            self.fail('Message was not received')

    def test_technical_errors_should_not_crash(self):
        self.assertEquals([], self.messages_received)
        self.send_message('blarf', routing_key='test.error')
        for i in range(100):
            if self.messages_received:
                break
        else:
            self.fail('Message was not received')
        self.assertEquals(1, len(self.messages_received))

    def test_rejected_messages_should_be_received_again_later(self):
        self.loop.CHANNEL_LIFE_TIME = 1
        self.assertEqual([], self.messages_received)
        self.send_message('blarf', routing_key='test.error')
        for i in range(200):
            time.sleep(0.025)
            os.write(self.loop.connection.notifier_w, 'W')
            if len(self.messages_received) >= 2:
                break
        else:
            self.fail('Message was not received again')


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
        self.assertEquals(2, self.worker.call_count)
        utilities = list(zope.component.getUtilitiesFor(
            gocept.amqprun.interfaces.IHandlerDeclaration))
        self.assertEquals(1, len(utilities))
        self.assertEquals('basic', utilities[0][0])

    def test_configuration_should_load_logging(self):
        import gocept.amqprun.interfaces
        import gocept.amqprun.main
        config = self.make_config(__name__, 'logging')
        gocept.amqprun.main.main(config)
        self.assertEquals(1, self.server.call_count)
        self.assertEquals(2, self.worker.call_count)
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

    def test_main_should_send_processstart_event(self):
        import gocept.amqprun.interfaces
        import gocept.amqprun.main
        import zope.component
        config = self.make_config(__name__, 'basic')
        handler = mock.Mock()
        zope.component.provideHandler(
            handler, (gocept.amqprun.interfaces.IProcessStarting,))
        gocept.amqprun.main.main(config)
        self.assertTrue(handler.called)

    def test_main_should_register_ISender(self):
        import gocept.amqprun.interfaces
        import gocept.amqprun.main
        import zope.component
        config = self.make_config(__name__, 'basic')
        gocept.amqprun.main.main(config)
        self.assertEqual(
            self.server(), zope.component.getUtility(
                gocept.amqprun.interfaces.ISender))


class TestMainProcess(gocept.amqprun.testing.MainTestCase):

    def start_server(self):
        self.make_config(__name__, 'process')
        script = tempfile.NamedTemporaryFile(suffix='.py')
        script.write("""
import sys
sys.path[:] = %(path)r
import gocept.amqprun.main
gocept.amqprun.main.main('%(config)s')
        """ % dict(path=sys.path, config=self.config.name))
        script.flush()
        self.log = tempfile.TemporaryFile()
        process = subprocess.Popen(
            [sys.executable, script.name],
            stdout=self.log, stderr=subprocess.STDOUT)
        time.sleep(1)
        self.pid = process.pid

    def assert_shutdown(self, signal_):
        self.start_server()
        self._queues.append('test.queue')
        for i in range(50):
            self.send_message('honk', 'test.routing')
        os.kill(self.pid, signal_)
        time.sleep(1)
        self.log.seek(0)
        for i in range(22):
            result = os.waitpid(self.pid, os.WNOHANG)
            if result != (0, 0):
                break
            time.sleep(0.5)
        else:
            os.kill(self.pid, signal.SIGKILL)
            self.fail('Child process did not exit')
        self.assertIn('Received signal %s, terminating.' % signal_,
                      self.log.read())

    def test_sigterm_shuts_down_process_properly(self):
        self.assert_shutdown(signal.SIGTERM)

    def test_sigint_shuts_down_process_properly(self):
        self.assert_shutdown(signal.SIGINT)
