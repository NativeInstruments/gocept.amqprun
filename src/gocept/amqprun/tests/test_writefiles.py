# Copyright (c) 2010-2011 gocept gmbh & co. kg
# See also LICENSE.txt

import datetime
import gocept.amqprun.testing
import os
import pkg_resources
import shutil
import string
import tempfile
import time
import unittest
import zope.component
import zope.xmlpickle


class FileWriterTest(unittest.TestCase):

    def setUp(self):
        super(FileWriterTest, self).setUp()
        self.tmpdir = tempfile.mkdtemp()
        zope.component.testing.setUp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir)
        zope.component.testing.tearDown()
        super(FileWriterTest, self).tearDown()

    def create_message(self, body='testbody'):
        from gocept.amqprun.message import Message
        message = Message({}, body, routing_key='routing')
        message.header.message_id = 'myid'
        message.header.timestamp = time.mktime(
            datetime.datetime(2011, 7, 14, 14, 15).timetuple())
        return message

    def test_write_message_body(self):
        from gocept.amqprun.writefiles import FileWriter
        self.assertEqual(0, len(os.listdir(self.tmpdir)))
        writer = FileWriter(self.tmpdir, pattern='foo')
        message = self.create_message()
        writer(message)
        contents = open(os.path.join(self.tmpdir, 'foo')).read()
        self.assertEqual(message.body, contents)

    def test_creates_intermediate_directories(self):
        from gocept.amqprun.writefiles import FileWriter
        self.assertEqual(0, len(os.listdir(self.tmpdir)))
        writer = FileWriter(self.tmpdir, pattern='foo/bar/baz')
        message = self.create_message()
        writer(message)
        filename = os.path.join(
            self.tmpdir, 'foo', 'bar', 'baz')
        self.assertTrue(os.path.exists(filename))
        contents = open(os.path.join(filename)).read()
        self.assertEqual(message.body, contents)

    def test_writes_message_headers(self):
        from gocept.amqprun.writefiles import FileWriter
        self.assertEqual(0, len(os.listdir(self.tmpdir)))
        writer = FileWriter(self.tmpdir, pattern='foo.xml')
        message = self.create_message()
        writer(message)
        self.assertEqual(2, len(os.listdir(self.tmpdir)))
        self.assertTrue(os.path.exists(os.path.join(self.tmpdir, 'foo.xml')))
        contents = open(os.path.join(self.tmpdir, 'foo.header.xml')).read()
        header = zope.xmlpickle.loads(contents)
        self.assertEqual(message.header.message_id, header.message_id)

    def test_sends_event(self):
        from gocept.amqprun.writefiles import FileWriter

        def handler(event):
            self.event = event

        zope.component.getSiteManager().registerHandler(
            handler, (gocept.amqprun.interfaces.IMessageStored,))
        writer = FileWriter(self.tmpdir, pattern='foo/bar/baz')
        writer(self.create_message())
        self.assertEqual('foo/bar/baz', self.event.path)


class AMQPWriteDirectiveTest(unittest.TestCase):

    def setUp(self):
        zope.component.testing.setUp()

    def tearDown(self):
        zope.component.testing.tearDown()

    def run_directive(
        self, routing_key=None, queue_name=None,
        directory=None, pattern=None,
        arguments=None):
        import gocept.amqprun.interfaces
        config = string.Template(unicode(
            pkg_resources.resource_string(__name__, 'writefiles.zcml'),
            'utf8'))
        config = config.substitute(dict(
                routing_key=routing_key,
                queue_name=queue_name,
                directory=directory,
                pattern=pattern,
                arguments=arguments,
                ))
        zope.configuration.xmlconfig.string(config.encode('utf-8'))
        return zope.component.getUtility(
            gocept.amqprun.interfaces.IHandlerDeclaration,
            name='gocept.amqprun.amqpwrite.' + queue_name)

    def test_directive_registers_handler_as_utility(self):
        handler = self.run_directive(
                routing_key='test.foo test.bar',
                queue_name='test.queue',
                directory='/dev/null')
        self.assertEqual(['test.foo', 'test.bar'], handler.routing_key)

    def test_pattern_supports_escape_with_and_without_dollar(self):
        handler = self.run_directive(
                routing_key='test.foo test.bar',
                queue_name='test.queue',
                directory='/dev/null',
                pattern='{foo}/${bar}/{qux}')
        self.assertEqual(
            '${foo}/${bar}/${qux}', handler.handler_function.pattern)

    def test_directive_supports_arguments(self):
        handler = self.run_directive(
                routing_key='test.foo test.bar',
                queue_name='test.queue',
                directory='/dev/null',
                arguments='x-ha-policy = all')
        self.assertEqual({'x-ha-policy': 'all'}, handler.arguments)


class WriterIntegrationTest(gocept.amqprun.testing.MainTestCase):

    def setUp(self):
        super(WriterIntegrationTest, self).setUp()
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir)
        super(WriterIntegrationTest, self).tearDown()

    def test_message_should_be_processed(self):
        self.assertEqual(0, len(os.listdir(self.tmpdir)))
        self.make_config(
            __name__, 'writefiles', dict(
                routing_key='test.data',
                directory=self.tmpdir,
                queue_name=self.get_queue_name('test'),
                pattern='', arguments=''))
        self.start_server()
        body = 'This is only a test.'
        self.send_message(body, routing_key='test.data')
        self.wait_for_processing()
        self.assertEqual(2, len(os.listdir(self.tmpdir)))
