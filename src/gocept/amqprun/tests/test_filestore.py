# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

from gocept.amqprun.filestore import FileStoreReader
import gocept.amqprun.filestore
import gocept.amqprun.testing
import mock
import os
import pkg_resources
import shutil
import tempfile
import time
import unittest


class ReaderTest(gocept.amqprun.testing.LoopTestCase):

    def setUp(self):
        super(ReaderTest, self).setUp()
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir)
        super(ReaderTest, self).tearDown()

    @mock.patch('amqplib.client_0_8.Connection')
    def test_empty_new_directory_nothing_happens(self, connection):
        reader = FileStoreReader(self.tmpdir, 'route', mock.Mock())
        reader.scan()
        self.assertFalse(connection().channel().basic_publish.called)

    @mock.patch('amqplib.client_0_8.Connection')
    @mock.patch('amqplib.client_0_8.Message')
    def test_scan_should_move_file_to_cur(self, message, connection):
        reader = FileStoreReader(self.tmpdir, 'route', mock.Mock())
        f = open(os.path.join(self.tmpdir, 'new', 'foo'), 'w')
        f.write('contents')
        f.close()
        self.assertEqual(1, len(os.listdir(os.path.join(self.tmpdir, 'new'))))

        reader.scan()

        message.assert_called_with('contents', content_type='text/xml')
        connection().channel().basic_publish.assert_called_with(
            message(), 'amq.topic', routing_key='route')

        self.assertEqual(0, len(os.listdir(os.path.join(self.tmpdir, 'new'))))
        self.assertEqual(1, len(os.listdir(os.path.join(self.tmpdir, 'cur'))))

    def create_reader(self):
        reader = FileStoreReader(self.tmpdir, 'route', mock.Mock())
        self.start_thread(reader)

    @mock.patch('amqplib.client_0_8.Connection')
    def test_loop_can_be_stopped_from_outside(self, connection):
        # this test simply should not hang indefinitely
        self.create_reader()

    @mock.patch('amqplib.client_0_8.Connection')
    def test_setting_port_appends_to_hostname(self, connection):
        server = mock.Mock()
        server.hostname = 'localhost'
        server.port = '8080'
        FileStoreReader(self.tmpdir, 'route', server)
        self.assertEqual('localhost:8080', connection.call_args[1]['host'])


class ReaderQueueTest(gocept.amqprun.testing.QueueTestCase):

    def setUp(self):
        super(ReaderQueueTest, self).setUp()
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir)
        super(ReaderQueueTest, self).tearDown()

    def test_send_message(self):
        queue = self.get_queue_name('test.receive')
        self.channel.queue_declare(queue=queue)
        self.channel.queue_bind(queue, 'amq.topic', routing_key='route')
        server = mock.Mock()
        server.hostname = 'localhost'
        server.port = None
        server.username = 'guest'
        server.password = 'guest'
        server.virtual_host = '/'
        reader = FileStoreReader(self.tmpdir, 'route', server)
        reader.send('foo')
        time.sleep(0.05)
        message = self.channel.basic_get(queue)
        self.assertEqual('foo', message.body)


class MainTest(unittest.TestCase):

    @mock.patch('gocept.amqprun.filestore.FileStoreReader')
    def test_main_should_read_config(self, reader):
        config = tempfile.NamedTemporaryFile()
        config.write(pkg_resources.resource_string(__name__, 'filestore.conf'))
        config.flush()
        gocept.amqprun.filestore.main(config.name)
        self.assertEqual('/tmp', reader.call_args[0][0])
        self.assertEqual('route', reader.call_args[0][1])
