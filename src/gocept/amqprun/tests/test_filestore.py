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
        reader = FileStoreReader(self.tmpdir, 'localhost', 'route')
        reader.scan()
        self.assertFalse(connection().channel().basic_publish.called)

    @mock.patch('amqplib.client_0_8.Connection')
    @mock.patch('amqplib.client_0_8.Message')
    def test_scan_should_move_file_to_cur(self, message, connection):
        reader = FileStoreReader(self.tmpdir, 'localhost', 'route')
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
        reader = FileStoreReader(self.tmpdir, 'localhost', 'route')
        self.start_thread(reader)

    @mock.patch('amqplib.client_0_8.Connection')
    def test_loop_can_be_stopped_from_outside(self, connection):
        # this test simply should not hang indefinitely
        self.create_reader()


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
        reader = FileStoreReader(self.tmpdir, 'localhost', 'route')
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
        reader.assert_called_with('/tmp', 'localhost', 'route')


class FileWriterTest(unittest.TestCase):

    def setUp(self):
        super(FileWriterTest, self).setUp()
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir)
        super(FileWriterTest, self).tearDown()

    def test_unique_filename_uses_more_than_millisecond_precision(self):
        from gocept.amqprun.filestore import FileWriter
        writer = FileWriter('routing', '/dev/null')
        filename = writer._unique_filename()
        digits = filename.split('.')[-1]
        self.assertGreater(digits, 2)

    def test_write_message_body(self):
        from gocept.amqprun.filestore import FileWriter
        self.assertEqual(0, len(os.listdir(self.tmpdir)))
        writer = FileWriter('routing', self.tmpdir)
        message = mock.Mock()
        message.body = 'This is only a test.'
        writer(message)
        self.assertEqual(1, len(os.listdir(self.tmpdir)))
        filename = os.listdir(self.tmpdir)[0]
        self.assertRegexpMatches(filename, '^routing')
        contents = open(os.path.join(self.tmpdir, filename)).read()
        self.assertEqual(message.body, contents)
