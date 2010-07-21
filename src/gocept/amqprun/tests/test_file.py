# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

from gocept.amqprun.file import FileStoreReader
import mock
import os
import shutil
import tempfile
import unittest


class FileStoreReaderTest(unittest.TestCase):

    def setUp(self):
        super(FileStoreReaderTest, self).setUp()
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir)
        super(FileStoreReaderTest, self).tearDown()

    @mock.patch('amqplib.client_0_8.Connection')
    def test_empty_new_directory_nothing_happens(self, connection):
        reader = FileStoreReader(self.tmpdir, 'localhost', 'route')
        reader.scan()
        self.assertFalse(connection().channel().basic_publish.called)

    @mock.patch('amqplib.client_0_8.Connection')
    @mock.patch('amqplib.client_0_8.Message')
    def test_read_should_move_file_to_cur(self, message, connection):
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
