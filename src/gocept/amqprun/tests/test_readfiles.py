# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

from gocept.amqprun.readfiles import FileStoreReader
import gocept.amqprun.interfaces
import gocept.amqprun.testing
import mock
import os
import shutil
import tempfile
import zope.component


class ReaderTest(gocept.amqprun.testing.LoopTestCase):

    def setUp(self):
        super(ReaderTest, self).setUp()
        self.sender = mock.Mock()
        zope.component.provideUtility(
            self.sender, gocept.amqprun.interfaces.ISender)
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir)
        super(ReaderTest, self).tearDown()

    def test_empty_new_directory_nothing_happens(self):
        reader = FileStoreReader(self.tmpdir, 'route')
        reader.scan()
        self.assertFalse(self.sender.send.called)

    def test_scan_should_move_file_to_cur(self):
        reader = FileStoreReader(self.tmpdir, 'route')
        f = open(os.path.join(self.tmpdir, 'new', 'foo'), 'w')
        f.write('contents')
        f.close()
        self.assertEqual(1, len(os.listdir(os.path.join(self.tmpdir, 'new'))))

        with mock.patch('gocept.amqprun.message.Message') as Message:
            reader.scan()

        Message.assert_called_with(
            dict(content_type='text/xml'), 'contents', routing_key='route')
        self.sender.send.assert_called_with(Message())

        self.assertEqual(0, len(os.listdir(os.path.join(self.tmpdir, 'new'))))
        self.assertEqual(1, len(os.listdir(os.path.join(self.tmpdir, 'cur'))))

    def create_reader(self):
        reader = FileStoreReader(self.tmpdir, 'route')
        self.start_thread(reader)

    def test_loop_can_be_stopped_from_outside(self):
        # this test simply should not hang indefinitely
        self.create_reader()
