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

    def test_should_move_file_to_cur_on_commit(self):
        reader = FileStoreReader(self.tmpdir, 'route')
        f = open(os.path.join(self.tmpdir, 'new', 'foo'), 'w')
        f.write('contents')
        f.close()

        reader.send = mock.Mock()
        reader.session = mock.Mock()
        reader.scan()
        self.assertTrue(reader.session.mark_done.called)

    def test_exception_in_send_should_not_move_file(self):
        reader = FileStoreReader(self.tmpdir, 'route')
        f = open(os.path.join(self.tmpdir, 'new', 'foo'), 'w')
        f.write('contents')
        f.close()

        reader.send = mock.Mock()
        reader.send.side_effect = RuntimeError('provoked')
        reader.session = mock.Mock()
        reader.scan()
        self.assertFalse(reader.session.mark_done.called)

    def create_reader(self):
        reader = FileStoreReader(self.tmpdir, 'route')
        self.start_thread(reader)

    def test_loop_can_be_stopped_from_outside(self):
        # this test simply should not hang indefinitely
        self.create_reader()


class ReaderIntegrationTest(gocept.amqprun.testing.MainTestCase):

    def setUp(self):
        super(ReaderIntegrationTest, self).setUp()
        self.tmpdir = tempfile.mkdtemp()
        os.mkdir(os.path.join(self.tmpdir, 'new'))
        os.mkdir(os.path.join(self.tmpdir, 'cur'))

    def tearDown(self):
        super(ReaderIntegrationTest, self).tearDown()
        shutil.rmtree(self.tmpdir)

    def test_should_send_message_and_move_file(self):
        self.make_config(
            __name__, 'readfiles', dict(
                routing_key='test.data',
                directory=self.tmpdir))

        self.expect_response_on('test.data')
        self.start_server()
        f = open(os.path.join(self.tmpdir, 'new', 'foo.xml'), 'w')
        f.write('contents')
        f.close()
        message = self.wait_for_response()
        self.assertEqual('contents', message.body)
        self.assertEqual(
            'foo.xml', message.properties['application_headers']['X-Filename'])
        self.assertEqual(0, len(os.listdir(os.path.join(self.tmpdir, 'new'))))
        self.assertEqual(1, len(os.listdir(os.path.join(self.tmpdir, 'cur'))))
