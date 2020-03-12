# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

from gocept.amqprun.readfiles import FileStoreReader, FileStoreDataManager
import gocept.amqprun.interfaces
import gocept.amqprun.testing
import mock
import os
import shutil
import tempfile
import unittest
import zope.component
import time


class ReaderTest(unittest.TestCase):

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


class FileStoreDataManagerTest(unittest.TestCase):

    def setUp(self):
        self.session = mock.Mock()
        self.dm = FileStoreDataManager(self.session)

    def test_committing_transaction_should_commit_and_reset_session(self):
        UNUSED_TRANSACTION = None
        self.dm.tpc_begin(UNUSED_TRANSACTION)
        self.dm.commit(UNUSED_TRANSACTION)
        self.dm.tpc_vote(UNUSED_TRANSACTION)
        self.dm.tpc_finish(UNUSED_TRANSACTION)
        self.assertTrue(self.session.commit.called)
        self.assertTrue(self.session.reset.called)

    def test_aborting_transaction_should_commit_and_reset_session(self):
        UNUSED_TRANSACTION = None
        self.dm.abort(UNUSED_TRANSACTION)
        self.assertFalse(self.session.commit.called)
        self.assertTrue(self.session.reset.called)


class ReaderIntegrationTest(gocept.amqprun.testing.MainTestCase):

    def setUp(self):
        super(ReaderIntegrationTest, self).setUp()
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        super(ReaderIntegrationTest, self).tearDown()
        shutil.rmtree(self.tmpdir)

    def wait_for_directory_present(self, path, timeout=10):
        wait = 0
        while wait < timeout:
            if os.path.exists(path):
                return
            time.sleep(0.25)
            wait += 0.25
        raise RuntimeError

    def test_should_send_message_and_move_file(self):
        self.make_config(__name__, 'readfiles')
        self.expect_message_on('test.data')
        self.start_server_in_subprocess(
            self.tmpdir, 'test.data', module='gocept.amqprun.readfiles')

        new_path = os.path.join(self.tmpdir, 'new')
        self.wait_for_directory_present(new_path)

        with open(os.path.join(new_path, 'foo.xml'), 'w') as f:
            f.write('contents')

        message = self.wait_for_message()
        self.assertEqual('contents', message.body)
        self.assertEqual(
            'foo.xml', message.properties['application_headers']['X-Filename'])
        self.assertEqual(0, len(os.listdir(os.path.join(self.tmpdir, 'new'))))
        self.assertEqual(1, len(os.listdir(os.path.join(self.tmpdir, 'cur'))))
        self.stop_server_in_subprocess()

    def test_process_should_exit_on_filesystem_error(self):
        self.make_config(__name__, 'readfiles-error')
        self.start_server_in_subprocess(
            self.tmpdir, 'test.route', module='gocept.amqprun.readfiles')
        directory_path = os.path.join(self.tmpdir, 'new')
        self.wait_for_directory_present(directory_path)
        os.rmdir(directory_path)
        status = self.wait_for_subprocess_exit()
        self.assertNotEqual(0, status)
        self.stdout.seek(0)
        self.assertIn('Unhandled exception, terminating.', self.stdout.read())
