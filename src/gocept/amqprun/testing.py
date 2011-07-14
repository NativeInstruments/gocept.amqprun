# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import amqplib.client_0_8 as amqp
import asyncore
import datetime
import email.utils
import mock
import pkg_resources
import signal
import string
import tempfile
import threading
import time
import unittest
import zope.component.testing
import zope.configuration.xmlconfig


class QueueLayer(object):

    hostname = 'localhost'

    @classmethod
    def setUp(cls):
        cls.connection = amqp.Connection(host=cls.hostname)
        cls.channel = cls.connection.channel()

    @classmethod
    def tearDown(cls):
        cls.channel.close()
        cls.connection.close()

    @classmethod
    def testSetUp(cls):
        pass

    @classmethod
    def testTearDown(cls):
        pass


class QueueTestCase(unittest.TestCase):

    layer = QueueLayer

    def setUp(self):
        import gocept.amqprun
        super(QueueTestCase, self).setUp()
        self._queue_prefix = 'test.%f.' % time.time()
        self._queues = []
        zope.component.testing.setUp()
        zope.configuration.xmlconfig.file(
            pkg_resources.resource_filename(__name__, 'configure.zcml'),
            package=gocept.amqprun)
        self.connection = self.layer.connection
        self.channel = self.layer.channel

    def tearDown(self):
        for queue_name in self._queues:
            try:
                # NOTE: we seem to need a new channel for each delete;
                # trying to use self.channel for all queues results in its
                # closing after the first delete
                with self.connection.channel() as channel:
                    channel.queue_delete(queue_name)
            except amqp.AMQPChannelException:
                pass
        zope.component.testing.tearDown()
        super(QueueTestCase, self).tearDown()

    def get_queue_name(self, suffix):
        queue_name = self._queue_prefix + suffix
        self._queues.append(queue_name)
        return queue_name

    def send_message(self, body, routing_key=''):
        self.channel.basic_publish(
            amqp.Message(body, timestamp=datetime.datetime.now(),
                         msgid=email.utils.make_msgid('gocept.amqprun.test')),
            'amq.topic', routing_key=routing_key)
        time.sleep(0.1)


class LoopTestCase(unittest.TestCase):

    def setUp(self):
        super(LoopTestCase, self).setUp()
        self.loop = None

    def tearDown(self):
        if self.loop is not None:
            self.loop.stop()
            self.thread.join()
        super(LoopTestCase, self).tearDown()
        self.assertEqual({}, asyncore.socket_map)

    def start_thread(self, loop):
        self.loop = loop
        self.thread = threading.Thread(target=self.loop.start)
        self.thread.start()
        for i in range(100):
            if self.loop.running:
                break
            time.sleep(0.025)
        else:
            self.fail('Loop did not start up.')


class MainTestCase(LoopTestCase, QueueTestCase):

    def setUp(self):
        import gocept.amqprun.worker
        super(MainTestCase, self).setUp()
        self._timeout = gocept.amqprun.worker.Worker.timeout
        gocept.amqprun.worker.Worker.timeout = 0.05
        self.orig_signal = signal.signal
        signal.signal = mock.Mock()

        self.receive_queue = self.get_queue_name('receive')
        self.channel.queue_declare(queue=self.receive_queue)
        self._queues.append(self.receive_queue)

    def expect_response_on(self, routing_key):
        self.channel.queue_bind(
            self.receive_queue, 'amq.topic', routing_key=routing_key)

    def tearDown(self):
        import gocept.amqprun.worker
        for t in list(threading.enumerate()):
            if isinstance(t, gocept.amqprun.worker.Worker):
                t.stop()
        signal.signal = self.orig_signal
        super(MainTestCase, self).tearDown()
        gocept.amqprun.worker.Worker.timeout = self._timeout

    def create_reader(self):
        import gocept.amqprun.main
        self.thread = threading.Thread(
            target=gocept.amqprun.main.main, args=(self.config.name,))
        self.thread.start()
        for i in range(100):
            if (gocept.amqprun.main.main_reader is not None and
                gocept.amqprun.main.main_reader.running):
                break
            time.sleep(0.025)
        else:
            self.fail('Reader did not start up.')
        self.loop = gocept.amqprun.main.main_reader

    def make_config(self, package, name, mapping=None):
        zcml_base = string.Template(
            unicode(pkg_resources.resource_string(package, '%s.zcml' % name),
                    'utf8'))
        self.zcml = tempfile.NamedTemporaryFile()
        self.zcml.write(zcml_base.substitute(mapping).encode('utf8'))
        self.zcml.flush()

        sub = dict(site_zcml=self.zcml.name)
        if mapping:
            sub.update(mapping)

        base = string.Template(
            unicode(pkg_resources.resource_string(package, '%s.conf' % name),
                    'utf8'))
        self.config = tempfile.NamedTemporaryFile()
        self.config.write(base.substitute(sub).encode('utf8'))
        self.config.flush()
        return self.config.name

    def wait_for_response(self, timeout=100):
        for i in range(100):
            if not self.loop.tasks.qsize():
                break
            time.sleep(0.05)
        else:
            self.fail('Message was not processed.')

        # message was taken from queue, wait for response messages to arrive
        for i in range(timeout):
            message = self.channel.basic_get(self.receive_queue, no_ack=True)
            if message:
                break
            time.sleep(1)
        else:
            self.fail('No success message received')
        return message
