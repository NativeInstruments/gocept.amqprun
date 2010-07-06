# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import amqplib.client_0_8 as amqp
import pkg_resources
import string
import tempfile
import threading
import time
import unittest
import zope.component.testing


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
        self._queue_prefix = 'test.%f.' % time.time()
        self._queues = []
        zope.component.testing.setUp()
        self.connection = self.layer.connection
        self.channel = self.layer.channel
        self.reader = None

    def tearDown(self):
        if self.reader is not None:
            self.reader.stop()
            self.thread.join()

        for queue_name in self._queues:
            try:
                self.channel.queue_delete(queue_name)
            except amqp.AMQPChannelException:
                pass
        zope.component.testing.tearDown()

    def get_queue_name(self, suffix):
        queue_name = self._queue_prefix + suffix
        self._queues.append(queue_name)
        return queue_name

    def create_reader(self):
        import gocept.amqprun.server
        self.reader = gocept.amqprun.server.MessageReader(self.layer.hostname)
        self.thread = threading.Thread(target=self.reader.start)
        self.thread.start()
        for i in range(100):
            if self.reader.running:
                break
            time.sleep(0.025)
        else:
            self.fail('Reader did not start up.')

    def send_message(self, body, routing_key=''):
        self.channel.basic_publish(amqp.Message(body), 'amq.topic',
                                   routing_key=routing_key)
        time.sleep(0.05)


class MainTestCase(QueueTestCase):

    def setUp(self):
        import gocept.amqprun.worker
        self._timeout = gocept.amqprun.worker.Worker.timeout
        gocept.amqprun.worker.Worker.timeout = 0.05
        super(MainTestCase, self).setUp()

    def tearDown(self):
        import gocept.amqprun.worker
        for t in list(threading.enumerate()):
            if isinstance(t, gocept.amqprun.worker.Worker):
                t.stop()
        super(MainTestCase, self).tearDown()
        gocept.amqprun.worker.Worker.timeout = self._timeout

    def create_reader(self):
        import gocept.amqprun.server
        self.thread = threading.Thread(
            target=gocept.amqprun.server.main, args=(self.config.name,))
        self.thread.start()
        for i in range(100):
            if (gocept.amqprun.server.main_reader is not None and
                gocept.amqprun.server.main_reader.running):
                break
            time.sleep(0.025)
        else:
            self.fail('Reader did not start up.')
        self.reader = gocept.amqprun.server.main_reader

    def make_config(self, package, name):
        base = string.Template(
            pkg_resources.resource_string(package, '%s.conf' % name))
        zcml = pkg_resources.resource_filename(
            package, '%s.zcml' % name)
        self.config = tempfile.NamedTemporaryFile()
        self.config.write(
            base.substitute({'site_zcml': zcml}))
        self.config.flush()
        return self.config.name
