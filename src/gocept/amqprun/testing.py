# Copyright (c) 2010-2011 gocept gmbh & co. kg
# See also LICENSE.txt

import amqplib.client_0_8 as amqp
import datetime
import email.utils
import gocept.amqprun
import gocept.amqprun.connection
import gocept.amqprun.interfaces
import logging
import mock
import os
import pkg_resources
import plone.testing
import plone.testing.zca
import signal
import string
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import zope.component
import zope.component.globalregistry
import zope.event


class SettingsLayer(plone.testing.Layer):

    defaultBases = (plone.testing.zca.LAYER_CLEANUP,)

    def setUp(self):
        plone.testing.zca.pushGlobalRegistry()
        settings = dict()
        zope.component.provideUtility(
            settings, provides=gocept.amqprun.interfaces.ISettings)

    def tearDown(self):
        plone.testing.zca.popGlobalRegistry()

SETTINGS_LAYER = SettingsLayer()


class ZCMLSandbox(plone.testing.zca.ZCMLSandbox):

    def testSetUp(self):
        plone.testing.zca.pushGlobalRegistry()

    def testTearDown(self):
        plone.testing.zca.popGlobalRegistry()

ZCML_LAYER = ZCMLSandbox(
    filename='configure.zcml', package=gocept.amqprun, module=__name__)


class QueueLayer(plone.testing.Layer):

    defaultBases = [ZCML_LAYER]
    RABBITMQCTL_COMMAND = os.environ.get(
        'AMQP_RABBITMQCTL', 'sudo rabbitmqctl')

    def setUp(self):
        self['amqp-hostname'] = os.environ.get('AMQP_HOSTNAME', 'localhost')
        self['amqp-username'] = os.environ.get('AMQP_USERNAME', 'guest')
        self['amqp-password'] = os.environ.get('AMQP_PASSWORD', 'guest')
        self['amqp-virtualhost'] = os.environ.get('AMQP_VIRTUALHOST', None)
        if self['amqp-virtualhost'] is None:
            self['amqp-virtualhost'] = '/test.%f' % time.time()
            self.rabbitmqctl('add_vhost %s' % self['amqp-virtualhost'])
            self.rabbitmqctl(
                'set_permissions -p %s %s ".*" ".*" ".*"'
                % (self['amqp-virtualhost'], self['amqp-username']))
            self['amqp-virtualhost-created'] = True
        self['amqp-connection'] = amqp.Connection(
            host=self['amqp-hostname'],
            userid=self['amqp-username'],
            password=self['amqp-password'],
            virtual_host=self['amqp-virtualhost'])
        self['amqp-channel'] = self['amqp-connection'].channel()

    def tearDown(self):
        self['amqp-channel'].close()
        del self['amqp-channel']
        self['amqp-connection'].close()
        del self['amqp-connection']
        if 'amqp-virtualhost-created' in self:
            self.rabbitmqctl('delete_vhost %s' % self['amqp-virtualhost'])
            del self['amqp-virtualhost-created']

    def rabbitmqctl(self, parameter):
        command = '%s %s' % (self.RABBITMQCTL_COMMAND, parameter)
        stdout = subprocess.check_output(
            'LANG=C %s' % command, stderr=subprocess.STDOUT, shell=True)
        if 'Error' in stdout:
            raise RuntimeError('%s failed:\n%s' % (command, stdout))

QUEUE_LAYER = QueueLayer()


class QueueTestCase(unittest.TestCase):

    layer = QUEUE_LAYER

    def setUp(self):
        super(QueueTestCase, self).setUp()
        self._queue_prefix = 'test.%f.' % time.time()
        self._queues = []
        self.connection = self.layer['amqp-connection']
        self.channel = self.layer['amqp-channel']

        self.receive_queue = self.get_queue_name('receive')
        self.channel.queue_declare(queue=self.receive_queue)
        self._queues.append(self.receive_queue)

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
        super(QueueTestCase, self).tearDown()

    def get_queue_name(self, suffix):
        queue_name = self._queue_prefix + suffix
        self._queues.append(queue_name)
        return queue_name

    def send_message(self, body, routing_key='', headers=None, **kw):
        self.channel.basic_publish(
            amqp.Message(body, timestamp=datetime.datetime.now(),
                         application_headers=headers or {},
                         msgid=email.utils.make_msgid('gocept.amqprun.test'),
                         **kw),
            'amq.topic', routing_key=routing_key)
        time.sleep(0.1)

    def expect_message_on(self, routing_key):
        self.channel.queue_bind(
            self.receive_queue, 'amq.topic', routing_key=routing_key)

    # BBB
    expect_response_on = expect_message_on

    def wait_for_message(self, timeout=100):
        """Wait for a response on `self.receive_queue`.

        timeout ... wait for n seconds.

        """
        for i in range(timeout):
            message = self.channel.basic_get(self.receive_queue, no_ack=True)
            if message:
                break
            time.sleep(1)
        else:
            raise RuntimeError('No message received')
        return message

    # BBB
    wait_for_response = wait_for_message

    def create_server(self, **kw):
        import gocept.amqprun.server
        params = dict(hostname=self.layer['amqp-hostname'],
                      username=self.layer['amqp-username'],
                      password=self.layer['amqp-password'],
                      virtual_host=self.layer['amqp-virtualhost'])
        # XXX not DRY, the default value is declared in Server.__init__()
        setup_handlers = kw.pop('setup_handlers', True)
        params.update(kw)
        return gocept.amqprun.server.Server(
            gocept.amqprun.connection.Parameters(**params),
            setup_handlers=setup_handlers)


class LoopTestCase(unittest.TestCase):

    def setUp(self):
        super(LoopTestCase, self).setUp()
        self.loop = None

    def tearDown(self):
        if self.loop is not None:
            self.loop.stop()
            self.thread.join()
        super(LoopTestCase, self).tearDown()
        if self.loop is not None and getattr(self.loop, 'connection', None):
            self.assertEqual({}, self.loop.connection.socket_map)

    def start_thread(self, loop):
        self.loop = loop
        self.thread = self._start_thread(loop)

    def _start_thread(self, loop):
        thread = threading.Thread(target=loop.start)
        thread.start()
        if not loop.wait_until_running(2.5):
            self.fail('Loop did not start up.')
        return thread


def set_zca_registry(registry):
    plone.testing.zca._hookRegistry(registry)
    zope.component.getSiteManager.reset()


class MainTestCase(LoopTestCase, QueueTestCase):

    def setUp(self):
        import gocept.amqprun.worker
        super(MainTestCase, self).setUp()
        self._timeout = gocept.amqprun.worker.Worker.timeout
        gocept.amqprun.worker.Worker.timeout = 0.05
        self.orig_signal = signal.signal
        signal.signal = mock.Mock()
        self.orig_registry = zope.component.getGlobalSiteManager()
        set_zca_registry(
            zope.component.globalregistry.BaseGlobalComponents('amqprun-main'))

    def tearDown(self):
        import gocept.amqprun.interfaces
        import gocept.amqprun.worker
        zope.event.notify(gocept.amqprun.interfaces.ProcessStopping())
        for t in list(threading.enumerate()):
            if isinstance(t, gocept.amqprun.worker.Worker):
                t.stop()
        signal.signal = self.orig_signal
        set_zca_registry(self.orig_registry)
        super(MainTestCase, self).tearDown()
        gocept.amqprun.worker.Worker.timeout = self._timeout
        # heuristic to avoid accreting more and more debug log output handlers
        if logging.root.handlers:
            handler = logging.root.handlers[-1]
            if isinstance(handler, logging.StreamHandler):
                logging.root.handlers.pop()

    def start_server(self):
        import gocept.amqprun.main
        self.server_started = threading.Event()
        zope.component.getSiteManager().registerHandler(
            lambda x: self.server_started.set(),
            [gocept.amqprun.interfaces.IProcessStarted])

        self.thread = threading.Thread(
            target=gocept.amqprun.main.main, args=(self.config.name,))
        self.thread.start()
        for i in range(200):
            if (gocept.amqprun.main.main_server is not None and
                    gocept.amqprun.main.main_server.running):
                break
            time.sleep(0.025)
        else:
            self.fail('Server did not start up.')
        self.loop = gocept.amqprun.main.main_server
        self.server_started.wait()

    def start_server_in_subprocess(self):
        script = tempfile.NamedTemporaryFile(suffix='.py')
        script.write("""
import sys
sys.path[:] = %(path)r
import gocept.amqprun.main
gocept.amqprun.main.main('%(config)s')
        """ % dict(path=sys.path, config=self.config.name))
        script.flush()
        self.stdout = tempfile.TemporaryFile()
        process = subprocess.Popen(
            [sys.executable, script.name],
            stdout=self.stdout, stderr=subprocess.STDOUT)
        time.sleep(1)
        self.pid = process.pid

    def wait_for_subprocess_exit(self, timeout=30):
        for i in range(timeout):
            pid, status = os.waitpid(self.pid, os.WNOHANG)
            if (pid, status) != (0, 0):
                return status
            time.sleep(0.5)
        else:
            os.kill(self.pid, signal.SIGKILL)
            self.stdout.seek(0)
            self.fail('Child process did not exit\n' + self.stdout.read())

    def make_config(self, package, name, mapping=None):
        zcml_base = string.Template(
            unicode(pkg_resources.resource_string(package, '%s.zcml' % name),
                    'utf8'))
        self.zcml = tempfile.NamedTemporaryFile()
        self.zcml.write(zcml_base.substitute(mapping).encode('utf8'))
        self.zcml.flush()

        sub = dict(
            site_zcml=self.zcml.name,
            amqp_hostname=self.layer['amqp-hostname'],
            amqp_username=self.layer['amqp-username'],
            amqp_password=self.layer['amqp-password'],
            amqp_virtualhost=self.layer['amqp-virtualhost'],
        )
        if mapping:
            sub.update(mapping)

        base = string.Template(
            unicode(pkg_resources.resource_string(package, '%s.conf' % name),
                    'utf8'))
        self.config = tempfile.NamedTemporaryFile()
        self.config.write(base.substitute(sub).encode('utf8'))
        self.config.flush()
        return self.config.name

    def wait_for_processing(self, timeout=100):
        for i in range(timeout):
            if not self.loop.tasks.qsize():
                break
            time.sleep(0.05)
        else:
            self.fail('Message was not processed.')

    def wait_for_response(self, timeout=100):
        self.wait_for_processing(timeout)
        return super(MainTestCase, self).wait_for_response(timeout)
