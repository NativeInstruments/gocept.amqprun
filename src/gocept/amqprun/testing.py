# Copyright (c) 2010-2011 gocept gmbh & co. kg
# See also LICENSE.txt

import amqp
import datetime
import email.utils
import gocept.amqprun
import gocept.amqprun.interfaces
import gocept.amqprun.main
import gocept.amqprun.worker
import logging
import os
import pkg_resources
import plone.testing
import plone.testing.zca
import signal
import string
import subprocess
import sys
import tempfile
import time
import unittest


class ZCASandbox(plone.testing.Layer):

    defaultBases = [plone.testing.zca.LAYER_CLEANUP]

    def testSetUp(self):
        plone.testing.zca.pushGlobalRegistry()

    def testTearDown(self):
        plone.testing.zca.popGlobalRegistry()


ZCA_LAYER = ZCASandbox()


class QueueLayer(plone.testing.Layer):

    defaultBases = [ZCA_LAYER]
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
        self['amqp-connection'].connect()
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
            raise RuntimeError(
                '%s failed:\n%s' % (command, stdout))  # pragma: no cover


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
            # NOTE: we seem to need a new channel for each delete;
            # trying to use self.channel for all queues results in its
            # closing after the first delete
            with self.connection.channel() as channel:
                channel.queue_delete(queue_name)
        super(QueueTestCase, self).tearDown()

    def get_queue_name(self, suffix):
        queue_name = self._queue_prefix + suffix
        self._queues.append(queue_name)
        return queue_name

    def send_message(self, body, routing_key='', headers=None, **kw):
        self.channel.basic_publish(
            amqp.Message(
                body,
                timestamp=time.mktime(datetime.datetime.now().timetuple()),
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

    def wait_for_message(self, timeout=10):
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
            params, setup_handlers=setup_handlers)


class MainTestCase(QueueTestCase):

    def setUp(self):
        super(MainTestCase, self).setUp()
        plone.testing.zca.pushGlobalRegistry()

    def tearDown(self):
        super(MainTestCase, self).tearDown()
        plone.testing.zca.popGlobalRegistry()
        # heuristic to avoid accreting more and more debug log output handlers
        if logging.root.handlers:
            handler = logging.root.handlers[-1]
            if isinstance(handler, logging.StreamHandler):
                logging.root.handlers.pop()

    def start_server(self):
        self.server = gocept.amqprun.main.create_configured_server(
            self.config.name)
        self.server.connect()

    def start_server_in_subprocess(self, *args, **kwargs):
        script = tempfile.NamedTemporaryFile(suffix='.py')
        module = kwargs.pop('module', 'gocept.amqprun.main')
        config = [self.config.name]
        config.extend(args)
        script.write("""
import sys
sys.path[:] = %(path)r
import %(module)s
%(module)s.main%(config)r
        """ % dict(path=sys.path, config=tuple(config), module=module))
        script.flush()
        self.stdout = tempfile.TemporaryFile()
        process = subprocess.Popen(
            [sys.executable, script.name],
            stdout=self.stdout, stderr=subprocess.STDOUT)
        time.sleep(1)
        self.pid = process.pid

    def stop_server_in_subprocess(self):
        os.kill(self.pid, signal.SIGINT)
        self.wait_for_subprocess_exit()
        self.pid = None

    def wait_for_subprocess_exit(self, timeout=30):
        for i in range(timeout):
            pid, status = os.waitpid(self.pid, os.WNOHANG)
            if (pid, status) != (0, 0):
                return status
            time.sleep(0.5)
        else:  # pragma: no cover
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
